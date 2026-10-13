// dsh-session-vacuum —— 会话日志自愈（开机维护一次）
//
// 干什么：DSH 的会话日志里堆积着海量流式碎片（assistant/chunk 与三种打包行）。
// 它们在 assistant/message 定稿后就没有任何用途 —— 回放不看、喂模型不读（投影层里
// 连 chunks 这个词都没有）—— 但只写不删，于是老会话一路膨胀：一个 1813 轮的会话
// 是 22.4 万行 / 272 MB，加载时展开成 843 万个对象 / 1.29 GB 常驻，最后 OOM。
//
// 为什么做成插件而不是改加载器：
//   插件改不了已经加载进内存的事件，但它不需要 —— 只要趁会话不在写的时候把文件修小，
//   以后每一次加载都是小的。于是内存/卡顿/崩溃一起解决，而且完全不碰 node_modules。
//
// 安全设计（每一条都是今天交了学费换来的）：
//   1. 只处理【最近没被动过】的文件：正在写的会话跳过，绝不从 DSH 脚下换文件
//   2. 改之前先建 .orig 备份，且永不删除
//   3. 只在碎片确实超标时才动手（阈值可配），几十 KB 的会话根本不碰
//   4. 改完立即重新解压校验（seq 连续 + 四类引用无悬空 + 加载契约），不过就不换
//   5. 全程串行，且每个文件之间让出事件循环，不拖慢启动
//   6. 默认 enabled=false：装上什么都不做，看过日志再开
//
// 按需清理（2026-10-13 加）：
//   开机那一次维护有个真实痛点 —— 它对【10 分钟内写过的会话】一律跳过，而用户真正
//   关心的恰恰是那个会话；再加上 run-end 只记 handled，另外 56 个文件为什么没被处理
//   完全看不见，于是这个插件"看起来什么都没干"。所以补两件事：
//     a. run-end 记下【每个跳过原因各多少个】（不改变清理行为，只改可观测性）
//     b. 一个 Host 工具 clean_session_logs，让 agent / 脚本【当场】问它跑一次
//   两者走的是【同一个】受 liveWindowMs 保护、且必须过 vacuumOne() 的通道 ——
//   按需接口不是"更大的锤子"，它只是不必等到下次开机。
//
// ASCII ONLY on purpose -- this box runs a GBK code page.

import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import zlib from 'node:zlib'

const name = 'dsh-session-vacuum'
// ★ 必须声明要用哪些服务。Cordis 是【按需注入】的：没有 inject 声明的服务在 ctx 上根本
//   不存在，读一下就会抛 `cannot get property "tools" without inject`
//   （实测：插件在沙箱里被这一条直接停用了）。
//   只声明 tools —— 它是工具的落点，没有它这个插件就没有按需入口。
//   webServer 不声明：headless profile 里没有它，声明了就要求它存在。
//   路由改用可选注入 ctx.inject(['webServer'], ...)，有就注册、没有就跳过。
const inject = ['tools']

// 按需入口的两个名字：工具给 agent 用，路由给脚本用。两者是同一个适配层。
const TOOL_NAME = 'clean_session_logs'
const ROUTE_PATH = '/api/local/session-vacuum/run'
// 报告里最多逐条列出几个文件名（多了没用：run-end 里有完整计数）
const REPORT_NAME_LIMIT = 12

// ★ 在【模块加载期】解析一次 @deepseek-ai/dsh-tools，而不是等 apply() 调用时再 import。
//
//   为什么：profile 的依赖树里能解析到这个包，但那是个动态 import —— 如果在 apply() 里
//   现拉，工具的注册就要等到 import 落地之后，也就是说 apply() 返回时 ctx.tools.register
//   还没被调用过。插件加载本来就是异步的（loader 会 await import 这个模块），所以放在这里
//   一分钱都不多花，换来的是【apply() 里的一切都是同步的】—— 装上就是装上了，
//   没有"稍后才生效"的窗口。
//
//   失败不致命：任何原因解析不到就用裸对象形态注册（见 buildTool）。
const TOOLS_PACKAGE = '@deepseek-ai/dsh-tools'
let bundledDefineTool = null
try {
  const mod = await import(TOOLS_PACKAGE)
  if (mod && typeof mod.defineTool === 'function') bundledDefineTool = mod.defineTool
} catch {
  // 这个 build 没有这个包（或导出名变了）=> 裸对象形态，一样能注册。
  bundledDefineTool = null
}

const DEFAULTS = {
  enabled: false,
  // 开机后延迟多久开始扫（毫秒），避开启动高峰
  startDelayMs: 20000,
  // 两次维护之间的最小间隔（毫秒），默认 6 小时
  minIntervalMs: 6 * 60 * 60 * 1000,
  // 碎片行数下限：低于它一律不动
  minChunkRows: 2000,
  // 碎片占比下限
  minRatio: 0.25,
  // 多少分钟以内被改过的会话视为"正在写"，跳过
  liveWindowMs: 10 * 60 * 1000,
  // 每次维护最多处理多少个文件
  maxFilesPerRun: 12,
  dryRun: true,
  logFile: 'logs/session-vacuum.jsonl',
}

// 只写不读的流式碎片行
const CHUNK_TYPES = new Set(['text-chunks', 'reasoning-chunks', 'tool-call-chunks', 'assistant/chunk'])
// 会进 surface 的类型，必须带 surfaceOp 标记
const SURFACE_TYPES = new Set(['user/message', 'assistant/message', 'tool/result'])

function dshHome() {
  return process.env.DSH_HOME || path.join(os.homedir(), '.dsh')
}
function configPath() {
  return path.join(dshHome(), 'dsh-session-vacuum', 'config.json')
}
function readConfig() {
  try {
    return { ...DEFAULTS, ...JSON.parse(fs.readFileSync(configPath(), 'utf8')) }
  } catch {
    return { ...DEFAULTS }
  }
}
function logLine(obj) {
  try {
    const file = path.join(dshHome(), readConfig().logFile)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    fs.appendFileSync(file, JSON.stringify({ t: new Date().toISOString(), ...obj }) + '\n')
  } catch {
    // 记日志永远不能影响主流程
  }
}

// ---------------------------------------------------------------- zstd 多帧

// 会话日志是【很多个】独立 zstd 帧拼起来的（不是一个大帧），所以不能整体解压。
// 标准 API 只认第一帧，必须自己走帧边界。
//
// ★ 不能靠"扫 magic"找边界。28 b5 2f fd 这四字节完全可能【出现在某一帧的载荷里】，
//   那是个假边界；而 Node 的 zstdDecompressSync 遇到半截帧【不抛错、返回空】，
//   于是切点落在干净的换行处 —— 截断后的行集照样能解析、能过自校验，
//   然后真日志被替换成截断版。实测（合成日志）：661 行读回 200 行、写成 38 行，
//   状态还是 vacuumed。所以这里改成【按帧结构算长度】：帧头 + 每个 block 的 3 字节头。
const ZSTD_MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd])

/**
 * 从 start 开始那一帧的字节长度；不是合法帧头就返回 -1。
 *
 * zstd 帧结构（RFC 8878）：
 *   magic(4) | frame header(2..14) | blocks | [content checksum(4)]
 * frame header descriptor 的位：7-6 = Frame_Content_Size_flag，
 *   5 = Single_Segment_flag，3 = 保留位（必须为 0），2 = Content_Checksum_flag，
 *   1-0 = Dictionary_ID_flag。
 * block header 是 3 字节小端：bit0 = Last_Block，bit1-2 = Block_Type
 *   （0=Raw，1=RLE，2=Compressed，3=保留），bit3-23 = Block_Size。
 *
 * @param buffer - 文件原始字节
 * @param start - 帧的起始偏移
 * @returns 帧长度（含头尾），或 -1 表示这里不是一帧的开头 / 数据不完整
 */
function frameLength(buffer, start) {
  if (start + 5 > buffer.length) return -1
  if (!buffer.subarray(start, start + 4).equals(ZSTD_MAGIC)) return -1
  let at = start + 4
  const descriptor = buffer[at]
  at += 1
  if ((descriptor & 0x08) !== 0) return -1 // 保留位非 0 => 不是帧头
  const singleSegment = (descriptor & 0x20) !== 0
  const checksum = (descriptor & 0x04) !== 0
  const dictFlag = descriptor & 0x03
  const sizeFlag = descriptor >> 6
  // Window_Descriptor 只在非单段帧里出现
  if (!singleSegment) at += 1
  at += dictFlag === 0 ? 0 : dictFlag === 1 ? 1 : dictFlag === 2 ? 2 : 4
  at += sizeFlag === 0 ? (singleSegment ? 1 : 0) : sizeFlag === 1 ? 2 : sizeFlag === 2 ? 4 : 8
  if (at + 3 > buffer.length) return -1
  for (;;) {
    if (at + 3 > buffer.length) return -1
    const header = buffer[at] | (buffer[at + 1] << 8) | (buffer[at + 2] << 16)
    const last = header & 1
    const type = (header >> 1) & 3
    const size = header >> 3
    at += 3
    if (type === 3) return -1 // 保留的 block 类型
    at += type === 1 ? 1 : size
    if (at > buffer.length) return -1
    if (last === 1) break
  }
  if (checksum) at += 4
  if (at > buffer.length) return -1
  return at - start
}

/**
 * 逐帧解压一份多帧 zstd 日志。
 *
 * ★ truncated 为 true 表示【最后还剩一段走不过去】—— 可能是崩溃留下的半截帧，
 *   也可能是我们没认出来的东西。调用方必须据此拒绝改写：宁可这个会话不维护，
 *   也不能拿一份读残的数据去替换真日志。
 *
 * @param buffer - 文件原始字节
 * @returns {{text: string, ranges: Array<{from: number, to: number}>, truncated: boolean, trailingBytes: number}}
 */
function decompressFrames(buffer) {
  const parts = []
  const ranges = []
  let at = 0
  while (at < buffer.length) {
    const length = frameLength(buffer, at)
    if (length < 0) break
    const end = at + length
    parts.push(zlib.zstdDecompressSync(buffer.subarray(at, end)))
    ranges.push({ from: at, to: end })
    at = end
  }
  if (ranges.length === 0) throw new Error('not a zstd frame stream')
  return {
    text: Buffer.concat(parts).toString('utf8'),
    ranges,
    truncated: at < buffer.length,
    trailingBytes: buffer.length - at,
  }
}

/** 把明文按原帧的字节比例重新打包成多帧。 */
function compressFrames(text) {
  const lines = text.split('\n').filter((l) => l.length > 0)
  if (lines.length === 0) throw new Error('empty log')
  const frames = [zlib.zstdCompressSync(Buffer.from(lines[0] + '\n', 'utf8'))]
  const BATCH = 500
  for (let i = 1; i < lines.length; i += BATCH) {
    const batch = lines.slice(i, i + BATCH).join('\n') + '\n'
    frames.push(zlib.zstdCompressSync(Buffer.from(batch, 'utf8')))
  }
  return Buffer.concat(frames)
}

// ---------------------------------------------------------------- 自愈算法

function isChunkRow(v) {
  return v !== null && typeof v === 'object' && CHUNK_TYPES.has(v.type)
}

function illegalRange(op) {
  // ★ 只对对象形状生效：字符串 "append" 是另一种合法标记，按对象读会误判成非法，
  //   从而丢掉每一条正常消息（这个 bug 被单元测试逮到过）。
  if (op === undefined || op === null || typeof op !== 'object') return false
  return typeof op.start !== 'number' || typeof op.end !== 'number' || op.start > op.end
}

function dropIllegal(events) {
  const kept = []
  let dropped = 0
  for (const ev of events) {
    const range = ev.data === undefined ? undefined : ev.data.shadowedRange
    const missingMarker = SURFACE_TYPES.has(ev.type) && ev.surfaceOp === undefined
    if (missingMarker || illegalRange(ev.surfaceOp) || illegalRange(range)) {
      dropped++
      continue
    }
    kept.push(ev)
  }
  if (dropped === 0) return 0
  events.length = 0
  events.push(...kept)
  return dropped
}

/**
 * 剪除一份日志里的碎片，重映射四类跨事件引用，重排 seq。
 * @param rows - 全部存储行（第一行是 header）
 * @returns { header, events, stats }
 */
function pruneRows(rows) {
  const header = rows.find((r) => r && typeof r === 'object' && r.type === 'session')
  if (header === undefined) throw new Error('session header line is missing')

  const events = []
  let chunkRows = 0
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue
    if (isChunkRow(row)) {
      chunkRows++
      continue
    }
    if (row.type === 'session') continue
    events.push(row)
  }
  const totalEvents = events.length

  const buildMap = () => {
    const map = new Map()
    let next = 0
    for (const ev of events) {
      if (typeof ev.seq === 'number') map.set(ev.seq, next)
      next++
    }
    return map
  }
  let oldToNew = buildMap()

  const remapRefs = (obj) => {
    if (Array.isArray(obj)) {
      obj.forEach(remapRefs)
      return
    }
    if (!obj || typeof obj !== 'object') return
    for (const key of Object.keys(obj)) {
      const val = obj[key]
      if (key === 'sourceEventSeqs' && Array.isArray(val)) {
        obj[key] = val.map((n) => oldToNew.get(n)).filter((n) => n !== undefined)
        continue
      }
      if (key === 'surfaceOp' && val !== null && typeof val === 'object' && val.op === 'replace') {
        const ns = oldToNew.get(val.start)
        const ne = oldToNew.get(val.end)
        if (ns === undefined || ne === undefined) delete obj[key]
        else {
          val.start = ns
          val.end = ne
        }
        continue
      }
      if (key === 'shadowedSeqs' && Array.isArray(val)) {
        obj[key] = val.map((n) => oldToNew.get(n)).filter((n) => n !== undefined)
      }
      if (key === 'shadowedRange' && val !== null && typeof val === 'object') {
        const ns = oldToNew.get(val.start)
        const ne = oldToNew.get(val.end)
        if (ns === undefined || ne === undefined) delete obj[key]
        else {
          val.start = ns
          val.end = ne
        }
        continue
      }
      remapRefs(val)
    }
  }

  for (const ev of events) remapRefs(ev)

  // 不动点：remap 会为"指向已丢事件的端点"再摘一次 surfaceOp，从而造出新的非法事件。
  let dropped = 0
  for (let pass = 0; pass < 8; pass++) {
    const now = dropIllegal(events)
    dropped += now
    if (now === 0) break
    oldToNew = buildMap()
    for (const ev of events) remapRefs(ev)
  }

  let seq = 0
  for (const ev of events) {
    ev.seq = seq
    seq++
  }

  return { header, events, stats: { keptEvents: totalEvents, chunkRows, dropped } }
}

/** 剪除后必须仍然满足加载契约，否则不许写回。 */
function verifyPruned(pruned) {
  const problems = []
  const events = pruned.events
  const survivors = new Set()
  for (let i = 0; i < events.length; i++) {
    const ev = events[i]
    if (ev.seq !== i) problems.push('seq ' + ev.seq + ' != index ' + i)
    survivors.add(ev.seq)
  }
  for (const ev of events) {
    const surface = SURFACE_TYPES.has(ev.type)
    if (!surface) {
      if (ev.surfaceOp !== undefined) problems.push('seq ' + ev.seq + ': non-surface carries surfaceOp')
      if (ev.sourceEventSeqs !== undefined) problems.push('seq ' + ev.seq + ': non-surface carries sourceEventSeqs')
    } else if (ev.surfaceOp === undefined) {
      problems.push('seq ' + ev.seq + ': surface event has no marker')
    }
    if (ev.surfaceOp !== undefined && ev.surfaceOp !== null && typeof ev.surfaceOp === 'object') {
      const op = ev.surfaceOp
      if (op.op !== 'replace') problems.push('seq ' + ev.seq + ': surfaceOp.op is not replace')
      else if (Object.keys(op).sort().join(',') !== 'end,op,start') problems.push('seq ' + ev.seq + ': surfaceOp keys wrong')
      else if (op.start > op.end) problems.push('seq ' + ev.seq + ': surfaceOp start after end')
      if (op.start !== undefined && !survivors.has(op.start)) problems.push('seq ' + ev.seq + ': surfaceOp.start missing')
      if (op.end !== undefined && !survivors.has(op.end)) problems.push('seq ' + ev.seq + ': surfaceOp.end missing')
    }
    if (Array.isArray(ev.sourceEventSeqs)) {
      for (const s of ev.sourceEventSeqs) {
        if (!survivors.has(s)) problems.push('seq ' + ev.seq + ': sourceEventSeqs cites missing ' + s)
        else if (s >= ev.seq) problems.push('seq ' + ev.seq + ': sourceEventSeqs cites non-earlier ' + s)
      }
    }
    const range = ev.data === undefined ? undefined : ev.data.shadowedRange
    if (range !== undefined && range !== null && typeof range === 'object') {
      if (!survivors.has(range.start) || !survivors.has(range.end)) problems.push('seq ' + ev.seq + ': shadowedRange endpoint missing')
      else if (range.start > range.end) problems.push('seq ' + ev.seq + ': shadowedRange start after end')
    }
  }
  return problems
}

// ---------------------------------------------------------------- 维护

function listSessionLogs(root, out = []) {
  let entries
  try {
    entries = fs.readdirSync(root, { withFileTypes: true })
  } catch {
    return out
  }
  for (const entry of entries) {
    const full = path.join(root, entry.name)
    if (entry.isDirectory()) listSessionLogs(full, out)
    else if (entry.name === 'session.jsonl.zstd') out.push(full)
  }
  return out
}

/** 单个会话的维护；返回一行结果对象。 */
function vacuumOne(file, cfg) {
  const stat = fs.statSync(file)
  if (Date.now() - stat.mtimeMs < cfg.liveWindowMs) {
    return { file, status: 'skipped-live' }
  }
  // 这里原先有个【空 if】（条件是 skipIfBackedUp，而它不在 DEFAULTS 里），
  // 也就是"已经维护过就跳过"那个分支从来没实现过。不实现是有意的：
  // .orig 永远不删，按"存在 .orig 就跳过"会让一个会话只被处理一次，
  // 之后即使又长大也不再维护。现在的行为是每次运行都重新评估（靠阈值兜底），
  // 而 .orig 只在第一次处理时写、之后绝不覆盖。

  const buffer = fs.readFileSync(file)
  let decoded
  try {
    decoded = decompressFrames(buffer)
  } catch (err) {
    return { file, status: 'unreadable', error: String(err.message) }
  }
  if (decoded.truncated) {
    // ★ 读残了就绝不写。见 frameLength 的注释：拿读残的数据去替换真日志会静默丢尾巴，
    //   而且截断点落在换行处时自校验也拦不住。
    return { file, status: 'unreadable-tail', trailingBytes: decoded.trailingBytes }
  }
  const rows = []
  for (const line of decoded.text.split('\n')) {
    if (line.length === 0) continue
    try {
      rows.push(JSON.parse(line))
    } catch {
      return { file, status: 'unparsable' }
    }
  }

  let chunks = 0
  let others = 0
  for (const row of rows) {
    if (isChunkRow(row)) chunks++
    else if (row && typeof row === 'object' && row.type !== 'session') others++
  }
  const ratio = chunks + others === 0 ? 0 : chunks / (chunks + others)
  if (chunks < cfg.minChunkRows || ratio < cfg.minRatio) {
    return { file, status: 'below-threshold', chunks, ratio: Number(ratio.toFixed(3)), mb: Number((stat.size / 1048576).toFixed(2)) }
  }

  let pruned
  try {
    pruned = pruneRows(rows)
  } catch (err) {
    return { file, status: 'prune-failed', error: String(err.message) }
  }

  const problems = verifyPruned(pruned)
  if (problems.length > 0) {
    // 校验不过就不写。宁可留着大文件，也不换一份坏文件进去。
    return { file, status: 'verify-failed', chunks, problems: problems.slice(0, 5) }
  }

  const outText = [JSON.stringify(pruned.header)].concat(pruned.events.map((e) => JSON.stringify(e))).join('\n') + '\n'
  const packed = compressFrames(outText)

  if (cfg.dryRun) {
    return {
      file,
      status: 'would-vacuum',
      beforeMB: Number((stat.size / 1048576).toFixed(2)),
      afterMB: Number((packed.length / 1048576).toFixed(2)),
      chunks,
      kept: pruned.stats.keptEvents,
      dropped: pruned.stats.dropped,
    }
  }

  // 备份 -> 原子替换（临时文件 + rename）
  if (!fs.existsSync(file + '.orig')) fs.copyFileSync(file, file + '.orig')
  const tmp = file + '.vacuum-tmp'
  fs.writeFileSync(tmp, packed)
  fs.renameSync(tmp, file)

  const after = fs.statSync(file)
  return {
    file,
    status: 'vacuumed',
    beforeMB: Number((stat.size / 1048576).toFixed(2)),
    afterMB: Number((after.size / 1048576).toFixed(2)),
    chunks,
    kept: pruned.stats.keptEvents,
    dropped: pruned.stats.dropped,
  }
}

let running = false

/**
 * 把一次扫描的逐文件结果汇总成计数。
 *
 * ★ 这里的每个桶都直接来自 vacuumOne() 真实会返回的 status 字符串，
 *   没有新造任何状态。分桶只影响【怎么报告】，不影响【清理什么】。
 *
 * @param results - 每个被扫描文件的 {status, ...}。
 * @returns 计数字段，可直接展开进 run-end / 报告。
 */
function tallyRun(results) {
  const count = (status) => results.filter((r) => r.status === status).length
  const skippedLive = count('skipped-live')
  const belowThreshold = count('below-threshold')
  const vacuumed = count('vacuumed')
  const wouldVacuum = count('would-vacuum')
  const verifyFailed = count('verify-failed')
  const unreadable = results.filter((r) => r.status === 'unreadable' || r.status === 'unreadable-tail').length
  const errored = results.filter((r) => r.status === 'error' || r.status === 'prune-failed').length
  // handled 保持它原来的含义：这一轮真的动过（或本来会动）的文件数。
  const handled = vacuumed + wouldVacuum + verifyFailed + unreadable + errored
  return {
    scanned: results.length,
    handled,
    vacuumed,
    wouldVacuum,
    skippedLive,
    belowThreshold,
    unreadable,
    other: results.length - handled - skippedLive - belowThreshold,
  }
}

/**
 * 跑一轮维护（开机定时器 / Host 工具 / web 路由共用同一条受保护通道）。
 *
 * @param ctx - 保留参数，与历史调用方式一致（不用它，但也不改签名）。
 * @param options - 可选。
 *   dryRun   - 覆盖 cfg.dryRun（按需入口用它做预览）。
 *   fileLimit - 覆盖 cfg.maxFilesPerRun。
 *   trigger  - 记进日志的来源标记：'boot' | 'tool' | 'route'。
 * @returns 逐文件结果数组（含每个文件为什么被跳过）；已在跑则返回 null。
 */
async function runVacuum(ctx, options = {}) {
  if (running) return null
  running = true
  const cfg = readConfig()
  const effective = { ...cfg }
  if (typeof options.dryRun === 'boolean') effective.dryRun = options.dryRun
  if (Number.isFinite(options.fileLimit) && options.fileLimit >= 0) effective.fileLimit = options.fileLimit
  const limit = Number.isFinite(effective.fileLimit) ? effective.fileLimit : cfg.maxFilesPerRun
  const results = []
  try {
    const root = path.join(dshHome(), 'sessions')
    const files = listSessionLogs(root)
    files.sort((a, b) => {
      try {
        return fs.statSync(b).size - fs.statSync(a).size
      } catch {
        return 0
      }
    })
    logLine({ mark: 'run-start', files: files.length, dryRun: effective.dryRun, trigger: options.trigger || 'boot' })
    let handled = 0
    for (const file of files) {
      if (handled >= limit) break
      let result
      try {
        result = vacuumOne(file, effective)
      } catch (err) {
        result = { file, status: 'error', error: String(err.message) }
      }
      results.push(result)
      if (result.status !== 'skipped-live' && result.status !== 'below-threshold') {
        handled++
        logLine({ mark: 'file', ...result })
      }
      // 每个文件之间让出事件循环，别拖慢 DSH
      await new Promise((resolve) => setImmediate(resolve))
    }
    // ★ 计数就是这次改动的重点：以前只看得到 handled，56 个被跳过的文件为什么被跳过
    //   完全不可见。这里把每个原因各多少个记下来 —— 行为一字未改，只是不再沉默。
    logLine({ mark: 'run-end', ...tallyRun(results) })
    return results
  } catch (err) {
    logLine({ mark: 'run-error', error: String(err && err.message ? err.message : err) })
    return results
  } finally {
    running = false
  }
}

// ---------------------------------------------------------------- 按需清理

/** 跳过原因的英文说法。用的是 vacuumOne() 真实的 status 字符串，没有新造。 */
const STATUS_TEXT = {
  'would-vacuum': 'would clean (dry run)',
  vacuumed: 'cleaned',
  'skipped-live': 'SKIPPED - modified inside liveWindowMs, so DSH may be writing it right now',
  'below-threshold': 'SKIPPED - not enough streaming fragments to be worth a rewrite',
  'verify-failed': 'SKIPPED - the pruned log failed the loader contract, so nothing was written',
  unreadable: 'SKIPPED - not a readable zstd frame stream',
  'unreadable-tail': 'SKIPPED - the frame walk could not account for the tail, so nothing was written',
  unparsable: 'SKIPPED - a decoded line is not JSON, so nothing was written',
  'prune-failed': 'SKIPPED - the session header row is missing',
  error: 'SKIPPED - the file could not be processed at all',
}

/**
 * 同一份报告文本，工具和 web 路由共用（避免"同一件事两套实现"）。
 *
 * 返回纯文本，不返回 JSON：调用方是模型，它要读的是"清了多少、跳了多少、为什么跳"，
 * 而不是一个要它自己解析的结构。
 *
 * @param results - runVacuum() 的逐文件结果；null 表示已经有一轮在跑。
 * @param cfg - 生效的配置（报告用 dryRun / liveWindowMs）。
 * @param root - 被扫描的 sessions 目录（人话用）。
 * @returns 报告的纯文本。
 */
function buildVacuumReport(results, cfg, root) {
  const lines = []
  const dry = cfg.dryRun !== false
  if (results === null) {
    lines.push('session-vacuum: a pass was already running, so this call did nothing.')
    lines.push('Wait for it to finish (its lines land in ' + cfg.logFile + ') and call again.')
    return lines.join('\n')
  }

  const tally = tallyRun(results)
  const cleaned = results.filter((r) => r.status === 'vacuumed' || r.status === 'would-vacuum')
  const skipped = results.filter((r) => r.status === 'skipped-live' || r.status === 'below-threshold')
  const limit = Number.isFinite(cfg.fileLimit) ? cfg.fileLimit : cfg.maxFilesPerRun
  const notScanned = Math.max(0, tally.scanned - limit)

  lines.push(dry ? 'session-vacuum: DRY RUN - nothing was written.' : 'session-vacuum: live pass finished.')
  lines.push('Scanned ' + tally.scanned + ' session log(s) under ' + root + '.')
  lines.push(dry
    ? 'Would clean: ' + cleaned.length + ' log(s).'
    : 'Cleaned: ' + tally.vacuumed + ' log(s).')

  let total = 0
  for (const r of cleaned.slice(0, REPORT_NAME_LIMIT)) {
    total += Math.max(0, (r.beforeMB || 0) - (r.afterMB || 0))
    lines.push('  - ' + r.file + ': ' + r.beforeMB + ' MB -> ' + r.afterMB + ' MB (saves '
      + (r.beforeMB - r.afterMB).toFixed(2) + ' MB), ' + r.chunks + ' fragment row(s) dropped, '
      + r.kept + ' event(s) kept')
  }
  if (cleaned.length > REPORT_NAME_LIMIT) {
    lines.push('  - ... and ' + (cleaned.length - REPORT_NAME_LIMIT) + ' more (see the log)')
  }
  lines.push(dry
    ? 'Total that a real pass would save: ' + total.toFixed(2) + ' MB.'
    : 'Total saved: ' + total.toFixed(2) + ' MB.')
  if (notScanned > 0) {
    // 不是"跳过原因"：那些文件根本没被看。但也不能凭空消失 —— 否则报告又变成另一种沉默。
    lines.push('not scanned: ' + notScanned + ' log(s) - maxFilesPerRun is ' + limit
      + ', so the biggest logs are handled first and the rest wait for the next pass.')
  }

  lines.push('Skipped: ' + (tally.skippedLive + tally.belowThreshold) + ' log(s), by reason:')
  lines.push('  - skippedLive: ' + tally.skippedLive + ' - modified within the last '
    + Math.round((cfg.liveWindowMs || 0) / 60000) + ' minute(s), so DSH may still be writing it.')
  lines.push('  - belowThreshold: ' + tally.belowThreshold + ' - under minChunkRows ('
    + cfg.minChunkRows + ') or minRatio (' + cfg.minRatio + ').')
  for (const r of skipped.slice(0, REPORT_NAME_LIMIT)) {
    lines.push('  - ' + r.file + ': ' + (STATUS_TEXT[r.status] || r.status)
      + (r.chunks === undefined ? '' : ' (' + r.chunks + ' fragment row(s), ratio ' + r.ratio + ')'))
  }
  // 被阈值/存活判定之外的原因拒掉的文件同样要说清楚 —— 它们一样是"什么都没干"。
  const refused = results.filter((r) => r.status !== 'vacuumed' && r.status !== 'would-vacuum'
    && r.status !== 'skipped-live' && r.status !== 'below-threshold')
  if (refused.length > 0) {
    lines.push('Could not touch: ' + refused.length + ' log(s), by reason:')
    for (const r of refused.slice(0, REPORT_NAME_LIMIT)) {
      lines.push('  - ' + r.file + ': ' + (STATUS_TEXT[r.status] || r.status)
        + (r.error === undefined ? '' : ' (' + r.error + ')'))
    }
  }
  lines.push('run-end counters (' + cfg.logFile + '): scanned=' + tally.scanned + ', handled='
    + tally.handled + ', vacuumed=' + tally.vacuumed + ', wouldVacuum=' + tally.wouldVacuum
    + ', skippedLive=' + tally.skippedLive + ', belowThreshold=' + tally.belowThreshold
    + ', unreadable=' + tally.unreadable + ', other=' + tally.other)

  // ★ 这条必须留着：调用方多半正想清【自己】那个会话。DSH 把它握在内存里，
  //   从运行中的 DSH 内部去重写它的日志文件可能把它写坏。
  lines.push('NOTE: the session you are running in cannot be cleaned from inside a running DSH.')
  lines.push('  DSH holds that session in memory, and rewriting its log underneath it can corrupt it.')
  lines.push('  The liveWindowMs guard (' + Math.round((cfg.liveWindowMs || 0) / 60000)
    + ' min) already refuses it, so this pass cannot touch it - that is the only protection, and it was not weakened.')
  lines.push('  What to do instead: stop DSH and run E:\\Dsh百宝箱\\清理会话日志.cmd,')
  lines.push('  or leave it idle for at least liveWindowMs (' + Math.round((cfg.liveWindowMs || 0) / 60000)
    + ' min) and let a later boot clean it.')
  return lines.join('\n')
}

/** 参数 schema：两个入口共用。全部可选，不传就是"按配置跑一次"。 */
const ARGUMENTS_SPEC = {
  dryRun: {
    type: 'boolean',
    description: 'Preview only: decode, prune and verify every candidate log, report what a real pass '
      + 'would save, and write nothing. Default false (runs the pass the config allows).',
  },
}

const TOOL_DESCRIPTION = 'Run one session-log cleanup pass right now instead of waiting for the boot-time run, '
  + 'and report per-file what was cleaned and why every other log was skipped. Session logs accumulate dead '
  + 'streaming fragments (assistant/chunk and the packed chunk rows) that nothing reads; this deletes them and '
  + 'reports before -> after MB. It runs the SAME guarded pass as the boot timer: it never touches a log '
  + 'modified inside liveWindowMs and it never bypasses the thresholds. The session the caller is running in '
  + 'cannot be cleaned from inside a running DSH - stop DSH and run E:\\Dsh百宝箱\\清理会话日志.cmd instead.'

/**
 * 工具的实际执行体（同时也被测试直接调用）。
 *
 * @param args - {dryRun?}.
 * @returns 纯文本报告（不是 JSON）。
 */
async function runCleanSessionLogs(args) {
  const cfg = readConfig()
  if (cfg.enabled !== true) {
    return [
      'session-vacuum is disabled in this profile (enabled: false in ' + configPath() + '), so nothing ran.',
      'The plugin is inert by design on a fresh install: no timer, no scan.',
      'To use this tool, set "enabled": true in that file and restart DSH.',
    ].join('\n')
  }
  const dryRun = args && args.dryRun === true
  const root = path.join(dshHome(), 'sessions')
  const results = await runVacuum(undefined, {
    dryRun: dryRun ? true : cfg.dryRun,
    fileLimit: cfg.maxFilesPerRun,
    trigger: 'tool',
  })
  const effective = { ...cfg, dryRun: dryRun ? true : cfg.dryRun }
  const report = buildVacuumReport(results, effective, root)
  if (dryRun) return report
  return report + '\nCall again with dryRun: true to preview instead of writing.'
}

/**
 * 造一个工具定义。
 *
 * 这个 build 从插件里【可以】import @deepseek-ai/dsh-tools（实测：在 profile 的
 * 依赖树里能解析到），所以优先走官方 defineTool —— 它会把参数 schema 编译好并在
 * execute 之前做严格校验。万一某个 build 解析不到这个包，就退回本插件一直在用的
 * 裸对象形态（dsh-real-time-computer-use 就是这么注册的），而不是让工具消失。
 *
 * @param defineTool - defineTool 或 null。
 * @returns 可交给 ctx.tools.register 的定义。
 */
function buildTool(defineTool) {
  // ★ 参数/输出用的是 DSH 的 schema DSL，不是裸 JSON Schema：必填是【属性上】的
  //   required: true，根上的 required 数组会被 defineTool 直接拒掉
  //   （UNSUPPORTED_SCHEMA: schema.required is not supported by the value schema DSL）。
  //   实测踩过一次，所以这里照 dsh-tool-fs / dsh-tool-todo 的写法来。
  const output = {
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: { report: { type: 'string', required: true } },
    },
    render: (_args, value) => [{ type: 'text', text: value.report }],
  }
  if (typeof defineTool === 'function') {
    return defineTool({
      name: TOOL_NAME,
      description: TOOL_DESCRIPTION,
      parameters: ARGUMENTS_SPEC,
      output,
      async execute(args) {
        return { report: await runCleanSessionLogs(args) }
      },
    })
  }
  return {
    name: TOOL_NAME,
    description: TOOL_DESCRIPTION,
    parameters: { type: 'object', properties: { dryRun: { type: 'boolean' } } },
    output,
    async execute(args) {
      return { report: await runCleanSessionLogs(args) }
    },
  }
}

/** 读一个请求体（路由用，带上限）。 */
function readRequestBody(req, limitBytes = 65536) {
  return new Promise((resolve) => {
    const parts = []
    let size = 0
    req.on('data', (chunk) => {
      size += chunk.length
      if (size > limitBytes) {
        resolve('')
        return
      }
      parts.push(chunk)
    })
    req.on('end', () => resolve(Buffer.concat(parts).toString('utf8')))
    req.on('error', () => resolve(''))
  })
}

/**
 * web 路由：POST /api/local/session-vacuum/run
 *
 * ★ 只允许 dry run。真正的清理留给 Host 工具 —— 它有 agent 在旁边看着，而且这是
 *   "同一件事两个实现"最容易失控的地方：路由这边不给自己留任何破坏性入口。
 */
function vacuumRoute(ctx) {
  return {
    kind: 'exact',
    path: ROUTE_PATH,
    handler: async (req, res) => {
      const send = (code, text) => {
        res.writeHead(code, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' })
        res.end(text + '\n')
      }
      if (String(req.method || '').toUpperCase() !== 'POST') {
        send(405, 'session-vacuum: this route is POST-only (it always runs the scan). Send {} or {"dryRun":true}.')
        return
      }
      const cfg = readConfig()
      if (cfg.enabled !== true) {
        send(403, 'session-vacuum is disabled (enabled: false in ' + configPath() + '); nothing ran.')
        return
      }
      if (cfg.dryRun !== true) {
        send(403, 'session-vacuum: refusing to run a non-dry pass over HTTP (this profile has dryRun: false). '
          + 'A real pass is only run by the ' + TOOL_NAME + ' tool, where an agent owns the call. '
          + 'Set "dryRun": true to use this route as a preview.')
        return
      }
      const body = (await readRequestBody(req)).trim()
      let dryRun = true
      if (body.length > 0) {
        try {
          dryRun = JSON.parse(body).dryRun !== false
        } catch {
          send(400, 'session-vacuum: the body must be JSON, e.g. {} or {"dryRun":true}.')
          return
        }
      }
      if (dryRun !== true) {
        send(403, 'session-vacuum: this route is preview-only; the body may not ask for a real pass.')
        return
      }
      const root = path.join(dshHome(), 'sessions')
      const results = await runVacuum(undefined, { dryRun: true, fileLimit: cfg.maxFilesPerRun, trigger: 'route' })
      send(200, buildVacuumReport(results, { ...cfg, dryRun: true }, root))
    },
  }
}

/**
 * 只尝试解析一次 @deepseek-ai/dsh-tools（模块加载期已经试过一次，这里是给
 * "加载时没解析到"的 build 一个兜底路径）。
 *
 * @returns Promise<{ok: true, defineTool: Function|null} | {ok: false, error: string}>
 */
let toolModulePromise = null
function loadToolModule() {
  if (toolModulePromise === null) {
    toolModulePromise = import(TOOLS_PACKAGE)
      .then((mod) => ({ ok: true, defineTool: typeof mod.defineTool === 'function' ? mod.defineTool : null }))
      .catch((err) => ({ ok: false, error: String(err && err.message ? err.message : err) }))
  }
  return toolModulePromise
}

/**
 * apply() 的工具注册段。
 *
 * 参数用【具名对象】：这里有两个回调（register 与 deactivate），位置传参写错顺序不会
 * 报错，只会静默地把注册当成撤销用 —— 排查成本极高。具名之后写错名字就是 undefined。
 *
 * ★ 注入点是 defineTool 本身，而不是"注册器"：测试可以传一个假的 defineTool 来模拟
 *   解析不到 dsh-tools 的那个 build，注册仍然走 ctx.tools.register 这一条真路。
 *   （早期版本注入的是注册器，结果测试拿一个 identity 函数把 buildTool 的输出直接吞掉了，
 *     看起来像"注册成功"其实什么都没注册 —— 这就是那个假验证的教训。）
 *
 * @param options.ctx - 宿主上下文。
 * @param options.register - 收一个工具定义并注册它；返回撤销函数（插件已撤销则立刻撤销）。
 * @param options.deactivate - 记一行 deactivated 并撤掉一切（与同步失败同一条路）。
 * @param options.defineTool - 可选：defineTool 的替代实现，只为离线测试注入。
 * @throws 当这个 build 根本没有 ctx.tools 时（由调用方记一行然后停用）。
 */
function registerTool(options) {
  const ctx = options.ctx
  const register = options.register
  const deactivate = options.deactivate
  if (!ctx || !ctx.tools || typeof ctx.tools.register !== 'function') {
    throw new Error('ctx.tools.register is not available in this DSH build')
  }
  const defineTool = typeof options.defineTool === 'function'
    ? options.defineTool
    : (typeof ctx.tools.defineTool === 'function'
      ? ctx.tools.defineTool
      : bundledDefineTool)
  if (defineTool !== null && defineTool !== undefined) {
    register(buildTool(defineTool))
    return
  }
  // 连加载期都没解析到：再试一次动态 import，这一次只能在 import 落地后注册。
  //
  // ★ 拿不到 defineTool 不是"什么都做不了"：裸对象形态照样能注册。真注册不了才停用，
  //   而且必须走和同步失败【同一条】路（记 deactivated + 撤掉一切），不能只写一行
  //   说明就悄悄继续跑 —— 那等于插件"停用"了却还在后台扫会话日志。
  loadToolModule().then((res) => {
    try {
      register(buildTool(res && res.ok ? res.defineTool : null))
    } catch (err) {
      deactivate('ctx.tools', err)
    }
  }).catch((err) => deactivate('ctx.tools', err))
}

/**
 * @param ctx - 宿主上下文。
 * @param toolDefineTool - 可选：defineTool 的替代实现（只为离线测试注入那条
 *   "解析不到 dsh-tools" 的失败路径；它只在冷启动的一瞬间存在，否则测不到）。
 */
function apply(ctx, toolDefineTool) {
  const disposers = []
  const cfg = readConfig()
  if (!cfg.enabled) {
    // 装上默认什么都不做，连定时器都不起
    return
  }

  // 加固（见 README「要求 / 兼容性」）：插件【仅仅是不兼容】不该把整个 profile 拖垮 ——
  // 第三方插件 cancanqiqi/dsh-human-subagent 就是调了本 build 没有的服务方法，异常从
  // apply() 冒进加载器，整个 profile 起不来。所以这个 build 没有 ctx.effect 时，走同一条
  // "记一行日志然后停用" 的路，而不是抛异常。只是失败路径：一切都在时行为一字未改。
  if (!ctx || typeof ctx.effect !== 'function') {
    logLine({ mark: 'deactivated', reason: 'registration-failed', step: 'services', error: 'ctx.effect is not available in this DSH build' })
    return
  }

  const timer = setTimeout(() => {
    // 开机这一轮走的是同一条 runVacuum()，只是不带任何覆盖参数。
    runVacuum(ctx, { trigger: 'boot' }).catch(() => {})
  }, cfg.startDelayMs)
  if (typeof timer.unref === 'function') timer.unref()

  disposers.push(() => clearTimeout(timer))

  let disposed = false
  const addDisposer = (dispose) => {
    if (typeof dispose !== 'function') return
    if (disposed) {
      try {
        dispose()
      } catch {}
      return
    }
    disposers.push(dispose)
  }
  const deactivate = (step, err) => {
    disposed = true
    for (const dispose of disposers) {
      try {
        dispose()
      } catch {}
    }
    logLine({ mark: 'deactivated', reason: 'registration-failed', step, error: String(err && err.message ? err.message : err) })
  }

  // ---- 撤销点先挂上 ----
  //
  // ★ 顺序有讲究：ctx.effect 必须在两个注册点【之前】登记。apply() 自己中途失败时
  //   （工具服务缺失 / register 抛异常），宿主手上必须已经有一个可调用的撤销点，
  //   否则那个刚挂上的开机定时器就没人能撤了 —— 插件"停用"了却还在后台扫会话日志。
  //   这也是加固的一部分：失败路径也要干净，而不只是"不抛异常"。
  try {
    ctx.effect(() => () => {
      disposed = true
      for (const dispose of disposers) {
        try {
          dispose()
        } catch {}
      }
    })
  } catch (err) {
    deactivate('ctx.effect', err)
    return
  }

  // ---- Host 工具：让 agent 当场问一次，而不是等下次开机 ----
  // 失败路径：这个 build 没有 ctx.tools（或者 register 抛了）=> 记一行然后停用，
  // 绝不把异常放出去（见上面的加固说明）。
  //
  // ★ 这里传给 registerTool 的必须是【注册函数】（调 ctx.tools.register 并收下它返回的
  //   撤销函数），而不是 addDisposer 本身。曾经把 addDisposer 直接传进去过 —— 于是
  //   "注册"变成了"把工具定义当撤销函数收下"，addDisposer 看它不是函数就丢掉了：
  //   插件报告一切正常、工具却根本没进注册表。它不报错，只是安静地什么都不做。
  const registerToolDef = (def) => {
    addDisposer(ctx.tools.register(def))
  }
  try {
    registerTool({
      ctx,
      register: registerToolDef,
      deactivate,
      ...(typeof toolDefineTool === 'function' ? { defineTool: toolDefineTool } : {}),
    })
  } catch (err) {
    deactivate('ctx.tools', err)
    return
  }

  // ---- web 路由：只读预览，给脚本用 ----
  //
  // ★ webServer 用【可选注入】：它没写进 inject（headless profile 里没有这个服务），
  //   所以不能直接读 ctx.webServer —— 那会抛。ctx.inject 的回调在有这个服务时才跑。
  //   路由注册失败不算致命：工具还在，插件仍然可用。
  const registerRoute = (webCtx) => {
    try {
      if (webCtx && webCtx.webServer && typeof webCtx.webServer.register === 'function') {
        addDisposer(webCtx.webServer.register(vacuumRoute(webCtx)))
      }
    } catch (err) {
      logLine({ mark: 'route-failed', path: ROUTE_PATH, error: String(err && err.message ? err.message : err) })
    }
  }
  if (typeof ctx.inject === 'function') {
    try {
      ctx.inject(['webServer'], registerRoute)
    } catch (err) {
      // 可选注入本身不可用：没有路由，别的都还在。
      logLine({ mark: 'route-failed', path: ROUTE_PATH, error: 'ctx.inject unavailable: ' + String(err && err.message ? err.message : err) })
    }
  }
}

export { name, inject, apply }

// ---------------------------------------------------------------------------
// Internals, exported ADDITIVELY for the offline test suite
// (test/vacuum-tests.mjs). A DSH plugin is loaded by reading `name`, `inject`
// and `apply`, so exporting more names changes no runtime behaviour.
// 只为离线测试导出内部函数，不改动任何运行时行为。
export {
  DEFAULTS,
  CHUNK_TYPES,
  SURFACE_TYPES,
  TOOL_NAME,
  ROUTE_PATH,
  STATUS_TEXT,
  dshHome,
  configPath,
  readConfig,
  isChunkRow,
  illegalRange,
  dropIllegal,
  pruneRows,
  verifyPruned,
  frameLength,
  decompressFrames,
  compressFrames,
  listSessionLogs,
  vacuumOne,
  runVacuum,
  tallyRun,
  buildVacuumReport,
  buildTool,
  registerTool,
  loadToolModule,
  runCleanSessionLogs,
  vacuumRoute,
}
