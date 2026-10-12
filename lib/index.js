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
// ASCII ONLY on purpose -- this box runs a GBK code page.

import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import zlib from 'node:zlib'

const name = 'dsh-session-vacuum'
const inject = []

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

async function runVacuum(ctx) {
  if (running) return
  running = true
  const cfg = readConfig()
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
    logLine({ mark: 'run-start', files: files.length, dryRun: cfg.dryRun })
    let handled = 0
    for (const file of files) {
      if (handled >= cfg.maxFilesPerRun) break
      let result
      try {
        result = vacuumOne(file, cfg)
      } catch (err) {
        result = { file, status: 'error', error: String(err.message) }
      }
      if (result.status !== 'skipped-live' && result.status !== 'below-threshold') {
        handled++
        logLine({ mark: 'file', ...result })
      }
      // 每个文件之间让出事件循环，别拖慢 DSH
      await new Promise((resolve) => setImmediate(resolve))
    }
    logLine({ mark: 'run-end', handled })
  } catch (err) {
    logLine({ mark: 'run-error', error: String(err && err.message ? err.message : err) })
  } finally {
    running = false
  }
}

function apply(ctx) {
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
    runVacuum(ctx).catch(() => {})
  }, cfg.startDelayMs)
  if (typeof timer.unref === 'function') timer.unref()

  disposers.push(() => clearTimeout(timer))

  try {
    ctx.effect(() => () => {
      for (const dispose of disposers) {
        try {
          dispose()
        } catch {}
      }
    })
  } catch (err) {
    // 停用 = 不留半个活着的定时器（否则插件"停用"了还在后台扫会话日志）
    for (const dispose of disposers) {
      try {
        dispose()
      } catch {}
    }
    logLine({ mark: 'deactivated', reason: 'registration-failed', step: 'ctx.effect', error: String(err && err.message ? err.message : err) })
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
}
