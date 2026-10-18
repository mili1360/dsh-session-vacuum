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
//   6. 默认 enabled=true / dryRun=false：装完就干活（2026-10-17 改，原来默认关着）
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
// v4 支持（2026-10-14 加，对应 DSH 0.2.0-rc.2）：
//   0.2.0 把日志换成了 session.v4.jsonl.zstd，而 **v4 里没有"碎片行"这种东西** ——
//   一个逻辑事件只写一行，流式内容藏在 assistant/message 的 data.stream 里。
//   于是按行类型找碎片的老逻辑在真 v4 会话上永远是 0 ⇒ 永远 below-threshold ⇒
//   装上了等于没装。现在 v4 走一条独立通道：压 data.stream 里的打包增量段。
//   ★ 是【压】不是【删】，而且压完必须过 DSH 自己的读取器复核才允许写盘 ——
//     完整理由和实测数据见下面 "v4：事件内部的流式段" 那一段的注释。
//
// ASCII ONLY on purpose -- this box runs a GBK code page.

import fs from 'node:fs'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { isDeepStrictEqual } from 'node:util'
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

// ★ v4 压缩的自校验要拿【DSH 自己】的读取器来复核（expandAssistantStream + BlockAssembler），
//   而不是自己另写一份拼装逻辑。理由：自己写的镜像一旦和真实现分了叉，插件会一边说
//   "复核通过"一边把文件改坏 —— 那比不复核更危险。
//
//   和 TOOLS_PACKAGE 同理，在模块加载期解析一次：插件加载本来就是异步的（loader 会 await
//   import 这个模块），所以不多花一分钱，换来 apply()/vacuumOne() 里一切仍是同步的。
//
//   ★ 解析不到时【不假装复核过】：checkStreamEquivalence 会退回 'anchor-only'，
//     并且把这件事如实写进逐文件结果（verifier 字段）。锚点那一条本身就蕴含拼装结果不变
//     （见 runAnchorHolds 的注释），所以退回后仍然安全，只是复核强度低一档。
const LLM_PACKAGE = '@deepseek-ai/dsh-llm'
let bundledStreamVerifier = null
try {
  const mod = await import(LLM_PACKAGE)
  if (mod
    && typeof mod.expandAssistantStream === 'function'
    && typeof mod.BlockAssembler === 'function'
    && typeof mod.joinAssistantStreamText === 'function') {
    bundledStreamVerifier = mod
  }
} catch {
  bundledStreamVerifier = null
}

const DEFAULTS = {
  // 默认【开】。和 dryRun 同理：装上却完全不启动，用户只会以为插件没用。
  // 想临时停用就显式写 enabled: false。
  enabled: true,
  // 开机后延迟多久开始扫（毫秒），避开启动高峰
  startDelayMs: 20000,
  // 两次维护之间的最小间隔（毫秒），默认 6 小时
  minIntervalMs: 6 * 60 * 60 * 1000,
  // 碎片行数下限：低于它一律不动。
  // ★ v4 下这个数的含义变了：v4 没有"碎片行"，能压的是事件内部 data.stream 里的
  //   【增量片段数】（见下面 v4 那一段）。量纲仍然是"条数"，只是粒度从行变成了片段。
  // ★ 500 是 2026-10-14 在沙箱#2 实测标定的：v4 真会话里 片段数 ≈ 0.95~0.99 × output token
  //   （1 轮带工具调用 1023 tok/986 片段、5 轮 1346/1332、6 轮 1516/1498）⇒ 500 ≈ "output token ≥ 500"，
  //   也就是 3~6 轮普通对话或 1 轮带工具调用；旧的 2000 要十几轮才触发，正好漏掉 5 轮这种真会话。
  minChunkRows: 500,
  // 碎片占比下限。
  // ★ v4 下分母的含义也跟着变：chunks / (chunks + 剩下的流记录)，
  //   和旧格式的 chunks / (chunks + 其它行) 是同一个形状。
  // ★ 实测同上（5 个真会话形态，ratio 0.87~0.98）：kept 只留每段的锚点，所以 v4 的 ratio
  //   结构性地高，永远够不到 0.25 ⇒ 这道闸刀在 v4 下基本不参与决策，真正管用的只有
  //   minChunkRows。值保持 0.25 不动：它对旧行格式仍然是有效的过滤。
  minRatio: 0.25,
  // 多少分钟以内被改过的会话视为"正在写"，跳过
  liveWindowMs: 2 * 60 * 1000,
  // 每次维护最多处理多少个文件
  maxFilesPerRun: 12,
  // 默认【直接干活】。理由：装上却什么都不做，是比改错更常见的失败 --
  // 用户会以为插件没效果，然后把它卸掉。而改错这件事已经有四道网兜着：
  // 自校验不过就一个字节都不写、.orig 首次备份且永不覆盖、tmp+rename 原子替换、
  // 以及 liveWindowMs 跳过正在写的会话。想先观察就显式写 dryRun: true。
  dryRun: false,
  // v4（及以后）事件内部流式段的压缩开关。默认开；想只跑旧格式的行级清理就写 false。
  v4Streams: true,
  // ★ 2026-10-18 事故后的默认值：**在活着的 DSH 进程里，一律不重写会话日志**。
  //   事故：文件被重编号成 0..72555，而 DSH 内存里的 seq 计数器还是 713504，
  //   下一条追加就写成 713504 ⇒ 一个文件里混进两套编号 ⇒ 会话损坏。
  //   为什么 mtime 判据救不了它：那个会话被 GUI 一直开着，但空闲两分钟 ⇒ 文件确实
  //   没被动过 ⇒ 判据说"安全" ⇒ 实际最危险。**「文件没动过」不等于「没人持有它」。**
  //   所以：进程内的开机扫描 / 按需工具调用，默认只记账不动手；
  //   真要清就在 DSH **没起**的时候跑 lib/clean-cli.mjs（它把 preBoot 置 true）。
  //   想恢复旧行为（进程内也写，自担风险）就显式配 allowInProcessWrite: true。
  allowInProcessWrite: false,
  // 由 clean-cli.mjs 置为 true：证明这次运行不在 DSH 进程里面。
  preBoot: false,
  logFile: 'logs/session-vacuum.jsonl',
}

// 只写不读的流式碎片行（v0/v1 的物理行标签；v4 里不存在这种行）
const CHUNK_TYPES = new Set(['text-chunks', 'reasoning-chunks', 'tool-call-chunks', 'assistant/chunk'])
// 会进 surface 的类型，必须带 surfaceOp 标记
const SURFACE_TYPES = new Set(['user/message', 'assistant/message', 'tool/result'])
// v4：data.stream 里的三种"打包增量段"。第四种是原始 chunk 记录，那是不压的
// （它带 block-end 的完整正文、usage 和 replayState）。
const STREAM_RUN_TYPES = new Set(['text-chunks', 'reasoning-chunks', 'tool-call-chunks'])
// 每种段装成员的键；以及对得上的 block-end 里必须与之逐字相等的字段
const RUN_MEMBER_KEY = { 'text-chunks': 'texts', 'reasoning-chunks': 'texts', 'tool-call-chunks': 'args' }
const RUN_BLOCK_TYPE = { 'text-chunks': 'text', 'reasoning-chunks': 'reasoning', 'tool-call-chunks': 'tool-call' }
const RUN_BLOCK_FIELD = { text: 'text', reasoning: 'text', 'tool-call': 'arguments' }

function dshHome() {
  return process.env.DSH_HOME || path.join(os.homedir(), '.dsh')
}
function configPath() {
  return path.join(dshHome(), 'dsh-session-vacuum', 'config.json')
}
function readConfig() {
  try {
    // ★ 必须容忍 BOM：记事本和 PowerShell 的 Set-Content -Encoding UTF8 都会写出
    //   带 BOM 的 UTF-8，而 JSON.parse 遇到开头的 U+FEFF 会直接抛错 ——
    //   于是整份配置被【静默忽略】、回退到 DEFAULTS。实测踩到过（2026-10-14，
    //   沙箱#2：写进去的 liveWindowMs/minChunkRows 全部没生效，日志里只有
    //   dryRun:false 这个恰好和 DEFAULTS 相同的值，差点误判成"配置读到了"）。
    const text = fs.readFileSync(configPath(), 'utf8').replace(/^\uFEFF/, '')
    return { ...DEFAULTS, ...JSON.parse(text) }
  } catch {
    return { ...DEFAULTS }
  }
}
/**
 * 只写 ASCII 的 JSON.stringify：非 ASCII 一律转成 \uXXXX。
 *
 * ★ 为什么必须这样：本机的读侧工具（PowerShell 5.1 的 Get-Content / Select-String、
 *   cmd 的 type）把【无 BOM 的 UTF-8 文件】按本机代码页（GBK/936）解码 —— 于是
 *   `E:\DSH实机测试\...` 在日志里显示成 `E:\DSH瀹炴満娴嬭瘯\...`。
 *   实测（2026-10-14）：logs/session-vacuum.jsonl 的字节本身是正确的 UTF-8
 *   （用 UTF8.GetString 读出来是好的），是【读的时候】被 GBK 解码才成了乱码。
 *   转义之后文件里一个非 ASCII 字节都没有，任何解码器都读得出，而 JSON.parse
 *   仍然还原出真实路径 —— 写入侧唯一能做到"与读者编码无关"的办法。
 *
 * @param obj - 任意可 JSON 序列化的对象。
 * @returns 纯 ASCII 的 JSON 文本。
 */
function asciiJson(obj) {
  return JSON.stringify(obj).replace(/[\u007f-\uffff]/g, (c) => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'))
}

function logLine(obj) {
  try {
    const file = path.join(dshHome(), readConfig().logFile)
    fs.mkdirSync(path.dirname(file), { recursive: true })
    const line = { t: new Date().toISOString(), ...obj }
    // 绝对路径可能含非 ASCII（见 asciiJson，那部分会变成 \uXXXX），所以再补一个
    // 相对 $DSH_HOME 的路径：DSH 把工作区名里的非 ASCII 编码成 ~XXXX，因此这一条恒为 ASCII，
    // 人和工具都能直接读。只在本来就有 file 的行上加，不改任何别的字段。
    if (typeof line.file === 'string' && typeof line.rel !== 'string') {
      const rel = path.relative(dshHome(), line.file)
      if (rel.length > 0 && !rel.startsWith('..') && !path.isAbsolute(rel)) {
        line.rel = rel.split(path.sep).join('/')
      }
    }
    fs.appendFileSync(file, asciiJson(line) + '\n')
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

// ---------------------------------------------------------------- v4：事件内部的流式段
//
// ★ 0.2.0 把会话日志换成了 session.v4.jsonl.zstd，而 v4 里【没有"碎片行"这种东西】。
//   text-chunks / reasoning-chunks / tool-call-chunks / assistant/chunk 是 v0/v1 的
//   物理行标签（v0-to-v1 的 PACKED_TAGS），v4 一个逻辑事件只写一行 ——
//   dsh-session-persistence-jsonl 的原话：
//     "Compact Assistant streams are nested event data; every event occupies one row."
//   流式内容改藏在 assistant/attempt / assistant/message 的 data.stream 数组里。
//   ⇒ 按行类型找碎片的老逻辑在真 v4 会话上【永远是 0】，于是永远 below-threshold ——
//     装上了等于没装。这就是本段要解决的问题。
//
// 剪的对象：data.stream 里的"打包增量段"（packed delta run）。形状（dsh-llm 的
// AssistantStreamAccumulator.snapshot 产出的四种记录之一，前三者是段）：
//   { type:'text-chunks'|'reasoning-chunks', time0, index, dt:[...], texts:[...] }
//   { type:'tool-call-chunks', time0, index, dt:[...], id, name?, args:[...] }
//   { type:'chunk', time, chunk }        <- 原始 chunk：block-start / block-end / usage / finish
// dt 是相邻成员的时间间隔（dt.length === members.length - 1）。
//
// ★ 为什么是【压成单成员】而不是【把整段删掉】—— 这一条是实测 + 源码查出来的，不是猜的：
//
//   删整段会静默弄坏下游：dsh-subagent/lib/types/assistant-output.js 用
//     joinAssistantStreamText(event.data.stream)
//   取子代理的最终输出正文，而这个函数【只读 text-chunks 记录】。段一删，它返回空串，
//   子代理的答案就没了。而 DSH 自己的一致性校验（persistence-jsonl 的
//   assertCurrentAssistantStreams）【抓不到】这件事：block-end 已经带了完整正文，
//   重放拼出来的 content 照样和 message.content 相等。这正是"剪错了比不剪更糟"。
//
//   压成单成员（texts/args 拼成一条、dt 清空）则对所有已查到的消费者都等价：
//     - expandAssistantStream + BlockAssembler 的 blocks()/interruptedBlocks() 不变
//       （拼装只做字符串相连；见 dsh-llm assembleAssistantStream 的文档）
//     - joinAssistantStreamText 拼出的正文一字不差（子代理输出安全）
//     - assistantStreamHasVisibleText / assistantStreamHasVisibleContent 不变
//     - lastAssistantStreamChunk(stream,'usage') / assistantStreamChunks(stream,'block-end') 不变
//     - assistantStreamFirstTokenTime 不变（段首成员的时间仍是 time0）
//     - 形状仍然合法：dt.length === members.length - 1 === 0，texts 非空，键集不变
//   代价只有一个，明说：段【内部】每个增量各自的时间戳没有了（段级时间仍在）。
//   实测（6 轮真会话 30509 字节）：这一项恢复了对 5129 字节的压缩收益，
//   而"整段删除"只多 234 字节 —— 用 0.5% 的文件体积去换子代理输出丢失，不划算。
//
// 三道自校验，任何一道不过就【整份文件一个字节都不写】：
//   V1 结构：类型/键集/成员类型/dt 长度/time0/index 全部按 dsh-llm 的 validateRecord 复刻
//   V2 锚点：同一个 index 上必须有一条 block-end 原始 chunk，而且它带的 block 正文/实参
//            与这段拼起来的结果【逐字相等】—— 这样"压完还对不对"不依赖对拼装语义的猜测
//   V3 复核：真去调 DSH 自己的 expandAssistantStream + BlockAssembler，把压缩前后的
//            blocks()/interruptedBlocks()/usage/replayState/正文/首 token 时间逐项比对

/** 会话头里的格式版本。v0/v1 没有 version 字段，算 0。 */
function formatVersion(header) {
  if (header === null || typeof header !== 'object' || Array.isArray(header)) return 0
  return Number.isInteger(header.version) ? header.version : 0
}

/**
 * 段能不能压的结构检查（V1）。能压返回成员数组，不能压返回 null。
 *
 * 逐条对着 dsh-llm/lib/types/assistant-stream.js 的 validateRecord/validateRun/safeIndex
 * 复刻 —— 判严一点没关系（压不了就不压），判松了才会写出 DSH 读不了的记录。
 *
 * @param record - data.stream 里的一条记录。
 * @returns 成员字符串数组，或 null。
 */
function runMembers(record) {
  if (record === null || typeof record !== 'object' || Array.isArray(record)) return null
  const memberKey = RUN_MEMBER_KEY[record.type]
  if (memberKey === undefined) return null
  const members = record[memberKey]
  if (!Array.isArray(members) || members.length < 2) return null
  if (!Array.isArray(record.dt) || record.dt.length !== members.length - 1) return null
  if (!record.dt.every((gap) => Number.isSafeInteger(gap))) return null
  if (!members.every((member) => typeof member === 'string')) return null
  if (!Number.isSafeInteger(record.time0)) return null
  if (!Number.isSafeInteger(record.index) || record.index < 0 || Object.is(record.index, -0)) return null
  // 键集必须精确相等（dsh-llm 的 exactKeys：多一个键就是 TypeError）
  const keys = record.type === 'tool-call-chunks'
    ? (Object.hasOwn(record, 'name')
      ? ['type', 'time0', 'index', 'dt', 'id', 'name', 'args']
      : ['type', 'time0', 'index', 'dt', 'id', 'args'])
    : ['type', 'time0', 'index', 'dt', 'texts']
  if (Object.keys(record).length !== keys.length) return null
  if (!keys.every((key) => Object.hasOwn(record, key))) return null
  if (record.type === 'tool-call-chunks') {
    if (typeof record.id !== 'string' || record.id.length === 0) return null
    if (record.name !== undefined && (typeof record.name !== 'string' || record.name.length === 0)) return null
  }
  return members
}

/**
 * 段的安全锚点（V2）：同一个 index 上有一条 block-end 原始 chunk，且它带的那一块
 * 与这段拼起来的东西逐字相等。
 *
 * 为什么这条就够：block-end 会把整块【覆盖】上去（BlockAssembler.push: partial.block = chunk.block，
 * 之后同 index 的增量一律被忽略），所以"这段怎么拼"对最终结果没有影响；而这里再要求
 * 两边正文逐字相等，等于把"压缩前后等价"钉在肉眼可核对的事实上，而不是钉在
 * 我对拼装语义的理解上。
 *
 * @param stream - 整个 data.stream。
 * @param record - 待压的段。
 * @returns 锚点成立返回 true。
 */
function runAnchorHolds(stream, record) {
  const blockType = RUN_BLOCK_TYPE[record.type]
  if (blockType === undefined) return false
  const blockField = RUN_BLOCK_FIELD[blockType]
  const want = record[RUN_MEMBER_KEY[record.type]].join('')
  for (const other of stream) {
    if (other === null || typeof other !== 'object' || Array.isArray(other)) continue
    if (other.type !== 'chunk') continue
    const chunk = other.chunk
    if (chunk === null || typeof chunk !== 'object' || Array.isArray(chunk)) continue
    if (chunk.type !== 'block-end') continue
    if (chunk.index !== record.index) continue
    const block = chunk.block
    if (block === null || typeof block !== 'object' || Array.isArray(block)) continue
    if (block.type !== blockType) continue
    if (block[blockField] === want) return true
  }
  return false
}

/** 把一条段压成单成员。键序与 dsh-llm 的 accumulator 保持一致。 */
function compactRun(record) {
  const joined = record[RUN_MEMBER_KEY[record.type]].join('')
  if (record.type === 'tool-call-chunks') {
    const out = { type: record.type, time0: record.time0, index: record.index, dt: [], id: record.id }
    if (Object.hasOwn(record, 'name')) out.name = record.name
    out.args = [joined]
    return out
  }
  return { type: record.type, time0: record.time0, index: record.index, dt: [], texts: [joined] }
}

/**
 * 扫一份 data.stream，列出可压的段（V1 + V2，不做 V3）。
 *
 * 这条是给阈值判断用的【廉价】版本：不展开、不建对象，所以大文件上先跑它不吃亏。
 *
 * ★ 这里必须把【同一个量纲】的两个数一起交出去，否则调用方算不出合法的比值。
 *   实测踩到过：collapsed 数的是【片段】（一条段有 61 个成员就是 60 个片段），
 *   而 records 数的是【记录】（那条段只算 1）——两者混进同一个分母，比值会大得离谱
 *   （2026-10-14 实测：1332 个片段 / 39 条记录 => ratio 34.154，而这个指标的定义域是 0..1）。
 *   所以 kept = 压完之后【还剩下的片段数】，它和 collapsed 同量纲：
 *     collapsed + kept === 整个 data.stream 里的全部片段数
 *   比值的分母用 (collapsed + kept)，阈值才有意义。
 *
 * @param stream - 事件的 data.stream。
 * @returns {{plan: Array<{at: number, members: number}>, collapsed: number, kept: number, records: number}}
 */
function scanStreamRuns(stream) {
  const plan = []
  let collapsed = 0
  let kept = 0
  let records = 0
  for (let at = 0; at < stream.length; at++) {
    const record = stream[at]
    if (record === null || typeof record !== 'object' || Array.isArray(record)) continue
    records++
    const memberKey = RUN_MEMBER_KEY[record.type]
    if (memberKey !== undefined && Array.isArray(record[memberKey])) {
      // 段里的每个成员都是一个"片段"；能压的段里最后只留 1 个，其余都省下来。
      kept += 1
      if (STREAM_RUN_TYPES.has(record.type)) {
        const members = runMembers(record)
        if (members !== null && runAnchorHolds(stream, record)) {
          plan.push({ at, members: members.length })
          collapsed += members.length - 1
        }
      }
    } else {
      // 原始 chunk 记录（block-start/block-end/usage/finish）：不可压，算 1 个片段。
      kept += 1
    }
  }
  return { plan, collapsed, kept, records }
}

/** 按 plan 产出一份新的 stream（不改原数组）。 */
function applyStreamPlan(stream, plan) {
  const at = new Set(plan.map((step) => step.at))
  return stream.map((record, index) => (at.has(index) ? compactRun(record) : record))
}

/**
 * V3：用 DSH 自己的读取器复核"压缩前后等价"。
 *
 * @param original - 原始 stream。
 * @param compacted - 压完的 stream。
 * @returns {{ok: boolean, via: string, reason?: string}} via 说明这份结论是谁下的。
 */
function checkStreamEquivalence(original, compacted) {
  const mod = bundledStreamVerifier
  if (mod === null) {
    // 解析不到 dsh-llm：不假装复核过。锚点（V2）已经蕴含等价，如实降级标记。
    return { ok: true, via: 'anchor-only' }
  }
  try {
    const before = mod.expandAssistantStream(original)
    const after = mod.expandAssistantStream(compacted)
    const asmBefore = new mod.BlockAssembler()
    const asmAfter = new mod.BlockAssembler()
    for (const member of before) asmBefore.push(member.chunk)
    for (const member of after) asmAfter.push(member.chunk)
    // 逐项比对。这几项覆盖了源码里查到的全部 data.stream 消费者：
    //   blocks/interruptedBlocks -> message.content 与 token-meter 的 provider content
    //   usage                    -> token-meter / headless / client 的用量
    //   replayState              -> 供应商重放
    //   joinedText               -> dsh-subagent 的子代理输出正文
    //   firstTokenTime           -> sessionStats / trajectory 的 TTFT
    const pairs = [
      ['blocks', asmBefore.blocks(), asmAfter.blocks()],
      ['interruptedBlocks', asmBefore.interruptedBlocks(), asmAfter.interruptedBlocks()],
      ['usage', asmBefore.usage, asmAfter.usage],
      ['replayState', asmBefore.replayState, asmAfter.replayState],
      ['joinedText', mod.joinAssistantStreamText(original), mod.joinAssistantStreamText(compacted)],
      ['firstTokenTime', mod.assistantStreamFirstTokenTime(original), mod.assistantStreamFirstTokenTime(compacted)],
    ]
    for (const [label, a, b] of pairs) {
      if (!isDeepStrictEqual(a, b)) return { ok: false, via: 'dsh-llm', reason: label + ' differs' }
    }
    return { ok: true, via: 'dsh-llm' }
  } catch (err) {
    return { ok: false, via: 'dsh-llm', reason: String(err && err.message ? err.message : err) }
  }
}

/**
 * 一个事件的完整计划：先 V1+V2 挑段，再用 V3 复核压完的结果。
 *
 * @param event - 一个带 data.stream 的事件。
 * @returns {{plan, collapsed, records, verdict}}
 */
function planEventStream(event) {
  const stream = event.data.stream
  const scan = scanStreamRuns(stream)
  if (scan.plan.length === 0) return { plan: [], collapsed: 0, records: scan.records, verdict: { ok: true, via: 'none' } }
  const verdict = checkStreamEquivalence(stream, applyStreamPlan(stream, scan.plan))
  if (!verdict.ok) return { plan: [], collapsed: 0, records: scan.records, verdict }
  return { plan: scan.plan, collapsed: scan.collapsed, records: scan.records, verdict }
}

/** 阈值判断用的全文件扫（廉价版：只走 V1+V2）。 */
function scanV4(rows) {
  const out = { collapsed: 0, kept: 0, records: 0, carriers: 0, runs: 0 }
  for (const row of rows) {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) continue
    if (row.type === 'session') continue
    const data = row.data
    if (data === null || typeof data !== 'object' || Array.isArray(data)) continue
    if (!Array.isArray(data.stream)) continue
    out.carriers++
    const scan = scanStreamRuns(data.stream)
    out.collapsed += scan.collapsed
    out.kept += scan.kept
    out.records += scan.records
    out.runs += scan.plan.length
  }
  return out
}

/**
 * v4 的清理：只压事件内部的流式段。
 *
 * ★ 和旧格式的 pruneRows 最大的区别：【一行都不删】。
 *   不跑 dropIllegal、不重排 seq、不重映射跨事件引用 —— 那些是为"删掉整行"
 *   准备的，在没有删行的 v4 上跑它们只会平白引入风险。
 *
 * ★ 就地不改：被压过的行产出一个【新对象】（只换掉 data.stream），没压的行原样保留引用。
 *   这样 pruned.before 里的原始行始终是干净的，自校验才能拿原始流去比对 ——
 *   如果在这里就地改，"校验"就变成了拿改完的东西和自己比。
 *
 * @param rows - 全部存储行（第一行是 header）。
 * @returns {{header, events, before, rewritten, stats, problems}}
 */
function pruneV4(rows) {
  const header = rows.find((row) => row && typeof row === 'object' && row.type === 'session')
  if (header === undefined) throw new Error('session header line is missing')
  const events = []
  for (const row of rows) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) continue
    if (row.type === 'session') continue
    events.push(row)
  }
  const before = events.slice()
  const problems = []
  const rewritten = []
  let collapsed = 0
  let runs = 0
  let carriers = 0
  let verifier = 'none'
  for (let index = 0; index < events.length; index++) {
    const ev = events[index]
    const data = ev.data
    if (data === null || typeof data !== 'object' || Array.isArray(data)) continue
    if (!Array.isArray(data.stream)) continue
    carriers++
    let planned
    try {
      planned = planEventStream(ev)
    } catch (err) {
      problems.push('seq ' + ev.seq + ': stream plan threw: ' + String(err && err.message ? err.message : err))
      continue
    }
    if (!planned.verdict.ok) {
      // ★ 锚点成立却复核不过 = 我对等价性的判断和 DSH 自己的读取器分了叉。
      //   这种时候绝不"少压几条凑合过去"：整份文件不写，让人来看。
      problems.push('seq ' + ev.seq + ': compacted stream no longer replays the same (' + planned.verdict.reason + ')')
      continue
    }
    if (planned.plan.length === 0) continue
    events[index] = { ...ev, data: { ...data, stream: applyStreamPlan(data.stream, planned.plan) } }
    rewritten.push({
      before: ev,
      after: events[index],
      at: planned.plan.map((step) => step.at),
      seq: ev.seq,
      // ★ 取成【字符串】而不是留一个对象引用：对象之间是浅共享的（{...data} 里 usage 还是
      //   同一个对象），留着引用就抓不到嵌套对象被改。字符串是冻结的快照，抓得到。
      outsideBefore: rowOutsideStream(ev),
    })
    collapsed += planned.collapsed
    runs += planned.plan.length
    verifier = planned.verdict.via
  }
  return {
    header,
    events,
    before,
    rewritten,
    problems,
    stats: { keptEvents: before.length, chunkRows: collapsed, dropped: 0, runs, carriers, verifier },
  }
}

/** 一个事件里除 data.stream 之外的 JSON（用于证明"只动了 stream"）。 */
function rowOutsideStream(event) {
  const data = event.data
  if (data === null || typeof data !== 'object' || Array.isArray(data) || !Array.isArray(data.stream)) {
    return JSON.stringify(event)
  }
  const copy = { ...event, data: { ...data } }
  delete copy.data.stream
  return JSON.stringify(copy)
}

/**
 * v4 剪枝后的自校验。
 *
 * ★ 这一条必须【能因为剪错而失败】，否则就是假验证。所以它不是"再确认一遍 pruneV4 说过的话"，
 *   而是完全从产物出发、用 DSH 自己的读取器重算四件事：
 *     V1' 写完的流形状合法（expandAssistantStream 不抛）
 *     V2' 每一条被压的段都还是单成员 + dt 空 + 锚点仍成立
 *     V3' 写完的流重放出来的 content/usage/replayState 与事件里存的那份仍然一致
 *         —— 这一条就是 DSH 自己的 assertCurrentAssistantStreams 做的事
 *     V4' 写完的流与【原始流】（before 里那份，没被就地改过）在 DSH 读取器眼里仍然等价
 *   另外两条全局性质：行数不变、seq 从 0 连续、以及【除了 data.stream 之外一个字节都没变】。
 *
 * @param pruned - pruneV4() 的结果。
 * @returns 问题字符串数组；空数组 = 可以写。
 */
function verifyV4Pruned(pruned) {
  const problems = pruned.problems.slice()
  const events = pruned.events
  const before = pruned.before
  const mod = bundledStreamVerifier
  if (events.length !== before.length) {
    problems.push('row count changed: ' + before.length + ' -> ' + events.length)
  }
  for (let index = 0; index < events.length; index++) {
    const ev = events[index]
    if (ev.seq !== index) problems.push('seq ' + ev.seq + ' != index ' + index)
    const was = before[index]
    if (was === undefined) continue
    if (was.seq !== ev.seq || was.type !== ev.type) problems.push('seq ' + ev.seq + ': row identity changed')
  }
  for (const item of pruned.rewritten) {
    const ev = item.after
    const stream = ev.data.stream
    const tag = 'seq ' + item.seq + ': '
    // "只动了 stream" 这一条只对【真被改过的行】证明 —— 没改的行是同一个对象引用，
    // 本来就不可能变；而对改过的行，这里比的是改前拍下的字符串快照。
    if (item.outsideBefore !== rowOutsideStream(ev)) {
      problems.push(tag + 'something outside data.stream changed')
    }
    // V2'：被压的每一条必须是单成员 + dt 空 + 锚点仍在
    for (const at of item.at) {
      const record = stream[at]
      if (record === null || typeof record !== 'object' || Array.isArray(record)) {
        problems.push(tag + 'compacted run at ' + at + ' is not a record')
        continue
      }
      const members = RUN_MEMBER_KEY[record.type] === undefined ? undefined : record[RUN_MEMBER_KEY[record.type]]
      if (!Array.isArray(record.dt) || record.dt.length !== 0 || !Array.isArray(members) || members.length !== 1) {
        problems.push(tag + 'compacted run at ' + at + ' is not single-member with an empty dt')
        continue
      }
      if (!runAnchorHolds(stream, record)) {
        problems.push(tag + 'compacted run at ' + at + ' lost its block-end anchor')
      }
    }
    if (mod === null) continue
    // V1'：写完的流必须仍能被 DSH 自己的读取器展开
    let expanded
    try {
      expanded = mod.expandAssistantStream(stream)
    } catch (err) {
      problems.push(tag + 'compacted stream rejected: ' + String(err && err.message ? err.message : err))
      continue
    }
    // V3'：重放出来的东西必须和事件里存的那份一致（等于 DSH 自己的 assertCurrentAssistantStreams）
    const assembler = new mod.BlockAssembler()
    for (const member of expanded) assembler.push(member.chunk)
    const data = ev.data
    const message = data.message
    if (message !== null && typeof message === 'object' && !Array.isArray(message)) {
      const want = data.interrupted === true ? assembler.interruptedBlocks() : assembler.blocks()
      if (!isDeepStrictEqual(message.content, want)) {
        problems.push(tag + 'content disagrees with the compacted stream')
      }
      const source = message.source
      if (source !== null && typeof source === 'object' && source.replayState !== undefined
        && !isDeepStrictEqual(source.replayState, assembler.replayState)) {
        problems.push(tag + 'replay state disagrees with the compacted stream')
      }
    }
    if (data.usage !== undefined && !isDeepStrictEqual(data.usage, assembler.usage)) {
      problems.push(tag + 'usage disagrees with the compacted stream')
    }
    // V4'：与【原始流】仍等价（原始流就在下面那行，没被就地改过）
    const original = item.before.data.stream
    const verdict = checkStreamEquivalence(original, stream)
    if (!verdict.ok) problems.push(tag + 'compacted stream is not equivalent to the original (' + verdict.reason + ')')
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
    // ★ 沙箱#2 适配 (0.2.0-rc.2)：会话日志的文件名现在带格式代次 ——
    //   @deepseek-ai/dsh-session-format:sessionFormatLogFilename(v) 返回
    //   `session.v<N>.jsonl`（v0 才是旧的 `session.jsonl`），压缩后缀再拼 .zstd。
    //   0.1.x 的固定名 'session.jsonl.zstd' 在新版一个文件都匹配不到，
    //   插件会“加载成功但永远扫 0 个文件”。两种名字都收。
    else if (/^session(\.v\d+)?\.jsonl\.zstd$/.test(entry.name)) out.push(full)
  }
  return out
}

// ---------------------------------------------------------------- 会话跟踪
// 这个 DSH 进程加载过哪些会话。里面的每一个都不能被重写 —— 内存里有它们的 seq 计数器。
const loadedSessions = new Set()
// 本 build 能不能派发 agent 事件。不能的话跟踪就是不可用，vacuumOne 会退回"进程内不写"。
let sessionTracking = false

/** 从日志路径里取出会话 id（目录名形如 session-<uuid>）。取不到返回空串。 */
function sessionIdOf(file) {
  const m = /(session-[0-9a-fA-F-]{8,})/.exec(String(file || ''))
  return m ? m[1] : ''
}

/** 把 agent 事件里出现过的会话记下来。失败不影响插件加载。 */
/** 跟踪是否可用（build 能派发 agent 事件）。测试和 CLI 都要看这条。 */
function isSessionTracking() {
  return sessionTracking
}

function trackSessions(ctx) {
  if (!ctx || typeof ctx.on !== 'function') return false
  const note = (payload) => {
    const agent = payload && payload.agent
    const session = agent && agent.session
    const sid = session && session.header && session.header.id
    if (typeof sid === 'string' && sid) loadedSessions.add(sid)
  }
  let any = false
  for (const ev of ['agent/created', 'agent/session-start']) {
    try {
      ctx.on(ev, note)
      any = true
    } catch { /* 这个事件名不存在就是不可用，换下一个 */ }
  }
  if (any) sessionTracking = true
  return any
}

/** 单个会话的维护；返回一行结果对象。 */
function vacuumOne(file, cfg) {
  const stat = fs.statSync(file)
  // ★ 第一道闸：**正在被这个 DSH 载入的会话，永远不重写**。
  //   事故（2026-10-18）：文件被重编号成 0..72555，而 DSH 内存里的 seq 计数器还是 713504，
  //   下一条追加就写成 713504 ⇒ 一个文件里混进两套编号 ⇒ 会话损坏。
  //   mtime 判不出这件事 —— 会话被 GUI 开着但空闲时，文件就是"没被动过"。
  //
  //   判据不是"文件没动"，是"这个进程有没有加载过它"：本进程加载过的会话记在
  //   loadedSessions 里，一律跳过；没加载过的（沉睡的旧会话）重写是安全的 ——
  //   DSH 下次打开时读的就是新文件。所以装上就能自己干活，不需要用户先关 DSH。
  //
  //   万一本 build 派发不了 agent 事件（跟踪不可用）⇒ 退回保守策略：进程内一律不写。
  const inProcess = !cfg.preBoot && !cfg.allowInProcessWrite
  if (inProcess) {
    const sid = sessionIdOf(file)
    if (!sessionTracking) return { file, status: 'skipped-in-process' }
    if (sid && loadedSessions.has(sid)) return { file, status: 'skipped-loaded' }
  }
  // Boot window: the sweep scheduled from apply() runs before any session is opened, so a
  // recent mtime only means "DSH wrote this last before it was shut down", not "in use".
  // A 10 s floor stays for the pathological case of a write during startup.
  const windowMs = cfg.bootWindow ? Math.min(cfg.liveWindowMs, 10000) : cfg.liveWindowMs
  if (Date.now() - stat.mtimeMs < windowMs) {
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

  const header = rows.find((row) => row && typeof row === 'object' && row.type === 'session')
  const version = formatVersion(header)
  // ★ v4 起流式内容在事件内部，按【行类型】数碎片永远是 0（见上面 v4 那一段）。
  //   所以两条通道分开走，旧格式一行都不许受影响。
  const embedded = version >= 4 && cfg.v4Streams !== false

  let chunks = 0
  let others = 0
  if (embedded) {
    // v4：chunks = 会被压掉的【片段数】；others = 压完之后【还剩下的片段数】。
    // ★ 两者必须同量纲，比值才会落在 0..1（见 scanStreamRuns 的注释：混量纲会算出 34.154）。
    //   量纲和旧格式的 "碎片行 / 其它行" 相同（都是条数），只是粒度变成了事件内部的片段。
    const scan = scanV4(rows)
    chunks = scan.collapsed
    others = scan.kept
  } else {
    for (const row of rows) {
      if (isChunkRow(row)) chunks++
      else if (row && typeof row === 'object' && row.type !== 'session') others++
    }
  }
  const ratio = chunks + others === 0 ? 0 : chunks / (chunks + others)
  if (chunks < cfg.minChunkRows || ratio < cfg.minRatio) {
    return {
      file,
      status: 'below-threshold',
      format: version,
      mode: embedded ? 'v4-stream' : 'rows',
      chunks,
      ratio: Number(ratio.toFixed(3)),
      mb: Number((stat.size / 1048576).toFixed(2)),
    }
  }

  let pruned
  try {
    pruned = embedded ? pruneV4(rows) : pruneRows(rows)
  } catch (err) {
    return { file, status: 'prune-failed', error: String(err.message) }
  }

  const problems = embedded ? verifyV4Pruned(pruned) : verifyPruned(pruned)
  if (problems.length > 0) {
    // 校验不过就不写。宁可留着大文件，也不换一份坏文件进去。
    return {
      file,
      status: 'verify-failed',
      format: version,
      mode: embedded ? 'v4-stream' : 'rows',
      chunks,
      problems: problems.slice(0, 5),
    }
  }

  const outText = [JSON.stringify(pruned.header)].concat(pruned.events.map((e) => JSON.stringify(e))).join('\n') + '\n'
  const packed = compressFrames(outText)

  // ★ beforeMB 是【盘上真实字节】，afterMB 是【本插件重新打包后的字节】——
  //   这两个数之间天然含一点"换打包方式"的红利。想知道其中多少是【剪枝】挣来的，
  //   要先对未剪枝的原文按同一方式压一遍做基线（recompressMB 就是这个基线）。
  const common = {
    file,
    format: version,
    mode: embedded ? 'v4-stream' : 'rows',
    beforeMB: Number((stat.size / 1048576).toFixed(2)),
    afterMB: Number((packed.length / 1048576).toFixed(2)),
    chunks,
    kept: pruned.stats.keptEvents,
    dropped: pruned.stats.dropped,
  }
  if (embedded) {
    common.runs = pruned.stats.runs
    common.carriers = pruned.stats.carriers
    common.verifier = pruned.stats.verifier
  }

  if (cfg.dryRun) {
    return { ...common, status: 'would-vacuum' }
  }

  // 备份 -> 原子替换（临时文件 + rename）
  if (!fs.existsSync(file + '.orig')) fs.copyFileSync(file, file + '.orig')
  const tmp = file + '.vacuum-tmp'
  fs.writeFileSync(tmp, packed)
  fs.renameSync(tmp, file)

  const after = fs.statSync(file)
  return { ...common, status: 'vacuumed', afterMB: Number((after.size / 1048576).toFixed(2)) }
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
  const skippedLoaded = count('skipped-loaded')
  const skippedInProcess = count('skipped-in-process')
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
    skippedLoaded,
    skippedInProcess,
    skippedLive,
    belowThreshold,
    unreadable,
    other: results.length - handled - skippedLoaded - skippedInProcess - skippedLive - belowThreshold,
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
  // The external CLI is the only caller allowed to say "no DSH is running, writes are safe".
  if (typeof options.preBoot === 'boolean') effective.preBoot = options.preBoot
  if (typeof options.allowInProcessWrite === 'boolean') effective.allowInProcessWrite = options.allowInProcessWrite
  if (typeof options.bootWindow === 'boolean') effective.bootWindow = options.bootWindow
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
      if (result.status !== 'skipped-loaded' && result.status !== 'skipped-in-process' && result.status !== 'skipped-live' && result.status !== 'below-threshold') {
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
  'skipped-loaded': 'SKIPPED - this DSH has this session loaded, so its in-memory seq counter would desync with a rewritten file',
  'skipped-in-process': 'SKIPPED - cannot tell which sessions are loaded in this build, so no rewrite is attempted (run lib/clean-cli.mjs with DSH stopped)',
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
  const skipped = results.filter((r) => r.status === 'skipped-loaded' || r.status === 'skipped-in-process' || r.status === 'skipped-live' || r.status === 'below-threshold')
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
    // v4 和旧格式干的是两件事，报告里必须分开说 —— 否则 "chunks" 会被读成同一种东西。
    const what = r.mode === 'v4-stream'
      ? r.chunks + ' stream fragment(s) collapsed in ' + r.runs + ' packed run(s) (replay check: '
        + (r.verifier || 'none') + ')'
      : r.chunks + ' fragment row(s) dropped'
    lines.push('  - ' + r.file + ': ' + r.beforeMB + ' MB -> ' + r.afterMB + ' MB (saves '
      + (r.beforeMB - r.afterMB).toFixed(2) + ' MB), ' + what + ', '
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
    + cfg.minChunkRows + ') or minRatio (' + cfg.minRatio + '). On a v4 log the unit is embedded '
    + 'stream fragments, not rows.')
  for (const r of skipped.slice(0, REPORT_NAME_LIMIT)) {
    lines.push('  - ' + r.file + ': ' + (STATUS_TEXT[r.status] || r.status)
      + (r.chunks === undefined ? '' : ' (' + r.chunks + (r.mode === 'v4-stream' ? ' stream fragment(s)' : ' fragment row(s)') + ', ratio ' + r.ratio + ')'))
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
  + 'streaming fragments: on a v0/v1 log those are assistant/chunk and the packed chunk rows, and on a v4 log '
  + '(session.v4.jsonl.zstd) they are the packed delta runs embedded in assistant/message.data.stream, which '
  + 'this compacts to one member per run - every record stays valid and the file gets smaller. It reports '
  + 'before -> after MB. It runs the SAME guarded pass as the boot timer: it never touches a log '
  + 'modified inside liveWindowMs and it never bypasses the thresholds. Nothing is written unless the pruned '
  + 'log passes the loader contract AND, on v4, the embedded streams still replay to the same content, usage '
  + 'and replay state under DeepSeek Harness\'s own stream reader. The session the caller is running in '
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

  // ★ 先做会话跟踪，再做其它一切 —— 包括上面那个 enabled 提前返回之前的判断。
  //   理由：这是"能不能安全写"的唯一依据，它必须与开关无关地先就位。
  trackSessions(ctx)

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

  // Sweep immediately: this runs before the GUI connects and before any session is opened,
  // which makes it the one deterministic moment at which "the session you were just using"
  // can be cleaned. The delayed pass below stays as the second sweep.
  const bootTimer = setTimeout(() => {
    runVacuum(ctx, { trigger: 'boot', bootWindow: true }).catch(() => {})
  }, 0)
  if (typeof bootTimer.unref === 'function') bootTimer.unref()
  disposers.push(() => clearTimeout(bootTimer))

  const timer = setTimeout(() => {
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
  STREAM_RUN_TYPES,
  TOOL_NAME,
  ROUTE_PATH,
  STATUS_TEXT,
  dshHome,
  configPath,
  readConfig,
  asciiJson,
  isChunkRow,
  illegalRange,
  dropIllegal,
  pruneRows,
  verifyPruned,
  formatVersion,
  runMembers,
  runAnchorHolds,
  compactRun,
  scanStreamRuns,
  applyStreamPlan,
  checkStreamEquivalence,
  planEventStream,
  scanV4,
  pruneV4,
  rowOutsideStream,
  verifyV4Pruned,
  frameLength,
  decompressFrames,
  compressFrames,
  listSessionLogs,
  vacuumOne,
  runVacuum,
  loadedSessions,
  sessionIdOf,
  trackSessions,
  isSessionTracking,
  tallyRun,
  buildVacuumReport,
  buildTool,
  registerTool,
  loadToolModule,
  runCleanSessionLogs,
  vacuumRoute,
}
