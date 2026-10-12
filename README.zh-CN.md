# dsh-session-vacuum（中文说明）

[![dsh-plugin](https://img.shields.io/badge/dsh--plugin-community%20topic-blue)](https://github.com/topics/dsh-plugin)
[![deepseek-harness](https://img.shields.io/badge/deepseek--harness-plugin-4b6bfb)](https://github.com/topics/deepseek-harness)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> **Topics:** `dsh-plugin` · `deepseek-harness` · `session-log` · `streaming-fragments` ·
> `vacuum` · `prune` · `oom` · `long-session` · `memory-optimization`

> 英文主文档见 [README.md](README.md)。代码注释保持中文，本文件是完整的对照说明。

DSH 插件：开机后扫一遍会话日志，把**已经没人读的流式碎片行**删掉，
让一个长会话**以后每次加载都是小文件**。

- **装上什么都不做** ✓ 没有 `config.json` 时连定时器都不起，不读、不写、也不记日志
- **默认 dry-run** ✓ 就算开了，也只记「本来能省多少」，不动文件
- **写之前先自校验** ✓ 剪完的结果必须满足加载契约，不满足就一个字节都不写
- **★ 它是破坏性的** ✗ 碎片行是**真删掉**的；`.orig` 是唯一的备份，而且**永不自动清理** ✓

开之前请先读下面的 [破坏性是设计的一部分](#破坏性是设计的一部分) ✓

---

## 为什么做

会话日志是**很多个 zstd 帧拼起来的** JSONL。模型每吐一个字，都会写两遍：
一遍是原始增量（`assistant/chunk` 与打包行 `text-chunks` / `reasoning-chunks` /
`tool-call-chunks`），一遍是定稿的 `assistant/message`。
**没有任何东西读那些碎片** ✗ 回放不读、投影层里连 `chunks` 这个词都没有、
喂模型更不会带上它 —— 它们只写不删，于是越积越多 ✓

实测那个把插件逼出来的会话（**1813 轮**）：

| | |
|---|---|
| 日志行数 | 22.4 万行 |
| 磁盘占用 | 272 MB |
| 加载后展开的对象 | 843 万个 |
| 常驻内存 | 1.29 GB |
| 结果 | OOM |

文件小到平时注意不到，大到能把加载它的进程弄死 ✓
而且**每次加载都要付一遍这个成本** ⇒ 会话越用越卡，最后打不开 ✗
—— 而制造这一切的那些行，是**确定没人读**的 ✓

## 怎么做的

开机 `startDelayMs` 之后，**只跑一次**：

1. **扫** ✓ 遍历 `$DSH_HOME/sessions` 里的 `session.jsonl.zstd`，按体积从大到小排 ✓
   `liveWindowMs` 以内被动过的文件直接 `skipped-live` —— **绝不在会话可能正在写的时候换文件** ✓
2. **判阈值** ✓ 数一下碎片行数与它的占比 ✓ 低于 `minChunkRows` 或低于 `minRatio`
   就 `below-threshold`，**整个文件一个字节都不碰** ✓ 几百 KB 的碎片不值得重写一份日志 ✓
3. **剪** ✓ 删掉碎片行，然后**重排 `seq` 并把所有跨事件引用重映射到新编号** ✓
   四类引用都覆盖：`sourceEventSeqs`、`surfaceOp.start/end`、`shadowedSeqs`、
   `data.shadowedRange` ✓ 被重映射**弄成非法**的事件（端点丢了、surface 事件没有标记）
   也会被一并丢掉，并且**迭代到不动点** ✓ 所以结果是一致的，而不是「差不多一致」✗
4. **自校验** ✓ 剪完的结果必须满足 DSH 加载时的那套契约：`seq` 从 0 连续、
   每个 surface 事件都带标记、没有悬空引用、没有指向更晚事件的引用 ✓
   任何一条不满足 ⇒ `verify-failed` ⇒ **什么都不写**，文件保持原大小 ✓
   「不肯变小」永远可以接受，「写进去一份 DSH 读不了的日志」不可以 ✓
5. **原子替换** ✓ 先把原文件复制成 `<日志>.orig`（已存在就不覆盖），
   新内容写进 `<日志>.vacuum-tmp`，再 `rename` 覆盖 ✓ 中途崩溃留下的是完整原文件 ✓

它**不会**让一个已经加载进内存的会话变小 ✓ 也不需要：
把文件修小之后，**以后每一次加载都是小的** ✓ 内存、卡顿、OOM 都是从这里来的 ✓

## 和"运行时裁剪"那一路的区别

同一堆死重有第二种互补的解法：不在**静止时**处理，而在**读取时**处理。
[dsh-host-history-lite](https://github.com/GithungDang/dsh-host-history-lite) 拦的是
`session.history` / `subagent.history` 这两个 RPC 路由，在响应离开服务器之前，
把已完成步骤的 chunk 折回最终那条 `assistant/message`。两个工具打的是**同一批行**，
但位于不同层，**可以同时用**。

| | dsh-session-vacuum（本插件） | 运行时裁剪 |
|---|---|---|
| 在哪一层 | 磁盘上的会话日志 | RPC 响应 |
| 什么时候 | 每次开机一次，且只在会话没在写时 | 每次请求历史时 |
| 数据去向 | **删掉**，`.orig` 是唯一备份 | 留着，只是不发给客户端 |
| 省下什么 | 磁盘、加载时间、以及加载展开出来的内存 | 传输体积、客户端解析时间 |
| 可逆 | ✗ | ✓ 随时 |
| 谁受益 | 所有读日志的路径 + 以后每一次加载 | 只有 `dsh web` 客户端 |
| 别的工具读这份日志 | 看到的是变小后的日志 | 不受影响 |

按"哪里疼"选：`sessions\` 一直变大、加载时展开成几个 GB ⇒ 用本插件；
历史面板打开要等好几秒 ⇒ 用运行时那一路。

> **关于"这些碎片没人读"这句话**：在**服务器侧**成立 —— 碎片不属于发给模型的 surface，
> 服务端投影也不回读它们。但**客户端**在打开一段已完成对话时**确实会回放**这些碎片。
> 这也正是运行时裁剪那一派能安全丢掉它们的原因。

## 安装

这是 DSH profile 的插件，和其他插件挂同一层 ✓

1. 先让你们的 profile 能解析这个包，然后写进 profile 清单
   `~/.dsh/profiles/<profile>/package.json`：

   ```json
   {
     "dependencies": { "dsh-session-vacuum": "link:/path/to/dsh-session-vacuum" },
     "dsh": { "profile": { "bundles": ["@deepseek-ai/dsh-base", "dsh-session-vacuum"] } }
   }
   ```

   或者走显式的 patch 层，往 `~/.dsh/profiles/<profile>/cordis.patch.yml` 里加：

   ```yaml
   - insert:
       - id: session-vacuum
         name: 'file:///path/to/dsh-session-vacuum/lib/index.js'
   ```

2. **需要重启 DSH** ✓ 插件行只在开机时加载 ✓

3. 这时它**还是什么都不做**（默认 inert）✓ 要开就写
   `~/.dsh/dsh-session-vacuum/config.json`：

   ```json
   { "enabled": true, "dryRun": true }
   ```

   `enabled` 只在开机读一次 ⇒ 改它要重启 ✓
   `dryRun: true` 下它只会往 `~/.dsh/logs/session-vacuum.jsonl` 里记
   「本来会删多少」✓ 看数字对了，再把 `"dryRun"` 改成 `false` ✓

想彻底卸掉：删掉上面那两行，重启 ✓
它留下的东西只有三样：配置文件、日志文件、以及若干 `.orig` 备份 ✓

> **需要 Node 22.15 或更新**：代码用了 `zlib.zstdDecompressSync` ✓
> 更老的运行时它**什么都干不了**（解压调用失败 ⇒ 数不到碎片行 ⇒ 全部报 `below-threshold`）✓
> 也就是「做不了」而不是「做错」✓

## 配置

文件：`$DSH_HOME/dsh-session-vacuum/config.json`（默认 `~/.dsh/…`）✓
**文件不存在、或者不是合法 JSON** ⇒ 用默认值 ✓ **缺哪个键，那个键就用默认值** ✓

| 键 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 总开关 ✓ `false` = 加载了但完全空转，**连定时器都不起** ✓ 只在开机读一次，改动要重启 |
| `dryRun` | `true` | `true` = 解压、剪、自校验全做一遍，然后**只记 `would-vacuum` 不写文件** ✓ |
| `startDelayMs` | `20000` | 开机后延迟多久才开始扫（毫秒），避开启动高峰 |
| `liveWindowMs` | `600000` | 多久以内被动过的日志算「正在写」⇒ `skipped-live`（默认 10 分钟） |
| `minChunkRows` | `2000` | 碎片行数少于它就不动这个文件 |
| `minRatio` | `0.25` | 碎片行占全部事件行的比例低于它就不动 |
| `maxFilesPerRun` | `12` | 每次开机最多处理几个文件（从大到小）✓ `skipped-live` 和 `below-threshold` 的不占额度 |
| `logFile` | `logs/session-vacuum.jsonl` | 相对 `$DSH_HOME` |
| `minIntervalMs` | `21600000` | **0.1.0 里没生效** ✗ 声明成 6 小时，但代码里没有任何地方读它 —— 维护是「每次开机跑一次 `setTimeout`」，根本不存在第二次运行给它限流 ✓ 见 `CHANGELOG.md` |

代码里还读了一个**不在 DEFAULTS 里**的键：`skipIfBackedUp` ✓
它出现在 `.orig` 那个「空壳」判断里，**没有任何效果** ✗ 见下面的已知边界 ✓

## 日志

`$DSH_HOME/logs/session-vacuum.jsonl`，每行一个 JSON，都带 `t` 时间戳和 `mark`：

| `mark` | 说明 |
|---|---|
| `run-start` | 一次扫描开始 ✓ 带 `files`（找到几个日志）和 `dryRun` |
| `file` | 处理了一个文件 ✓ 带 `status` |
| `run-end` | 扫描结束 ✓ 带 `handled`（这次真的动了几个文件） |
| `run-error` | 扫描本身出错（单个文件的错误记成 `file` 行） |

`file` 行的 `status`：

| `status` | 说明 |
|---|---|
| `would-vacuum` | dry-run ✓ `beforeMB` / `afterMB` / `chunks` / `kept` / `dropped` 就是真跑会得到的结果 |
| `vacuumed` | 真做了 ✓ 日志已被替换，`<日志>.orig` 是替换前的字节 |
| `below-threshold` | 没过 `minChunkRows` 或 `minRatio` ✓ `chunks` / `ratio` 说明差多少 |
| `skipped-live` | `liveWindowMs` 内被动过 ⇒ 可能正在写 |
| `verify-failed` | 剪完的结果没过加载契约 ✓ `problems` 列前五条 ✓ **什么都没写** |
| `unreadable` | 不是 zstd 帧流 |
| `unparsable` | 解出来的一行不是 JSON ✓ **什么都没写** |
| `prune-failed` | 缺 `session` 头行 |
| `error` | 这个文件的其他异常 |

**推荐用法是先把它当诊断工具** ✓ 挂上之后 `dryRun: true` 跑一阵，
看到 `would-vacuum` 行里的 `beforeMB` / `afterMB` 数字合理，再决定要不要开真跑 ✓

## 安全设计

| 机制 | 作用 |
|---|---|
| **默认 inert** | 没有 `config.json` ⇒ `enabled:false` ⇒ `apply()` 在起定时器之前就返回 ✓ 不读不写不记 |
| **默认 dry-run** | 整个日志解压、剪、自校验都真做一遍，然后**把结果扔掉** ✓ |
| **绝不动活文件** | `liveWindowMs` 以内被动过的直接跳过 ✓ 整个设计的前提就是「只对 DSH 没在写的日志动手」✓ |
| **绝不动小文件** | 两道下限都过了才允许重写一个字节 ✓ |
| **写之前自校验** | 加载契约是在**剪完的结果上重新验一遍**，不是假定的 ✓ 拒绝的代价是磁盘，写坏的代价是会话 ✓ |
| **原子替换** | 写临时文件 → `rename` ⇒ 日志要么是旧字节，要么是新字节 ✓ |
| **一份字节级备份** | `.orig` 在 rename 之前写好，**插件永不删除、永不覆盖** ✓ |
| **串行 + 让出事件循环** | 一次一个文件，文件之间 `setImmediate` ✓ 不拖慢启动和对话 ✓ |
| **一律放行** | 所有失败都变成日志里的 `status` ✓ 绝不往 DSH 启动路径里抛异常 ✓ |

## 破坏性是设计的一部分

它不是压缩器，也不做归档 ✓ **它就是从会话日志里删行** ✗ 所以下面这些必须说清楚：

- **碎片是真没了** ✗ 没搬走、没摘要、没写到别处 ✓ 真跑之后，
  替换前那份日志**唯一的副本就是 `<日志>.orig`** ✓
- **`.orig` 是唯一备份，而且永不自动清理** ✓ 这是刻意的：
  自动清理会把「还能救」悄悄变成「救不回来」 ✗
- **`.orig` 里同样不会被剪** ✓ 只有**你自己**删掉 `.orig` 才能把空间收回来；
  而删掉的那一刻，那些碎片就再也找不回来了 ✓
- **DSH 内部没有撤销路径** ✗ 恢复要停机，把 `session.jsonl.zstd` 换回它的 `.orig`，再启动 ✓
  另外注意：**已经加载在内存里的会话不受影响** ✓ 插件改的是「下一次加载看到的东西」✓
- **代价有界，但确实存在** ✓ 被删的行按当前 DSH 版本看是只写的 ✓
  但如果将来某个版本开始读原始流式增量（最可能是「回放打字动画」），
  被剪掉的信息就不在了 ✗ 这就是这笔交易：272 MB 与 1.29 GB 的对象，
  换掉一批**当前确定没人读**的碎片 ✓

所以推荐的顺序是：先用默认值装上 ⇒ `dryRun: true` ⇒
读 `logs/session-vacuum.jsonl` ⇒ 挑一组你自己能接受的阈值 ⇒
**确认备份是你丢得起的**，再改 `"dryRun": false` ✓

## 实测

| | |
|---|---|
| 引出这个插件的那个会话 | 1813 轮 / 22.4 万行 / 272 MB |
| 加载后展开的对象 | 843 万个 |
| 常驻内存 | 1.29 GB（随后 OOM） |
| 剪完之后文件里剩什么 | `session` 头行 + **每一条不是流式碎片的事件行** |

前三行是**那个事故本身**的数字，在写这个插件的机器上量的 ——
它们是这个插件存在的理由，**不是**对插件本身的基准测试 ✓
插件对**任意**日志做了什么，由它自己记的数字说清：删掉 `chunks` 行、
留下 `kept` 个事件、`beforeMB` → `afterMB` ✓
开 `dryRun` 跑一遍，你拿到的就是**你自己那些会话**的数字 —— 比别人的更有用 ✓

重写的正确性已经离线端到端验过：一份同形状的合成日志被剪、被替换，
然后**逐帧解压**重读、逐行 `JSON.parse`，并把存活的行走一遍比对
（`test/vacuum-tests.mjs`，18 项）✓

## 测试

```bash
npm test          # node test/vacuum-tests.mjs
npm run test:vacuum
```

全离线 ✓ 不碰 DSH ✓ 不联网 ✓ 不需要 API key ✓
`process.env.DSH_HOME` 在**导入插件之前**就被指到 `os.tmpdir()` 下的临时目录 ✓
测试里的每一份日志都是本地合成的 ✓
跑完打印 `18 passed, 0 failed`，有任何失败**退出码非零** ✓

| 范围 | 项数 | 覆盖 |
|---|---|---|
| 碎片识别 | 2 | 四种碎片行都认得；`chunk-stub` 以及其他长得像的**都不算** ✓ |
| 阈值 | 2 | 碎片行太少；碎片占比低于 `minRatio`（并带一个对照：同一个文件把下限降到够得着之后就会真处理）✓ |
| 活文件保护 | 2 | 一秒前写过的文件不动；同一个文件把窗口调小就会处理 —— 说明拦它的是 **mtime 窗口**，不是文件名 ✓ |
| 备份 | 1 | 已存在的 `.orig` 永不被覆盖（见下面那条说明）✓ |
| 剪 | 2 | 头行保留、所有非碎片事件保留、`seq` 从 0 连续；`sourceEventSeqs`、替换型 `surfaceOp`、`data.shadowedRange` 都被重映射到新编号 ✓ |
| 校验闸门 | 1 | 违反契约的日志报 `verify-failed`，之后文件**逐字节不变**，且不产生 `.orig` ✓ |
| dry-run | 1 | 报 `would-vacuum`、报出节省量、文件逐字节不变 ✓ |
| 真跑 | 3 | 日志被替换且更小、`.orig` 逐字节一致、不留临时文件；结果**逐帧重读**并逐行解析，事件数量与顺序都与原样一致 ✓ |
| 坏输入 | 2 | 不是 zstd 的文件、以及 JSON 撕裂的日志：只报告，绝不重写 ✓ |
| 发现与启动 | 2 | 递归只找 `session.jsonl.zstd`；`runVacuum` 会扫、会遵守 `maxFilesPerRun`、会逐文件记日志，而且 dry-run 下不动文件 ✓ |

> **给贡献者**：`test()` **必须 `await fn()`** ✗
> 同步的框架会在任何 async 断言执行之前就打 `ok`，并且让脚本先走到退出那一行
> ⇒ **整套测试静默变成空跑** ✓ 这一系列插件真的踩过这个坑 ✓
> 见 [dsh-history-fold](https://github.com/mili1360/dsh-history-fold) 的 `CHANGELOG.md` ✓
> 本项目的框架用同样的方法验过：临时加两个**必失败**的 async 用例
> ⇒ 两个都报 `FAIL`、退出码 `1` ⇒ 随后删掉 ✓

## 已知边界

- **帧边界按帧结构算，不靠扫 magic** ✓
  早先的版本是「切到下一个 `28 b5 2f fd`」✗ 这四个字节**可能出现在某一帧的载荷内部**，
  而 Node 的 `zstdDecompressSync` 遇到半截帧**不抛错、返回空** ⇒ 断点落在干净的换行处，
  截断后的行集**照样能解析、也照样能通过自校验** ✗
  已复现：661 行的合成日志只读回 200 行、被写成 38 行，`status` 还是 `vacuumed` ✗
  现在改成解析帧头 + 每个 block 的 3 字节头 ✓ 走不过去的尾巴一律报 `truncated` ✓
  `vacuumOne()` 据此返回 `unreadable-tail` 且**一个字都不写** ✓
  两条回归测试盯着它 ✓ 把旧规则塞回去，两条立刻变红 ✓
- **`.orig` 存在并不会让会话被跳过 —— 这是有意的** ✓
  因为 `.orig` 永不被删，「存在备份就跳过」会让一个会话**只被维护一次** ✓
  现在的行为是每次运行都按阈值重新评估 ✓
  真正存在的保证是：**第一次那份备份不会被覆盖** ✓
- **`minIntervalMs` 没有任何作用** ✗ 维护是每次开机跑一次，不存在第二次运行 ✓
- **一次只处理一个文件、每次开机最多 `maxFilesPerRun` 个** ✓
  一堆超大日志要清，得分几次重启才能清完 ✓
- **体积字段只保留两位小数** ✗ 小日志的 `beforeMB` / `afterMB` 都是 `0.0`，没有意义 ✓
  它们是给几 MB、几十 MB 的日志用的 ✓
- **它不能让正在运行的进程把内存吐出来** ✗
  已经开着的会话要关掉重开才会变小 ✓
- **阈值是在 Windows 上的 DeepSeek Harness 调的** ✓
  帧格式、行类型、加载契约都是 DSH 的 ✓
  将来 DSH 改了其中任何一样，结果是 `below-threshold` / `verify-failed` 这类**拒绝**，
  而不是写坏日志 ✓ 这是预期的失败方式，但它仍然是失败 ✗

## 许可

MIT，见 [LICENSE](LICENSE)。
