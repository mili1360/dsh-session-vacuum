# dsh-session-vacuum（中文说明）

[![dsh-plugin](https://img.shields.io/badge/dsh--plugin-community%20topic-blue)](https://github.com/topics/dsh-plugin)
[![deepseek-harness](https://img.shields.io/badge/deepseek--harness-plugin-4b6bfb)](https://github.com/topics/deepseek-harness)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> **Topics:** `dsh-plugin` · `deepseek-harness` · `session-log` · `streaming-fragments` ·
> `vacuum` · `prune` · `oom` · `long-session` · `memory-optimization`

> 英文主文档见 [README.md](README.md)。代码注释保持中文，本文件是完整的对照说明。

会话越长，DSH 越慢越重 ✗ —— 每次加载都把日志展开成几百万个对象 ✓ 最后直接把内存撑爆 ✓

**只把 chunk 从客户端藏起来是不够的** ✗ —— 磁盘上那个文件还是几百 MB ✓ 下次加载照样重建那几百万个对象 ✓ 照样 OOM ✓ 这个插件在**磁盘上**把它们删掉 ✓ 让"加载"这件事本身变小 ✓
促成它的那个会话是 **1,813 轮** ✓ 22.4 万行 ✓ 磁盘 272 MB ✓ 加载展开 **843 万个对象** ✓ 常驻 1.29 GB ✓ 结果 OOM ✓

**而且你从第一轮起就在攒它** ✗ —— 真会话实测：**可清的片段数 ≈ output token 的 0.95~0.99 倍** ✓
8 个样本 ✓ 从「1 轮 91 token ⇒ 88 个片段」到「6 轮 1,516 token ⇒ 1,498 个片段」✓
**没人读它们** ✓ 它们是只写不读的 ✓

- **装上就干活** ✓ 全新安装无需任何配置就会清理闲置的日志 ✓ 想关就写 `enabled: false` ✓ 想只看不动就写 `dryRun: true` ✓
- **2 分钟内被动过的会话永远不碰** ✓（`liveWindowMs`）
- **写之前先自校验** ✓ 剪完的结果必须满足加载契约，不满足就一个字节都不写 ✓
- **★ 它是破坏性的** ✗ 碎片行是**真删掉**的；`.orig` 是唯一的备份，而且**永不自动清理** ✓

开之前请先读下面的 [破坏性是设计的一部分](#破坏性是设计的一部分) ✓

**为什么不能只把 chunk 藏起来？** ✓ 在 `session.history` 响应里把它们折掉，对客户端是**真有用**的 ✓ 本插件不取代那种做法 ✓ 两者可以同时跑 ✓ —— **但 OOM 不在客户端发生** ✗ 它发生在 DSH **加载日志**那一刻：文件被读进来 ✓ 每一行都被解析 ✓ 碎片行展开成几百万个对象 ✓ 如果磁盘上那个文件还是 272 MB ✓ 那"响应里跳过它们"就挡不住下一次加载重建 ✓ 唯一能挡住的办法是**把文件变小** ✓ —— 细节见 [和"运行时裁剪"那一路的区别](#和运行时裁剪那一路的区别) ✓

---
> 符号说明：**✓** = 实测确认过 · **✗** = 陷阱，别这么做 · **⚠️** = 注意 · **⇒** = 所以


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

清理之后，**同一个会话**：

| | 清理前 | 清理后 |
|---|---|---|
| 会话文件 | 86.67 MB | 16.99 MB |
| 碎片行 | 169,795 | 0 |
| 非碎片内容 | —— | **一条未丢**（`dropped=0`）|
| 界面报告的生成速度 | 明显变慢 | **恢复正常** |

> ⚠️ **「生成速度」这一行是实测观察，不是承诺，也没有定因** ✗
> 观察到的是：清掉碎片之后，加载内存下降，界面报告的生成速度随之恢复 ✓
> 但**没有做过前后同条件的 tok/s 对照读数** ✗ —— 所以这里只记录现象 ✓
> 不要去推断因果：同一台机器上，上下文大小、缓存命中、时段、思考档
> 都被逐一怀疑过又被数据否掉过 ✗ 真正被证实的只有这一条现象 ✓

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

## 按需清理

开机那一轮维护有个很实际的毛病：它跳过 `liveWindowMs`（默认 10 分钟）以内写过的文件 ✗
而那恰恰是你真正在意的那个会话 ✓
而且在这个接口出现之前，那一轮只报「处理了几个文件」✗
于是「扫了 58 个、跳过 56 个」和「这插件根本没用」在日志上长得一模一样 ✗

### 日志会说清每个文件为什么被跳过

`run-end` 现在在原来的 `handled` 旁边，按原因各记一个计数 ✓

```json
{"mark":"run-end","scanned":58,"handled":2,"vacuumed":2,"wouldVacuum":0,
 "skippedLive":56,"belowThreshold":0,"unreadable":0,"other":0}
```

`handled` 的含义**一个字都没改** —— 仍然是「这一轮真的动了几个文件」✓
其余的就是**没动**的那些，按 `file` 行用的**同一批** `status` 字符串分桶 ✓
**清理行为没有任何变化**，改的只是可观测性 ✓

### `clean_session_logs` —— 让 agent 当场问一次

Host 工具 **`clean_session_logs`**：没有必填参数，只有一个可选参数 ✓

| 参数 | 默认 | 说明 |
|---|---|---|
| `dryRun` | `false` | `true` = 只预览：解压、剪、自校验全做一遍，报告本来能省多少，**一个字节都不写** ✓ |

它走的是**和开机定时器完全相同**的那条受保护通道 ——
同一个 `liveWindowMs`、同一对阈值、同一个 `vacuumOne()`（建 `.orig`、自校验、原子替换）✓
按需接口**不是更大的锤子**，它只是不必等到下次开机 ✓

它返回**纯文本**，不是 JSON —— 你在会话里看到什么，就是它干了什么 ✓

```
session-vacuum: DRY RUN - nothing was written.
Scanned 4 session log(s) under /home/you/.dsh/sessions.
Would clean: 2 log(s).
  - /home/you/.dsh/sessions/third/session.jsonl.zstd: 0.01 MB -> 0 MB (saves 0.01 MB), 120 fragment row(s) dropped, 92 event(s) kept
  - /home/you/.dsh/sessions/stale/session.jsonl.zstd: 0.01 MB -> 0 MB (saves 0.01 MB), 120 fragment row(s) dropped, 38 event(s) kept
Total that a real pass would save: 0.02 MB.
Skipped: 2 log(s), by reason:
  - skippedLive: 1 - modified within the last 10 minute(s), so DSH may still be writing it.
  - belowThreshold: 1 - under minChunkRows (20) or minRatio (0.2).
  - /home/you/.dsh/sessions/live/session.jsonl.zstd: SKIPPED - modified inside liveWindowMs, so DSH may be writing it right now
  - /home/you/.dsh/sessions/below/session.jsonl.zstd: SKIPPED - not enough streaming fragments to be worth a rewrite (8 fragment row(s), ratio 0.571)
run-end counters (logs/session-vacuum.jsonl): scanned=4, handled=2, vacuumed=0, wouldVacuum=2, skippedLive=1, belowThreshold=1, unreadable=0, other=0
NOTE: the session you are running in cannot be cleaned from inside a running DSH.
  DSH holds that session in memory, and rewriting its log underneath it can corrupt it.
  The liveWindowMs guard (10 min) already refuses it, so this pass cannot touch it - that is the only protection, and it was not weakened.
  What to do instead: stop DSH and run E:\Dsh百宝箱\清理会话日志.cmd,
  or leave it idle for at least liveWindowMs (10 min) and let a later boot clean it.
```

**你正在用的那个会话，没法从运行中的 DSH 内部清理** ✓ 报告里会明说这一点 ✓
DSH 把活动会话握在内存里、还在往里追加 ✗ 从底下换掉那个文件可能把它写坏 ✓
`liveWindowMs` 本来就拦得住它，**这个接口没有把这道防线放松一丝** ✓
真要清那一个：**先停掉 DSH**，跑 `E:\Dsh百宝箱\清理会话日志.cmd` ✓
或者让它闲置满 `liveWindowMs`，等下次开机的那一轮顺手清掉 ✓

### web 路由（只读预览）

脚本还可以走 HTTP 问：

```
POST /api/local/session-vacuum/run
```

**只认 POST**，而且**只做预览**：body 里要求真跑的，一律 403 ✗
`dryRun: false` 的 profile 更是直接拒绝 —— 不对外暴露任何破坏性入口 ✓
真跑留给工具，那边有 agent 看着这次调用 ✓
空 body（或 `{}` / `{"dryRun":true}`）= dry run，响应体就是上面那份文本报告 ✓

两个入口在 `enabled: false` 时都是空转的，而且都绕不过那道防线 ✓

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

## 什么时候生效

它只会动**当前没在被使用的**会话日志，所以要让某个会话被清掉，得按顺序来，不是一个开关：

1. **从这个会话切走** —— 别直接关 DSH
2. **等它闲置超过 2 分钟**（`liveWindowMs`）
3. **重启 DSH**
4. **启动后约 20 秒**（`startDelayMs`），插件扫到它 ⇒ 清掉碎片
5. **下次再打开这个会话** ⇒ 加载更小、更快、不再 OOM

正在用的会话永远不会被碰，没超过阈值的日志也不会被碰。

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

3. **不写任何配置它也会干活** ✓（`enabled` / `dryRun` 默认为「开」✗）—— 想先观察就写
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

## 要求 / 兼容性

**装上之后要确认它真的在工作** ✓ 光"profile 起得来"不算数 ✗
插件可能加载得好好的、路由也活着，却把宿主弄坏 —— 我们真踩过 ✓
验收动作: **新建一个会话、发一条消息** ✓
然后看 `$DSH_HOME/logs/session-vacuum.jsonl` 里有没有一对 `run-start` / `run-end`
（开机后约 `startDelayMs` 出现）✓
如果连新会话都建不出来 ⇒ 把上面那行挂载删掉、重启 ✓

针对 **DSH `0.1.0-rc.7`** 开发并测试 —— 这是一个**魔改版**，不是官方原版。别的版本可能把服务
名改掉：本插件声明 `inject: ['tools']`，用到 `ctx.effect`、`ctx.tools.register`（按需工具），
以及可选的 `ctx.webServer.register`（预览路由）。

### 新版 DSH：v4 会话格式 —— 已支持，但有两处要如实说清

DSH `0.2.0-rc.2` 把会话日志写成 **`session.v4.jsonl.zstd`**（格式 **v4**），而 **v4 里没有碎片行** ✗
—— 那些打包的增量段藏在 `assistant/message` 的 `data.stream` 里 ✓
所以在 v4 上，旧的行级剪枝**一个都匹配不到** ✗ ⇒ 插件会「加载得好好的、却什么都不干」✓✓✓
这条路径现在实现了 ✓（压的是**事件内部**的段 ✓ 而且压完必须过 DSH 自己的读取器复核才允许写盘 ✓）——
并已在沙箱里用**真会话**端到端验证：一个 5 轮会话 **27,715 → 17,071 B** ✓
清完之后日志照样能加载 ✓ 而且**每一轮消息逐字节相同** ✓

**没有验证、你依赖它之前应该权衡的**：

- **`assistant/attempt` 没有真数据覆盖** ✗ —— 代码路径被用例覆盖 ✓ 自校验也会拒写任何不等价的结果 ✓
  但这个 build **只在失败 / 重试 / 取消 / 流错误**的 attempt 上才写它 ✓
  而所有测试会话都成功了 ✓ 所以**没有真样本** ✓ 只能算「单元级保证」✗ 不是实测 ✓
- **v4 路径没有自动化回归** ✗ —— 自带测试套件只覆盖 v0/v1 的行格式 ✓
  v4 目前靠上面说的沙箱端到端跑撑着 ✓

两处都写出来 ✓ 是为了不让「沉默失效」藏在一个绿色的测试结果后面 ✓
—— 而沉默失效正是这个插件存在的理由 ✓

> **`inject` 不是可选项** ✗ Cordis 的服务是**按需注入**的：没有声明在 `inject` 里的服务，
> 在 `ctx` 上**根本不存在这个属性**，读一下就会抛
> `cannot get property "tools" without inject` ✗
> 这不是理论 —— 这个接口的第一版**就是被这一行在真机启动时直接停用的** ✓
> `webServer` **故意不声明**：headless profile 里没有它 ✓
> 路由改用可选的 `ctx.inject(['webServer'], ...)` 形式，
> 所以没有 web 服务的 profile 照样拿得到工具 ✓
> 连 `ctx.inject` 都没有时，记一行 `mark: "route-failed"` 然后继续 ✓

版本对不上时，插件**不会**让 profile 起不来 ✓ 它会直接**停用**：不定时器、不碰任何会话日志，
只在 `logs/session-vacuum.jsonl` 里留一行：

```json
{"mark":"deactivated","reason":"registration-failed","step":"services","error":"..."}
```

`step` 会指出是哪一步失败：`services`（没有 `ctx.effect`）、`ctx.effect`（它抛异常）、
或 `ctx.tools`（没有可用的工具注册表）✓
**路由失败不在这个表里**：它记成 `mark: "route-failed"`，
插件带着工具继续活着 —— 因为路由只是方便，工具才是重点 ✓
`@deepseek-ai/dsh-tools` 在插件加载时解析一次用来取 `defineTool`；
解析不到的话，工具会退回**裸对象形态**（`ctx.tools.register({...})`）注册，而不是消失 ✓

**万一装上之后 profile 根本起不来**：把 `<profile>/cordis.patch.yml` 里挂载它的那几行删掉
（若你是用 `dsh.profile.bundles` 装的，就把数组里的 `"dsh-session-vacuum"` 去掉），然后重启：

```yaml
- insert:
    - id: session-vacuum
      name: 'file:///path/to/dsh-session-vacuum/lib/index.js'
```

## 配置

文件：`$DSH_HOME/dsh-session-vacuum/config.json`（默认 `~/.dsh/…`）✓
**文件不存在、或者不是合法 JSON** ⇒ 用默认值 ✓ **缺哪个键，那个键就用默认值** ✓

| 键 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 总开关 ✓ `false` = 加载了但完全空转，**连定时器都不起** ✓ 只在开机读一次，改动要重启 |
| `dryRun` | `true` | `true` = 解压、剪、自校验全做一遍，然后**只记 `would-vacuum` 不写文件** ✓ |
| `startDelayMs` | `20000` | 开机后延迟多久才开始扫（毫秒），避开启动高峰 |
| `liveWindowMs` | `120000` | 多久以内被动过的日志算「正在写」⇒ `skipped-live`（默认 2 分钟） |
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
| `run-start` | 一次扫描开始 ✓ 带 `files`（找到几个日志）、`dryRun`，以及 `trigger`（谁让它跑的：`boot` / `tool` / `route`） |
| `file` | 处理了一个文件 ✓ 带 `status` |
| `run-end` | 扫描结束 ✓ 带 `handled`（这次真的动了几个文件）、`scanned`，以及**按原因分桶的计数**：`vacuumed` / `wouldVacuum` / `skippedLive` / `belowThreshold` / `unreadable` / `other` |
| `run-error` | 扫描本身出错（单个文件的错误记成 `file` 行） |
| `route-failed` | web 路由没注册上 ✓ 记一行但不致命：工具照常可用 |
| `deactivated` | 注册失败、插件已停用（见「要求 / 兼容性」） |

`file` 行的 `status`：

| `status` | 说明 |
|---|---|
| `would-vacuum` | dry-run ✓ `beforeMB` / `afterMB` / `chunks` / `kept` / `dropped` 就是真跑会得到的结果 |
| `vacuumed` | 真做了 ✓ 日志已被替换，`<日志>.orig` 是替换前的字节 |
| `below-threshold` | 没过 `minChunkRows` 或 `minRatio` ✓ `chunks` / `ratio` 说明差多少 |
| `skipped-live` | `liveWindowMs` 内被动过 ⇒ 可能正在写 |
| `verify-failed` | 剪完的结果没过加载契约 ✓ `problems` 列前五条 ✓ **什么都没写** |
| `unreadable` | 不是 zstd 帧流 |
| `unreadable-tail` | 帧走查没能对上尾部 ⇒ 读残了 ✓ **什么都没写** |
| `unparsable` | 解出来的一行不是 JSON ✓ **什么都没写** |
| `prune-failed` | 缺 `session` 头行 |
| `error` | 这个文件的其他异常 |

**推荐用法是先把它当诊断工具** ✓ 挂上之后 `dryRun: true` 跑一阵，
看到 `would-vacuum` 行里的 `beforeMB` / `afterMB` 数字合理，再决定要不要开真跑 ✓
`clean_session_logs` 带 `dryRun: true` 可以**随时**给你同一份画面，不用等下次开机 ✓

## 安全设计

| 机制 | 作用 |
|---|---|
| **默认就是开着的，这是有意的** | `enabled` 和 `dryRun` 默认都取「干活」的值 ✓ 因为「装上却什么都不做」才是更常见的失败 ✗ 用户会以为插件坏了然后卸掉它 ✓ 想先观察就写 `dryRun: true` ✓ 想彻底关掉就写 `enabled: false` ⇒ `apply()` 在起定时器之前就返回 ✓ 不读不写不记 ✓ `clean_session_logs` 只报告「插件是关的」✓ 路由回 403 ✓ |
| **按需接口不是更大的锤子** | 它调的是同一个 `runVacuum()` / 同一个 `vacuumOne()`，同一道 `liveWindowMs`、同一对阈值 ✓ 它清不了调用方自己那个会话，不能超 `maxFilesPerRun`，HTTP 路由**根本不能写** ✓ |
| **`dryRun: true` 是什么样** | 整个日志解压、剪、自校验都真做一遍，然后**把结果扔掉** ✓ 只记日志 ✓ |
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
（`test/vacuum-tests.mjs`）✓

## 测试

```bash
npm test          # node test/vacuum-tests.mjs
npm run test:vacuum
```

全离线 ✓ 不碰 DSH ✓ 不联网 ✓ 不需要 API key ✓
`process.env.DSH_HOME` 在**导入插件之前**就被指到 `os.tmpdir()` 下的临时目录 ✓
测试里的每一份日志都是本地合成的 ✓
跑完打印 `31 passed, 0 failed`，有任何失败**退出码非零** ✓

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
| 跳过原因计数 | 1 | 一个目录里同时放：一个够格的、一个活着的、一个不过阈值的、一个被上限剩下的 ⇒ `run-end` 报 `scanned: 4`、`skippedLive: 1`、`belowThreshold: 1`、`unreadable: 0`、`other: 0`，而 `handled` 仍然是「真的动了几个」✓ |
| 按需工具 | 5 | 文本报告逐个点名**清了的**和**跳过的**，并给出**原因**与 before→after MB；「调用方自己的会话清不了」那条注意事项 + 该怎么办都在报告里；`dryRun: true` 一个字节、一个 mtime 都不改，也不备份；真跑清掉的正好是它报告里说清掉的那些；刚写过的日志照样被活文件保护拦住 ✓ |
| web 路由 | 1 | GET ⇒ 405；body 要求真跑 ⇒ 403；`dryRun: false` 的 profile 直接拒绝；空 body 的 POST 是预览；停用的插件什么都不扫 ✓ |
| 注册 | 4 | 上下文不兼容时（`effect` 抛异常 / 没有 `effect` / `ctx` 是 `null` / 没有 `tools`）：`apply()` 正常返回、每次失败正好记一行 `deactivated`、不定任何定时器；工具是**同步**注册的；路由注册失败只损失路由本身；撤销时两个注册点都被撤掉 ✓ |

> **给贡献者**：`test()` **必须 `await fn()`** ✗
> 同步的框架会在任何 async 断言执行之前就打 `ok`，并且让脚本先走到退出那一行
> ⇒ **整套测试静默变成空跑** ✓ 这一系列插件真的踩过这个坑 ✓
> 见 [dsh-history-fold](https://github.com/mili1360/dsh-history-fold) 的 `CHANGELOG.md` ✓
> 本项目的框架用同样的方法验过：临时加两个**必失败**的 async 用例
> ⇒ 两个都报 `FAIL`、退出码 `1` ⇒ 随后删掉 ✓
>
> **另外：假注册表必须有能力失败** ✗ `defineTool` 是那个可注入的接缝 ✓
> 如果替身只会把自己的参数原样返回，`buildTool()` 就会把**原始定义**交给收撤销函数的那个
> 收集器，而它（判断正确）会把它丢掉 ⇒ 测试**看起来注册成功了，其实什么都没注册** ✗
> 测试里的 `fakeDefineTool()` 返回的是撤销函数，和宿主的注册表一样 ✓
> 这个错曾经让一整套绿色的测试盖在一个**根本不可能工作**的工具上 ✓

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
  **按需接口同样受这个上限约束**（它不给自己开后门）✓
  所以连着问两次，就是清超过一轮容量的目录的办法 ✓
  报告里会写明这一轮被上限剩下了几个（`not scanned`），不让那个数字凭空消失 ✓
- **同一时刻只跑一轮** ✗ 已经有一轮在跑时，`clean_session_logs` 不会排队 ✓
  它直接说明情况、什么都不做 ✓ 正在跑的那一轮以日志为准 ✓
- **体积字段只保留两位小数** ✗ 小日志的 `beforeMB` / `afterMB` 都是 `0.0`，没有意义 ✓
  它们是给几 MB、几十 MB 的日志用的 ✓
- **它不能让正在运行的进程把内存吐出来** ✗
  已经开着的会话要关掉重开才会变小 ✓
- **阈值是在 Windows 上的 DeepSeek Harness 调的** ✓
  帧格式、行类型、加载契约都是 DSH 的 ✓
  将来 DSH 改了其中任何一样，结果是 `below-threshold` / `verify-failed` 这类**拒绝**，
  而不是写坏日志 ✓ 这是预期的失败方式，但它仍然是失败 ✗

## 相关插件

同一套东西，三个插件各治一头 —— 都是"会话越跑越长、越贵越慢"这件事：

- **[dsh-history-fold](https://github.com/mili1360/dsh-history-fold)** —— 把可见历史里最老的一段折成
  一条消息（原文进无损归档），每轮请求带的 token 就降下来 ✓
  （和本插件不在同一层：那个减的是**发出去的东西**，本插件减的是**存下来的东西**）
- **[dsh-proactive](https://github.com/mili1360/dsh-proactive)** —— 长任务在 agent 回合结束后才跑完时，
  把它叫醒主动汇报 ✓


## 许可

MIT，见 [LICENSE](LICENSE)。
