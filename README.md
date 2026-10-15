# dsh-session-vacuum

[![dsh-plugin](https://img.shields.io/badge/dsh--plugin-community%20topic-blue)](https://github.com/topics/dsh-plugin)
[![deepseek-harness](https://img.shields.io/badge/deepseek--harness-plugin-4b6bfb)](https://github.com/topics/deepseek-harness)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> **Topics:** `dsh` · `dsh-plugin` · `deepseek-harness` · `session-cleanup` · `session-log` ·
> `streaming-fragments` · `vacuum` · `prune` · `oom` · `long-session` · `memory-optimization`

**Is your DSH session getting slower every turn, slower to load, and eventually unopenable — or
does it just OOM?**

That is not a feeling. Every token is written twice: the fragments (`assistant/chunk` and the
packed `text-chunks` / `reasoning-chunks` / `tool-call-chunks` rows) are read by nothing, and
they pile up. A 1,813-turn conversation reached **272 MB on disk**, expanded into **8.43 million
objects** when loaded, took **1.29 GB resident**, and died.

This plugin deletes them **on disk** — not hidden, actually deleted — so the next load is
smaller before anything else happens. Same session after one pass: **86.67 MB → 16.99 MB**,
169,795 fragments → 0, and the non-fragment content: **nothing lost** (`dropped=0`).

**You are accumulating this from turn one.** Measured on real sessions, the number of prunable
fragments tracks output tokens at roughly **0.95–0.99 : 1** — eight sessions, from 88 fragments
for a 91-token one-turn chat up to 1,498 fragments for a 1,516-token six-turn one. Nothing reads
them; they are write-only.

**It runs unless you stop it**, but you can look before it touches anything: `dryRun: true`
records what it *would* do and writes nothing. A pass that cannot verify its own output writes
nothing at all. The swap is atomic, and `.orig` is never deleted for you.

- **Live by default.** A fresh install prunes idle logs with no configuration at all. Set
  `enabled: false` to switch it off, or `dryRun: true` to watch without writing.
- **Never touches a session written in the last 2 minutes** (`liveWindowMs`).
- **Verified before it writes.** The pruned log has to satisfy the loader contract, or the
  original file is left exactly as it was.
- **Destructive by design.** The fragment rows are **deleted**. `.orig` is the only backup,
  and it is never removed for you. Read [Destructive by design](#destructive-by-design)
  before enabling it.

**Why not just hide the chunks?** Folding them out of the `session.history` response is a real
fix for the client, and this plugin does not replace it — the two run together. But the OOM does
not happen in the client. It happens when DSH loads the log: the file is read, every row is
parsed, and the chunk rows expand into millions of objects. If the file on disk is still 272 MB,
skipping them in the response does not stop the next load from rebuilding them. The only way to
stop that is to make the file smaller — see
[How this differs from the runtime approaches](#how-this-differs-from-the-runtime-approaches).

---

## Why

A session log is a concatenation of zstd frames holding JSONL rows. Every token a model
streams is written twice: once as a raw delta (`assistant/chunk`, and the packed rows
`text-chunks` / `reasoning-chunks` / `tool-call-chunks`) and once inside the finalised
`assistant/message`. **Nothing reads the fragments.** Replay does not read them, the model is
never fed them — the projection layer does not even know the word `chunks`. They accumulate
because they are write-only.

Measured on the session that motivated this plugin, a 1,813-turn conversation:

| | |
|---|---|
| rows in the log | 224,000 |
| file on disk | 272 MB |
| objects after loading it | 8.43 million |
| resident memory while loaded | 1.29 GB |
| outcome | out of memory |

The file is small enough to sit on disk unnoticed and large enough to kill the process that
loads it. And because every load pays that cost, the session gets slower and eventually
unopenable — while the rows that cause it are provably unused.

After a cleanup, on the very same session:

| | before | after |
|---|---|---|
| session file | 86.67 MB | 16.99 MB |
| fragment rows | 169,795 | 0 |
| non-fragment content | — | **nothing lost** (`dropped=0`) |
| generation speed shown by the UI | visibly slow | **back to normal** |

> ⚠️ **The "generation speed" row is an observation, not a promise, and it is not explained.** ✗
> What was observed: after the fragments were removed and load memory dropped, the generation
> speed reported by the UI recovered. No controlled before/after tok/s reading was taken, so
> this records the symptom only. Do not read a mechanism into it: context size, cache hit rate,
> time of day and the reasoning-effort setting were each suspected and each ruled out by data,
> and this is the only thing that was actually confirmed.

### What the file size costs on the read path (measured 2026-10-15)

Opening or refreshing a conversation pays for reading its log: the multi-frame zstd stream is
decompressed and every row is parsed. That is the server-side cost a page refresh sits on, and
it scales with the fragment rows this plugin exists to remove — so it was measured directly.

60 logs that still had their pre-cleanup `.orig` beside them, i.e. the **same conversation** on
both sides, decompress + `JSON.parse` of every row, median of 3 runs each:

| | |
|---|---|
| fragment rows across the 60 logs | 960,042 → 300,868 |
| total read time across the 60 logs | 62,893 ms → 6,668 ms (**9.4×**) |
| median speedup | **×7.77** (best ×97.5, worst ×0.72) |
| a 15,298-row log | 859 ms → **37 ms** (×23) |
| a 6,293-row log | 331 ms → **18 ms** (×18) |

> **Scope, so the number is not over-read** ✗: this measures **the log read** (decompress +
> parse every row). It does **not** cover projection computation or client-side rendering —
> neither was measured. The ×0.72 case is a log whose pre-cleanup copy is barely larger than
> the current one, so it is noise on a small number, not a regression. Eight further pairs were
> discarded outright because their `.orig` predates later activity, which would have compared
> two different conversation lengths.

## What it does

`startDelayMs` after boot, once:

1. **Scan.** Walk `$DSH_HOME/sessions` for `session.jsonl.zstd`, sort by size, biggest first.
   A file modified inside `liveWindowMs` is `skipped-live` — the plugin never swaps a log
   out from under a session that may be writing to it.
2. **Threshold.** Count fragment rows and their share of all event rows. Below `minChunkRows`
   or below `minRatio`, the file is `below-threshold` and is not touched at all. A few
   hundred kilobytes of fragments is not worth rewriting a log for.
3. **Prune.** Delete the fragment rows, then renumber `seq` and **remap every cross-event
   reference** onto the new numbering — `sourceEventSeqs`, `surfaceOp.start/end`,
   `shadowedSeqs` and `data.shadowedRange`. Events that the remap itself invalidates (a
   `surfaceOp` whose endpoints were dropped, a surface event left without a marker) are
   dropped too, in a fixpoint loop, so the result is consistent rather than almost-consistent.
4. **Self-verify.** The pruned log must satisfy the same contract DSH loads by: contiguous
   `seq` from 0, a marker on every surface event, no dangling references and no reference to
   a later event. Any violation ⇒ `verify-failed` ⇒ **nothing is written** and the log keeps
   its original size. Refusing to shrink a file is always acceptable; writing a file that DSH
   cannot load is not.
5. **Replace atomically.** Copy the original to `<log>.orig` (only if it does not already
   exist), write the new bytes to `<log>.vacuum-tmp`, then `rename` it over the log. A crash
   mid-write leaves the original intact.

The plugin cannot shrink an already-loaded session — it does not need to. Pruning the file
means every *future* load is small, which is where the memory, the stutter and the OOM came
from.

## Running it on demand

The boot run has one practical flaw: it skips anything written inside `liveWindowMs`
(2 minutes by default), which is usually *the session you actually care about*. And until
this interface existed, the run reported only how many files it handled, so a scan that
skipped 56 of 58 logs looked identical to a plugin that does nothing.

Two things follow from that, and both are now in place.

### The log says why every file was skipped

`run-end` carries a count per reason, next to the `handled` it always had:

```json
{"mark":"run-end","scanned":58,"handled":2,"vacuumed":2,"wouldVacuum":0,
 "skippedLive":56,"belowThreshold":0,"unreadable":0,"other":0}
```

`handled` keeps exactly the meaning it always had — the files this run actually acted on.
The rest are the ones it did **not** act on, broken down by the same `status` strings the
`file` lines use. **Nothing about what gets cleaned changed**; this is observability only.

### `clean_session_logs` — ask for a pass now

An agent can call the Host tool **`clean_session_logs`**. It takes no required arguments and
one optional one:

| argument | default | meaning |
|---|---|---|
| `dryRun` | `false` | `true` ⇒ preview only: decode, prune and verify every candidate, report what a real pass would save, and write nothing. |

It runs the **same** guarded pass as the boot timer — the same `liveWindowMs` window, the same
two thresholds, the same `vacuumOne()` that backs up to `.orig`, verifies, and replaces
atomically. The on-demand entry point is not a bigger hammer; it just does not have to wait
for the next boot.

It returns **plain text**, not JSON, so what you see in the transcript is what happened:

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

**The session you are running in cannot be cleaned from inside a running DSH**, and the report
says so. DSH holds an active session in memory and keeps appending to its log; rewriting that
file underneath it can corrupt it. The `liveWindowMs` guard already refuses it, and it was not
weakened for this interface. To clean that particular session: **stop DSH** and run
`E:\Dsh百宝箱\清理会话日志.cmd`, or leave it idle for at least `liveWindowMs` and let a later
boot do it.

### The web route (preview only)

A script can also ask, over HTTP:

```
POST /api/local/session-vacuum/run
```

It is **POST-only** and **preview-only**: any body that asks for a real pass is refused with
403, and a profile with `dryRun: false` refuses outright rather than exposing a destructive
endpoint. A real pass belongs to the tool, where an agent owns the call. An empty body (or
`{}` / `{"dryRun":true}`) is a dry run and the response body is the same text report.

Both entry points are inert while `enabled` is `false`, and neither one can bypass the guard.

## How this differs from the runtime approaches

The same dead weight has a second, complementary fix: trim it at **read time** instead of at
**rest**. [dsh-host-history-lite](https://github.com/GithungDang/dsh-host-history-lite)
intercepts the `session.history` / `subagent.history` RPC routes and folds finalised chunk
deltas back into the final `assistant/message` before the response leaves the server. Both
tools attack the same rows; they sit at different layers, and they can be used together.

| | dsh-session-vacuum (this) | runtime slimming |
|---|---|---|
| Where | the session log **on disk** | the **RPC response** |
| When | once per boot, while the session is idle | on every history request |
| What happens to the data | **deleted** — `.orig` is the only backup | kept; just not sent to the client |
| Saves | disk, load time, and the memory a load expands into | transfer size and client parse time |
| Reversible | no | yes, at any time |
| Who benefits | every reader of the log, and every future load | `dsh web` clients only |
| Other tools reading the log | see the smaller log | unaffected |

Pick by what hurts: a `sessions/` directory that keeps growing and a load that expands into
gigabytes → this plugin. A history panel that takes seconds to open → the runtime approach.

> **A note on "nothing reads them".** That is true on the *server*: the fragments are not part
> of the surface the model is sent, and no server-side projection reads them back. The
> **client** does replay them when it opens a finished conversation — which is exactly why the
> runtime approaches can drop them without changing what a finished conversation looks like.

## When it runs

It only works on session logs that are **not currently in use**, so collecting a specific
session is a sequence, not a switch:

1. Switch away from that session — don't just close DSH.
2. Leave it idle for more than 2 minutes (`liveWindowMs`).
3. Restart DSH.
4. About 20 seconds after startup (`startDelayMs`) the plugin sweeps and prunes it.
5. Next time you open that session it loads smaller, faster, and without the OOM.

A session you are using right now is never touched, and a log below the thresholds is never
touched either.

## Install

This is a plugin for a DSH profile. Add it to the profile — the same layer every other DSH
plugin uses.

1. Make the package resolvable by your profile, then add it to the profile manifest
   `~/.dsh/profiles/<profile>/package.json`:

   ```json
   {
     "dependencies": { "dsh-session-vacuum": "link:/path/to/dsh-session-vacuum" },
     "dsh": { "profile": { "bundles": ["@deepseek-ai/dsh-base", "dsh-session-vacuum"] } }
   }
   ```

   Or, using the explicit patch layer, append to
   `~/.dsh/profiles/<profile>/cordis.patch.yml`:

   ```yaml
   - insert:
       - id: session-vacuum
         name: 'file:///path/to/dsh-session-vacuum/lib/index.js'
   ```

2. **Restart DSH.** Plugin rows load at boot.

3. Nothing will happen yet — the defaults are inert. Turn it on by writing
   `~/.dsh/dsh-session-vacuum/config.json`:

   ```json
   { "enabled": true, "dryRun": true }
   ```

   `enabled` is read once at boot, so this needs a restart. `dryRun: true` then logs what it
   *would* remove without writing anything: watch `~/.dsh/logs/session-vacuum.jsonl` until
   the numbers look right, then set `"dryRun": false`.

To remove it, delete the row you added and restart. Nothing else is left behind — the config
file, the log file and any `.orig` backups are the only things this plugin creates.

> **Node 22.15 or newer** is required: the plugin uses `zlib.zstdDecompressSync`. On an older
> runtime it does nothing at all rather than doing something wrong — the decompression call
> fails, no rows are counted, and every file reports `below-threshold`.

## Requirements / compatibility

**After installing, confirm it actually works — a profile that boots is not proof.**
A plugin can load cleanly, serve its routes and still break the host; we have seen exactly
that. So the acceptance test is: **create a new session and send it one message**, then check
`$DSH_HOME/logs/session-vacuum.jsonl` for a `run-start` / `run-end` pair about
`startDelayMs` after boot. If new sessions cannot be created at all, remove the mount line
above and restart.

Developed and tested against DSH **`0.1.0-rc.7`** — a **customised** build, not a stock
upstream release. Another version may register its services differently. This plugin declares
`inject: ['tools']` and uses `ctx.effect`, `ctx.tools.register` (the on-demand tool), and
optionally `ctx.webServer.register` (the preview route).

### Newer DSH: the v4 session format — supported, with two honest gaps

DSH `0.2.0-rc.2` writes session logs as **`session.v4.jsonl.zstd`** (format **v4**), and v4 has
**no fragment rows** — the packed delta runs live inside `assistant/message`'s `data.stream`.
The row-level pruning that works on v0/v1 therefore matched nothing there, and the plugin would
have loaded cleanly and done nothing at all. That path is now implemented (it compacts the
runs *inside* the event while the loader's own reader verifies the result), and it was verified
end-to-end against real sessions in a sandbox: a 5-turn session went 27,715 → 17,071 B, and the
pruned log still loaded, with **every message byte-identical** after the fold.

What is **not** verified, and you should weigh before relying on it:

- **`assistant/attempt` has no real-data coverage.** The code path is exercised and the verifier
  refuses any non-equivalent result, but this build only writes that event for a *failed,
  retried, cancelled or stream-error* attempt — all of the test sessions succeeded, so no real
  sample exists. Treat it as unit-level assurance, not measured.
- **The v4 path has no automated regression.** The bundled test suite covers the v0/v1 row
  format only; v4 is currently held up by the sandbox end-to-end runs described above.

Both gaps are stated so that a silent failure — the exact thing this plugin exists to remove —
is not hidden behind a green test run.

> **Why `inject` is not optional.** Cordis injects services on demand: a service that is not
> declared in `inject` does not exist as a property on `ctx`, and reading it throws
> `cannot get property "tools" without inject`. That is not theory — the first build of this
> interface was deactivated by exactly that line on a real boot. `webServer` is deliberately
> **not** declared, because a headless profile has no web server: the route uses the optional
> `ctx.inject(['webServer'], ...)` form instead, so a profile without one still gets the tool.
> If `ctx.inject` is unavailable too, the plugin logs `mark: "route-failed"` and carries on.

If your build does not match, the plugin **deactivates and says so** — it does **not** stop the
profile from booting. No timer is armed, no session log is touched, and one line lands in
`logs/session-vacuum.jsonl`:

```json
{"mark":"deactivated","reason":"registration-failed","step":"services","error":"..."}
```

`step` names what failed: `services` (`ctx.effect` missing), `ctx.effect` (it threw), or
`ctx.tools` (no usable tools registry). A failing **route** is not in that list: it is logged
as `mark: "route-failed"` and the plugin stays active with its tool, because the route is a
convenience and the tool is the point. `@deepseek-ai/dsh-tools` is resolved at plugin load to
get `defineTool`; if it cannot be resolved the tool is still registered in the bare-object form
(`ctx.tools.register({...})`) rather than disappearing.

**If the profile will not start at all after installing**, remove the mounted lines from
`<profile>/cordis.patch.yml` (or drop `"dsh-session-vacuum"` from `dsh.profile.bundles`, if
that is how you installed it) and restart:

```yaml
- insert:
    - id: session-vacuum
      name: 'file:///path/to/dsh-session-vacuum/lib/index.js'
```

## Configuration

`$DSH_HOME/dsh-session-vacuum/config.json` (falling back to `~/.dsh/…`). Missing file, or a
file that is not valid JSON, means the defaults below. A missing key also means its default.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch. `false` ⇒ loaded but completely idle; not even a timer is started. Read once at boot — changing it needs a restart. |
| `dryRun` | `true` | `true` ⇒ decode, prune and verify in memory, then log `would-vacuum` and write nothing. |
| `startDelayMs` | `20000` | Milliseconds after boot before the scan starts, so it does not compete with startup. |
| `liveWindowMs` | `120000` | A log modified more recently than this (2 minutes) counts as live and is `skipped-live`. |
| `minChunkRows` | `500` | Do not touch a log with fewer fragment rows than this. |
| `minRatio` | `0.25` | Do not touch a log where fragment rows are less than this share of all event rows. |
| `maxFilesPerRun` | `12` | Cap on files handled per boot, largest first. Files skipped as live or below-threshold do not count against it. |
| `logFile` | `logs/session-vacuum.jsonl` | Relative to `$DSH_HOME`. |
| `minIntervalMs` | `21600000` | Declared (six hours) but **not enforced in 0.1.0** — maintenance is a single `setTimeout` per boot, so there is no repeat run for it to gate. See `CHANGELOG.md`. |

Two more keys are read by the code but are not defaults: `skipIfBackedUp`, which appears in
the `.orig` guard whose body is empty and therefore has no effect, and the `status` strings,
which are outputs rather than inputs. Both are listed under
[Known limitations](#known-limitations).

## Log

`$DSH_HOME/logs/session-vacuum.jsonl`, one JSON object per line. Every line carries a `t`
timestamp and a `mark`:

| `mark` | Meaning |
|---|---|
| `run-start` | A scan began. Carries `files` (how many logs were found), `dryRun`, and `trigger` (`boot`, `tool` or `route` — which entry point asked). |
| `file` | One file was handled, with its `status`. |
| `run-end` | The scan finished. Carries `handled` — how many files the run acted on — plus `scanned` and a count per skip reason: `vacuumed`, `wouldVacuum`, `skippedLive`, `belowThreshold`, `unreadable`, `other`. |
| `run-error` | The scan itself failed (the per-file errors are reported as `file` lines). |
| `route-failed` | The web route could not be registered. Reported, not fatal: the tool still works. |
| `deactivated` | A registration failed and the plugin stood down (see [Requirements / compatibility](#requirements--compatibility)). |

`status` on a `file` line is one of:

| `status` | Meaning |
|---|---|
| `would-vacuum` | `dryRun`. `beforeMB` / `afterMB` / `chunks` / `kept` / `dropped` say what a real run would do. |
| `vacuumed` | Done: the log was replaced and `<log>.orig` holds the pre-vacuum bytes. |
| `below-threshold` | Under `minChunkRows` or `minRatio`; `chunks` and `ratio` say by how much. |
| `skipped-live` | Modified inside `liveWindowMs` — possibly being written right now. |
| `verify-failed` | The pruned log failed the loader contract. `problems` lists the first five. Nothing was written. |
| `unreadable` | Not a zstd frame stream. |
| `unreadable-tail` | The frame walk could not account for the tail. Nothing was written. |
| `unparsable` | A line in the decoded text is not JSON. Nothing was written. |
| `prune-failed` | The `session` header row is missing. |
| `error` | Anything else about this file. |

A run is a diagnostic pass first: `dryRun: true` for long enough to see `would-vacuum` lines
with plausible `beforeMB` / `afterMB` is the intended way to decide. `clean_session_logs`
with `dryRun: true` gives you the same picture for one session, on demand.

## Safety model

- **Live by default, and that is deliberate.** `enabled` and `dryRun` both default to the
  working values, because an install that quietly does nothing is the more common failure: the
  user concludes the plugin is broken and removes it. To watch first, set `dryRun: true`; to
  switch it off entirely, set `enabled: false` — then `apply()` returns before creating a timer,
  nothing is read, written or logged, `clean_session_logs` reports that the plugin is disabled
  instead of scanning, and the web route answers 403.
- **The on-demand interface is not a bigger hammer.** It calls the same `runVacuum()` and the
  same `vacuumOne()`, with the same `liveWindowMs` guard and the same thresholds. It cannot
  clean the caller's own session, cannot exceed `maxFilesPerRun`, and the HTTP route cannot
  write at all.
- **Dry by default.** `dryRun: true` decodes, prunes and verifies the whole log and then
  throws the result away.
- **Never a live file.** Anything touched inside `liveWindowMs` is skipped. The point of the
  whole design is that this only ever runs against logs DSH is not writing.
- **Never a small file.** Both floors must be exceeded before a byte is rewritten.
- **Verify before write.** The loader contract is re-checked on the pruned result, not
  assumed. A refusal costs disk space; a bad log costs the session.
- **Atomic replace.** `write temp → rename`. The log is either the old bytes or the new ones.
- **One backup, byte-exact.** `.orig` is written before the rename and is never removed or
  overwritten by the plugin.
- **Serial and cooperative.** One file at a time, `setImmediate` between files, so a scan
  cannot stall startup or a turn.
- **Fails open.** Every failure is a `status` string in the log. Nothing is ever thrown into
  DSH's startup path.

## Destructive by design

This is not a compactor and it does not archive. It **deletes rows from session logs**, and
you should be clear about what that means:

- **The fragments are gone.** They are not moved, summarised or written anywhere else. After
  a real run, the only copy of the pre-vacuum log is `<log>.orig`.
- **`.orig` is the only backup**, and it is **never removed automatically**. This is
  deliberate: an automatic cleanup would silently turn a recoverable mistake into an
  unrecoverable one.
- **Nothing is pruned from `.orig` either.** Space is only reclaimed when *you* delete the
  `.orig` files, and the moment you do, those fragments are unrecoverable.
- **There is no undo path inside DSH.** Restoring means stopping DSH, replacing
  `session.jsonl.zstd` with its `.orig`, and starting again. Note that a session already
  loaded in memory is unaffected either way — the plugin changes what the *next* load sees.
- **The loss is bounded but real.** The deleted rows are, as far as the current DSH version
  is concerned, write-only. If a future version starts reading raw streaming deltas — replay
  of the typing animation is the obvious candidate — the pruned information will not be
  there. That is the trade: 272 MB and 1.29 GB of objects, against fragments that nothing
  currently reads.

Given all that, the intended sequence is: install with the defaults, set `dryRun: true`, read
`logs/session-vacuum.jsonl`, pick the thresholds you are comfortable with, and only then set
`"dryRun": false` — with backups you are willing to lose.

## Measured results

| | |
|---|---|
| the motivating session | 1,813 turns / 224,000 rows / 272 MB on disk |
| objects after loading it | 8.43 million |
| resident memory | 1.29 GB (then out of memory) |
| what a vacuum leaves in the file | the `session` header and every event row that is not a streaming fragment |

Those first three numbers are the incident, measured on the machine where the plugin was
written — they are why it exists, not a benchmark of it. What the plugin does to *any* log is
stated exactly by the counts it logs: `chunks` rows removed, `kept` events retained, and
`beforeMB` → `afterMB`. Run it in `dryRun` and you have the numbers for your own sessions,
which is more useful than anyone else's.

The rewrite has been verified end to end offline: a synthetic log of the same shape is
pruned, replaced, then re-read by decompressing its individual zstd frames and parsing every
JSONL line, and the surviving rows are compared against exactly the non-fragment rows that
went in (`test/vacuum-tests.mjs`).

## Tests

```bash
npm test          # node test/vacuum-tests.mjs
npm run test:vacuum
```

They run fully offline: no DSH, no network, no API key, and a throwaway `DSH_HOME` under
`os.tmpdir()` — `process.env.DSH_HOME` is redirected **before** the plugin is imported, and
every log in the suite is synthesised locally. The suite prints `31 passed, 0 failed` and
exits non-zero if anything failed.

| Area | Cases | Covers |
|---|---|---|
| detection | 2 | all four fragment row types match; `chunk-stub` and every other near miss do **not** |
| thresholds | 2 | too few fragment rows; fragment ratio under `minRatio`, with a control proving the same file qualifies once the floor is reachable |
| live guard | 2 | a file written a second ago is left alone; the same file is processed when the window shrinks — the guard is the mtime window, not the file name |
| backups | 1 | a pre-existing `.orig` is never overwritten (see the note below) |
| prune | 2 | header kept, every non-fragment event kept, `seq` contiguous from 0; `sourceEventSeqs`, a replace `surfaceOp` and `data.shadowedRange` all remapped onto the new numbering |
| verify gate | 1 | a log that fails the contract is reported `verify-failed` and the file is byte-identical afterwards, with no `.orig` |
| dry run | 1 | `would-vacuum`, a reported saving, and a byte-identical file |
| real run | 3 | the log is replaced and smaller, `.orig` is byte-exact, no temp file is left, and the result re-reads frame by frame as valid JSONL with the same events in the same order |
| bad input | 2 | a non-zstd file and a torn JSON line are reported, not rewritten |
| discovery / startup | 2 | recursion finds only `session.jsonl.zstd`; `runVacuum` scans, honours `maxFilesPerRun`, logs each file, and leaves the files alone under `dryRun` |
| skip-reason counts | 1 | one directory holding a qualifying log, a live log, a below-threshold log and one the cap leaves alone ⇒ `run-end` reports `scanned: 4`, `skippedLive: 1`, `belowThreshold: 1`, `unreadable: 0`, `other: 0`, and `handled` still means "acted on" |
| on-demand tool | 5 | the text report names each clean and each skip **with its reason** and the before→after MB; the caller-session caveat is stated with what to do instead; `dryRun: true` changes no byte and no mtime and backs nothing up; a real pass cleans exactly what it reported cleaning; the live guard still refuses a just-written log |
| web route | 1 | GET is refused with 405; a body asking for a real pass is refused with 403; a profile with `dryRun: false` refuses outright; an empty POST is a preview; a disabled plugin scans nothing |
| registration | 4 | a hostile `ctx` (`effect` throwing, `effect` absent, `ctx` null, `tools` absent) makes `apply()` return normally, log exactly one `deactivated` line per failure, and arm no timer; the tool is registered **synchronously**; a route failure costs the route only; disposal removes both registrations |

> **For contributors:** `test()` **must** `await fn()`, in every suite. A synchronous harness
> reports `ok` before any async assertion runs and lets the script reach the exit line first,
> which silently turns the whole suite into a no-op. This has really happened to this family
> of plugins — see `CHANGELOG.md` in
> [dsh-history-fold](https://github.com/mili1360/dsh-history-fold). The harness here was
> checked the same way: two deliberately failing async cases were added, both were reported
> `FAIL` and the run exited `1`, and the cases were then removed.
>
> **And a mock registry must be able to fail.** `defineTool` is the injected seam; a stand-in
> that just returns its argument makes `buildTool()` hand the raw definition to the disposer
> collector, which (correctly) drops it — the test then looks like it registered a tool while
> nothing was registered at all. `fakeDefineTool()` in the suite returns a disposer, like the
> host does. That mistake produced a green suite over a tool that could never have worked.

## Known limitations

- **Frame boundaries come from the frame structure, not from a magic-byte scan.** An earlier
  revision of this file sliced each frame up to the next `28 b5 2f fd`. Those four bytes can
  occur inside a frame's payload; Node's `zstdDecompressSync` does not throw on a mis-sliced
  frame (it returns an empty buffer), so the cut landed on a clean line boundary and the
  *truncated* row set still parsed and still passed self-verification — a 661-row synthetic
  log was read back as 200 rows and rewritten to 38 with status `vacuumed`. The walker now
  parses the frame header and every block header, and any bytes it cannot account for are
  reported as `truncated`, which makes `vacuumOne()` return `unreadable-tail` and write
  nothing. Two regression tests cover it; under the old rule both go red.
- **A session is not skipped because a `.orig` exists — deliberately.** Skipping on the mere
  presence of the backup would maintain a session only once, because `.orig` is never
  removed. Every run re-evaluates the thresholds instead. The guarantee that does exist:
  the first backup is never overwritten.
- **`minIntervalMs` does nothing.** Maintenance runs once per boot; there is no second run.
- **A pass is capped at `maxFilesPerRun` files, whichever entry point asks.** The tool does not
  get to ignore the cap, so asking it twice in a row is a way to work through a directory
  larger than one pass. The report says how many logs were left by the cap (`not scanned`) so
  that count is never silently missing.
- **One pass at a time.** If a scan is already running, `clean_session_logs` does not queue a
  second one — it says so and does nothing. The log is the record of the pass that is running.
- **Only one file at a time, and at most `maxFilesPerRun` per boot.** A directory with many
  oversized logs is cleaned up over several restarts.
- **The size fields are rounded to two decimals**, so `beforeMB` and `afterMB` are useless for
  small logs. They exist for the multi-megabyte ones.
- **It does not and cannot free memory in the running process.** A session that is already
  open stays as large as it is until it is closed and reopened.
- **Tuned and measured against DeepSeek Harness on Windows.** The framing, the row types and
  the loader contract are DSH's; a version that changes any of them will turn into
  `below-threshold` / `verify-failed` refusals rather than corrupted logs, which is the
  intended failure mode but is still a failure mode.

## Related plugins

Three small DSH plugins, each aimed at one part of the same problem — a session that gets
expensive and slow the longer it runs:

- **[dsh-history-fold](https://github.com/mili1360/dsh-history-fold)** — folds the oldest part
  of the visible history into one message backed by a lossless local archive, so each request
  carries fewer tokens. (Different layer: that one trims what is sent, this one trims what is
  stored.)
- **[dsh-proactive](https://github.com/mili1360/dsh-proactive)** — wakes the agent when a long
  background task finishes after the turn has ended, so it reports back on its own.


## License

MIT — see [LICENSE](LICENSE).

中文说明见 [README.zh-CN.md](README.zh-CN.md)。
