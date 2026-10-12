# dsh-session-vacuum

[![dsh-plugin](https://img.shields.io/badge/dsh--plugin-community%20topic-blue)](https://github.com/topics/dsh-plugin)
[![deepseek-harness](https://img.shields.io/badge/deepseek--harness-plugin-4b6bfb)](https://github.com/topics/deepseek-harness)
[![license: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> **Topics:** `dsh-plugin` · `deepseek-harness` · `session-log` · `streaming-fragments` ·
> `vacuum` · `prune` · `oom` · `long-session` · `memory-optimization`

A [DSH](https://github.com/deepseek-ai/deepseek-harness) plugin that prunes the **dead
streaming fragments** out of session logs at startup, so a long session stops expanding into
gigabytes of objects every time it is loaded.

- **Inert by default.** A fresh install starts no timer, reads nothing and writes nothing
  until you configure it.
- **Dry-run by default.** Even switched on, it only logs what it *would* save until you turn
  `dryRun` off.
- **Verified before it writes.** The pruned log has to satisfy the loader contract, or the
  original file is left exactly as it was.
- **Destructive by design.** The fragment rows are **deleted**. `.orig` is the only backup,
  and it is never removed for you. Read [Destructive by design](#destructive-by-design)
  before enabling it.

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

## Configuration

`$DSH_HOME/dsh-session-vacuum/config.json` (falling back to `~/.dsh/…`). Missing file, or a
file that is not valid JSON, means the defaults below. A missing key also means its default.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Master switch. `false` ⇒ loaded but completely idle; not even a timer is started. Read once at boot — changing it needs a restart. |
| `dryRun` | `true` | `true` ⇒ decode, prune and verify in memory, then log `would-vacuum` and write nothing. |
| `startDelayMs` | `20000` | Milliseconds after boot before the scan starts, so it does not compete with startup. |
| `liveWindowMs` | `600000` | A log modified more recently than this (10 minutes) counts as live and is `skipped-live`. |
| `minChunkRows` | `2000` | Do not touch a log with fewer fragment rows than this. |
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
| `run-start` | A scan began. Carries `files` (how many logs were found) and `dryRun`. |
| `file` | One file was handled, with its `status`. |
| `run-end` | The scan finished. Carries `handled` — how many files the run acted on. |
| `run-error` | The scan itself failed (the per-file errors are reported as `file` lines). |

`status` on a `file` line is one of:

| `status` | Meaning |
|---|---|
| `would-vacuum` | `dryRun`. `beforeMB` / `afterMB` / `chunks` / `kept` / `dropped` say what a real run would do. |
| `vacuumed` | Done: the log was replaced and `<log>.orig` holds the pre-vacuum bytes. |
| `below-threshold` | Under `minChunkRows` or `minRatio`; `chunks` and `ratio` say by how much. |
| `skipped-live` | Modified inside `liveWindowMs` — possibly being written right now. |
| `verify-failed` | The pruned log failed the loader contract. `problems` lists the first five. Nothing was written. |
| `unreadable` | Not a zstd frame stream. |
| `unparsable` | A line in the decoded text is not JSON. Nothing was written. |
| `prune-failed` | The `session` header row is missing. |
| `error` | Anything else about this file. |

A run is a diagnostic pass first: `dryRun: true` for long enough to see `would-vacuum` lines
with plausible `beforeMB` / `afterMB` is the intended way to decide.

## Safety model

- **Inert by default.** No config file ⇒ `enabled: false` ⇒ `apply()` returns before creating
  a timer. Nothing is read, written or logged.
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
went in (`test/vacuum-tests.mjs`, 18 cases).

## Tests

```bash
npm test          # node test/vacuum-tests.mjs
npm run test:vacuum
```

They run fully offline: no DSH, no network, no API key, and a throwaway `DSH_HOME` under
`os.tmpdir()` — `process.env.DSH_HOME` is redirected **before** the plugin is imported, and
every log in the suite is synthesised locally. The suite prints `18 passed, 0 failed` and
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

> **For contributors:** `test()` **must** `await fn()`, in every suite. A synchronous harness
> reports `ok` before any async assertion runs and lets the script reach the exit line first,
> which silently turns the whole suite into a no-op. This has really happened to this family
> of plugins — see `CHANGELOG.md` in
> [dsh-history-fold](https://github.com/mili1360/dsh-history-fold). The harness here was
> checked the same way: two deliberately failing async cases were added, both were reported
> `FAIL` and the run exited `1`, and the cases were then removed.

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

## License

MIT — see [LICENSE](LICENSE).

中文说明见 [README.zh-CN.md](README.zh-CN.md)。
