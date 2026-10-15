# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Docs

- **The read-path cost of the file size is measured and documented** (both READMEs). Opening or
  refreshing a conversation pays for decompressing the multi-frame zstd log and parsing every
  row, and that cost scales with the fragment rows this plugin removes — so it was measured on
  60 logs that still had their pre-cleanup `.orig` beside them (the same conversation on both
  sides), decompress + `JSON.parse` of every row, median of 3 runs: **960,042 rows → 300,868**
  and **62,893 ms → 6,668 ms** across the set, a **median speedup of ×7.77** (a 15,298-row log
  goes 859 ms → 37 ms; a 6,293-row one goes 331 ms → 18 ms). The section states plainly
  what it does NOT cover — projection computation and client-side rendering were not measured —
  so the number cannot be read as a whole-page-refresh promise. Eight pairs were discarded
  because their `.orig` predates later activity and the two sides were no longer the same
  conversation length.

### Added

- **`clean_session_logs`: run a pass on demand.** A Host tool, registered with
  `ctx.tools.register` exactly the way this codebase does it, so an agent no longer has to
  wait for the boot-time run — which skips everything written inside `liveWindowMs`, i.e.
  usually the session a user actually cares about. It takes no required arguments and one
  optional `dryRun` boolean, and it returns a **plain-text report**: logs scanned, logs
  cleaned (before → after MB per file, capped at a few lines), total saved, and how many logs
  were skipped **broken down by reason** — plus each skipped file's own reason. It runs the
  **same** guarded pass as the boot timer: same `runVacuum()`, same `vacuumOne()`, same
  `liveWindowMs`, same thresholds. It is not a bigger hammer and it cannot clean the caller's
  own session; the report says so, and says what to do instead (stop DSH and run
  `E:\Dsh百宝箱\清理会话日志.cmd`, or let a later boot clean it once it has been idle for
  `liveWindowMs`). `defineTool` is resolved from `@deepseek-ai/dsh-tools` at plugin load, so
  the tool registers **synchronously**; if that package cannot be resolved the tool is still
  registered in the bare-object form rather than disappearing.
- **A preview-only web route**, `POST /api/local/session-vacuum/run`, registered with
  `ctx.webServer.register({ kind, path, handler })`. POST-only (405 otherwise) and
  preview-only: a body asking for a real pass gets 403, and a profile configured with
  `dryRun: false` refuses outright rather than exposing a destructive endpoint. A real pass
  belongs to the tool, where an agent owns the call. Route registration failure is logged as
  `mark: "route-failed"` and is not fatal — the tool is the part that matters.
- **Skip-reason counts in `run-end`**: `scanned`, `vacuumed`, `wouldVacuum`, `skippedLive`,
  `belowThreshold`, `unreadable`, `other`, alongside the `handled` that was always there
  (unchanged in meaning: the files the run acted on). Previously the only thing a run reported
  was how many files it handled, so 56 skipped files and their reasons were invisible and a
  working plugin looked like a no-op. `run-start` also carries `trigger` (`boot` / `tool` /
  `route`) so the log says which entry point asked. **What gets cleaned did not change** —
  this is observability only.
- **16 new tests** (31 total): the skip-reason counters over a directory holding a qualifying,
  a live, a below-threshold and a cap-excluded log; the tool report naming each clean and each
  skip with its reason *and* carrying the caller-session caveat; `dryRun: true` changing no
  byte and no mtime; a real pass cleaning exactly what it reported; the live guard still
  refusing a just-written log through the tool path; the route's 405/403/preview/disabled
  behaviour; synchronous tool registration; a route failure costing only the route; and
  disposal removing both registrations. Three of them were proven non-vacuous by mutation:
  hard-coding the skip counters to 0, removing the caveat lines, and dropping the tool's
  `dryRun` argument each turned the suite red and restoring each turned it green.

### Changed

- **`apply()` registers `ctx.effect` before the other registrations.** The effect callback is
  the host's only handle for undoing them; if `apply()` failed later while the effect had not
  been handed over yet, the boot timer it had already armed could not be cleared — a plugin
  that says "deactivated" and keeps scanning. It is now registered first, and every later
  failure goes through the same deactivation path.
- **Two bugs found while building this, both silent.** (1) The tool's `output.schema` used a
  root `required` array; DSH's value-schema DSL wants `required: true` on the property and
  rejects the array with `UNSUPPORTED_SCHEMA`, so the tool could never have registered — the
  fix follows `dsh-tool-fs` / `dsh-tool-todo`. (2) The registrar passed to `registerTool` was
  the disposer collector, not a function that calls `ctx.tools.register`; the collector saw a
  non-function, dropped it, and the plugin reported success while the tool never entered the
  registry. Both are now pinned by tests that assert on what the registry actually receives.

### Fixed

- **The plugin was deactivated on a real boot by its own tool registration.** Cordis injects
  services on demand, so a service not declared in `inject` is not a property on `ctx` at all:
  `ctx.tools` threw `cannot get property "tools" without inject` and the plugin stood down —
  correctly and visibly, thanks to the existing hardening, but the interface was unusable. The
  offline suite could not have caught this; the sandbox boot did, which is why the boot is part
  of the acceptance test. `inject` is now `['tools']`, and the route uses the optional
  `ctx.inject(['webServer'], ...)` form instead of being declared, so a headless profile still
  gets the tool.
- **Hardening: `apply()` can no longer take a profile down.** The `ctx.effect` registration is
  now guarded, and a build that does not expose it gets ONE line in
  `logs/session-vacuum.jsonl` (`mark: "deactivated"`, `reason: "registration-failed"`, the
  failing `step`, and the error) and the plugin deactivates instead of throwing out of
  `apply()` into the loader. Deactivating also clears the timer it had already armed, so the
  plugin cannot log "deactivated" and then quietly scan session logs anyway. The trigger was a
  real third-party plugin (`cancanqiqi/dsh-human-subagent`) whose `apply()` threw
  `Cannot read properties of undefined (reading 'register')` and stopped a whole DSH profile
  from booting. Nothing on the success path changed — `enabled: false` still returns before
  anything else, and the config read is untouched. `README.md` / `README.zh-CN.md` now carry a
  *Requirements / compatibility* section.
- **A regression test for it** (`test/vacuum-tests.mjs`: "apply() returns normally when the
  context is hostile, and records the deactivation"), covering a throwing `ctx.effect`, a
  missing `ctx.effect` and a null `ctx`, and asserting one deactivation line per failure, no
  timer left running, and that a healthy `ctx` still mounts the timer. Proven by hand, not just
  by the green suite: removing the pre-check and removing the try/catch each turn the new case
  red (`20 passed, 1 failed`) and restoring them turns it green (`21 passed, 0 failed`).

## [0.1.0] - 2026-10-11

First release. It prunes, it verifies before it writes, and the rewritten log is confirmed
readable by decompressing its zstd frames and parsing every JSONL line.

### Added

- **Startup vacuum.** `startDelayMs` after boot, the plugin walks `$DSH_HOME/sessions`, sorts
  the logs by size, and prunes the ones whose streaming fragments are worth removing. It runs
  once per boot, serially, yielding the event loop between files.
- **Fragment-only pruning.** It deletes rows of type `assistant/chunk`, `text-chunks`,
  `reasoning-chunks` and `tool-call-chunks` — rows nothing reads after the corresponding
  `assistant/message` is finalised. Everything else is kept, including the `session` header.
- **Reference remapping.** Deleting rows renumbers `seq`, so the four cross-event references
  (`sourceEventSeqs`, `surfaceOp.start/end`, `shadowedSeqs`, `data.shadowedRange`) are
  remapped onto the new numbering, with a fixpoint loop for events that the remap itself
  invalidates.
- **Self-verify before writing.** The pruned log must satisfy the loader contract — contiguous
  `seq`, a marker on every surface event, no dangling or non-earlier references — or nothing
  is written and the file keeps its original size.
- **Atomic replace, with one backup.** The original is copied to `<log>.orig` and never
  removed automatically; the new bytes are written to `<log>.vacuum-tmp` and renamed over the
  log.
- **Inert and dry by default.** `enabled: false` / `dryRun: true`: a fresh install starts no
  timer at all, and turning it on still only logs what it *would* save.
- **Offline test suite** (`test/vacuum-tests.mjs`, 20 cases) that never touches the real
  `DSH_HOME`: chunk-row detection including the `chunk-stub` non-match, both thresholds, the
  live-file window, the existing-`.orig` behaviour, the prune and its remapping, the verify
  gate, `dryRun`, the real run, a re-read of the rewritten log, and the two frame-boundary
  regressions below.

### Fixed

- **Frame boundaries were found by scanning for the zstd magic, which truncated silently.**
  `decompressFrames()` sliced each frame up to the next `28 b5 2f fd`. Those four bytes can
  occur *inside* a frame's payload, and `zlib.zstdDecompressSync` does not throw on a
  mis-sliced frame — it returns an empty buffer — so the cut landed on a clean line boundary,
  the truncated row set still parsed, still passed `verifyPruned()`, and the live log was
  replaced by it with status `vacuumed`. Repro before the fix: a synthetic 661-row log read
  back as 200 rows and was rewritten to 38. The walker now parses the frame header and every
  block header (RFC 8878), so no payload byte can be mistaken for a boundary; whatever it
  cannot account for is reported as `truncated`, and `vacuumOne()` answers `unreadable-tail`
  and writes nothing. Cross-checked against an independent decoder on two real logs
  (19,938 and 44 frames, both consumed to the last byte, neither truncated). Two regression
  tests cover it, and both go red when the old rule is restored.
- **The dead `.orig` guard is gone.** `if (fs.existsSync(file + '.orig') && cfg.skipIfBackedUp !== false) { }`
  had an empty body and `skipIfBackedUp` was never a default. Removing it makes the actual
  behaviour explicit instead of implying a skip that never happened; a test pins the guarantee
  that does exist — the first backup is never overwritten.
- **`verify-failed` now reports `chunks`**, like every other refusal path.

### Known issues

Found while writing the test suite and the documentation. Reported rather than papered over.

- **`.orig` deliberately does not establish a skip.** Skipping on the mere presence of the
  backup would maintain a session only once, because `.orig` is never removed. Every run
  re-evaluates the thresholds instead.
- **`minIntervalMs` is documented but dead.** It is declared in `DEFAULTS` (six hours) and
  read nowhere: maintenance is a one-shot `setTimeout` per boot, so there is no second run for
  an interval to gate.
- **`beforeMB` / `afterMB` are rounded to two decimals**, so for small logs both read `0.0`
  and the logged saving is meaningless. It is exact for the multi-megabyte logs this plugin
  exists for.
