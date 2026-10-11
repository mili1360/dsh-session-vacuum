# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

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
