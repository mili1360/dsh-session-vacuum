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
- **Offline test suite** (`test/vacuum-tests.mjs`, 18 cases) that never touches the real
  `DSH_HOME`: chunk-row detection including the `chunk-stub` non-match, both thresholds, the
  live-file window, the existing-`.orig` behaviour, the prune and its remapping, the verify
  gate, `dryRun`, the real run, and a re-read of the rewritten log.

### Known issues

Found while writing the test suite and the documentation. Reported rather than papered over;
none of them are fixed in this release.

- **A stray zstd magic sequence silently truncates the log that gets written.**
  `decompressFrames()` finds frame boundaries by scanning for `28 b5 2f fd`. If those four
  bytes occur *inside* a frame's payload, the scan produces a bogus boundary, the following
  slice fails to decompress, and the decode loop stops there — returning a *truncated* text
  with no error. `vacuumOne()` then prunes what it got, passes self-verification (the cut
  lands on a frame boundary, so every remaining line parses), and replaces the live log with
  the truncated version. Reproduced: a synthetic 661-row log reads back as 200 rows and is
  rewritten to 38 rows with status `vacuumed`. `.orig` still holds the original bytes, so it
  is recoverable, but the plugin reports success.
- **`.orig` never establishes a skip.** The guard
  `if (fs.existsSync(file + '.orig') && cfg.skipIfBackedUp !== false) { }` has an empty body,
  and `skipIfBackedUp` is not part of `DEFAULTS`. A session that has already been vacuumed is
  therefore processed again on every run; the implemented and tested guarantee is only that
  the *first* backup is never overwritten.
- **`minIntervalMs` is documented but dead.** It is declared in `DEFAULTS` (six hours) and
  read nowhere: maintenance is a one-shot `setTimeout` per boot, so there is no second run for
  an interval to gate.
- **`verify-failed` log lines omit `chunks`.** `below-threshold`, `would-vacuum` and
  `vacuumed` all report how many fragment rows were seen; the refusal path does not.
- **`beforeMB` / `afterMB` are rounded to two decimals**, so for small logs both read `0.0`
  and the logged saving is meaningless. It is exact for the multi-megabyte logs this plugin
  exists for.
