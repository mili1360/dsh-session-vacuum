// Offline tests for dsh-session-vacuum: chunk-row detection, the two thresholds, the
// live-file guard, the prune, the self-verify, and the atomic replace.
//
// Nothing here touches a running DSH or the real DSH_HOME. DSH_HOME is pointed at a
// throwaway directory BEFORE the plugin is imported, and every session log in this suite
// is synthesised locally.
//
// Run: npm test   (or: node test/vacuum-tests.mjs)
//
// ASCII ONLY in syntax positions -- this box runs a GBK code page.

import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import zlib from 'node:zlib'

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'session-vacuum-test-'))
// Set BEFORE the plugin is imported: readConfig(), logLine() and runVacuum() all resolve
// their paths through dshHome(), and every case below writes only under this directory.
process.env.DSH_HOME = tmp
fs.mkdirSync(path.join(tmp, 'dsh-session-vacuum'), { recursive: true })

const plugin = await import('../lib/index.js')
assert.equal(plugin.name, 'dsh-session-vacuum')

let passed = 0
let failed = 0

// NOTE: this MUST await fn(). Every case below is `await test(..., async () => {...})`.
// A synchronous harness would print "ok" before a single async assertion ran, let the
// script reach the summary line first, and silently turn the whole suite into a no-op --
// which is the exact defect dsh-history-fold shipped for weeks while reporting
// "13 passed, 0 failed". Do not "optimise" the await away.
async function test(label, fn) {
  try {
    await fn()
    passed++
    console.log('  ok   ' + label)
  } catch (err) {
    failed++
    console.log('  FAIL ' + label + ' -> ' + (err && err.message ? err.message : String(err)))
  }
}

// ---------------------------------------------------------------- fixtures

const ZSTD_MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd])
// Deliberately different from the plugin's own batch size (500), so a fixture written here
// cannot accidentally agree with a broken reader: the framing must really be multi-frame.
const FRAME_LINES = 64

// Pack JSONL lines into a multi-frame zstd stream. This is the suite's OWN writer;
// building the fixture with the code under test would prove nothing.
function packFrames(lines) {
  const frames = [zlib.zstdCompressSync(Buffer.from(lines[0] + '\n', 'utf8'))]
  for (let i = 1; i < lines.length; i += FRAME_LINES) {
    frames.push(zlib.zstdCompressSync(Buffer.from(lines.slice(i, i + FRAME_LINES).join('\n') + '\n', 'utf8')))
  }
  return Buffer.concat(frames)
}

function frameStarts(buffer) {
  const starts = []
  let at = buffer.indexOf(ZSTD_MAGIC)
  while (at >= 0) {
    starts.push(at)
    at = buffer.indexOf(ZSTD_MAGIC, at + 4)
  }
  return starts
}

// The suite's OWN reader, likewise independent of lib/index.js.
function unpackFrames(buffer) {
  const starts = frameStarts(buffer)
  assert.ok(starts.length > 0, 'fixture must be a zstd frame stream')
  const parts = []
  for (let i = 0; i < starts.length; i++) {
    const to = i + 1 < starts.length ? starts[i + 1] : buffer.length
    parts.push(zlib.zstdDecompressSync(buffer.subarray(starts[i], to)))
  }
  return Buffer.concat(parts).toString('utf8')
}

const CHUNK_KINDS = ['assistant/chunk', 'text-chunks', 'reasoning-chunks', 'tool-call-chunks']

// Deterministic filler for fragment payloads. Real streaming fragments are natural language,
// which compresses only mildly; a fixture of identical stubs would compress to nothing and
// would make every size assertion ("a dry run must report a saving") vacuous.
const NOISE_ALPHABET = 'abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789'
let noiseSeed = 20260101
function noise(n) {
  let out = ''
  for (let i = 0; i < n; i++) {
    noiseSeed = (noiseSeed * 1103515245 + 12345) & 0x7fffffff
    out += NOISE_ALPHABET[(noiseSeed >>> 8) % NOISE_ALPHABET.length]
  }
  return out
}

/**
 * Build one synthetic session log as storage rows, header first.
 * Shape mirrors a real log: a `session` header, then per turn a user/message, a burst of
 * streaming fragments, the finalised assistant/message, a tool result, and another burst.
 *
 * @returns { rows, chunkRows, events } - `events` are the non-chunk, non-header rows.
 */
function buildRows(opts = {}) {
  const turns = opts.turns === undefined ? 12 : opts.turns
  const chunksPerTurn = opts.chunksPerTurn === undefined ? 5 : opts.chunksPerTurn
  const header = { type: 'session', id: 'session-test-0001', version: 1, createdAt: '2026-01-01T00:00:00.000Z' }
  const rows = [header]
  const events = []   // non-chunk, non-header rows only
  let chunkRows = 0
  let seq = 0
  const push = (row, isChunk) => {
    row.seq = seq
    seq++
    rows.push(row)
    if (isChunk !== true) events.push(row)
    return row.seq
  }
  const chunk = (i, k, tag) => {
    chunkRows++
    push({ type: CHUNK_KINDS[(i + k) % CHUNK_KINDS.length], data: { chunk: tag + '-' + i + ':' + k + ' ' + noise(200), bytes: 256 } }, true)
  }

  const plain = []
  for (let i = 0; i < turns; i++) {
    plain.push(push({
      type: 'user/message',
      surfaceOp: 'append',
      data: { message: { id: 'u' + i, role: 'user', content: [{ type: 'text', text: 'ask ' + i }], source: { kind: 'user' } } },
    }))
    for (let k = 0; k < chunksPerTurn; k++) chunk(i, k, 'head')
    plain.push(push({
      type: 'assistant/message',
      surfaceOp: 'append',
      sourceEventSeqs: [plain[plain.length - 1]],
      data: { message: { id: 'a' + i, role: 'assistant', content: [{ type: 'text', text: 'answer ' + i }], source: { kind: 'model' } } },
    }))
    plain.push(push({
      type: 'tool/result',
      surfaceOp: 'append',
      data: {
        message: {
          id: 'r' + i,
          role: 'user',
          content: [{ type: 'tool-result', toolCallId: 'c' + i, content: [{ type: 'text', text: 'out ' + i }] }],
          source: { kind: 'tool', callId: 'c' + i },
        },
      },
    }))
    for (let k = 0; k < chunksPerTurn; k++) chunk(i, k, 'tail')
  }

  if (opts.withRemappedRefs !== false && plain.length >= 8) {
    // A replacement spanning two surviving events, plus a shadowed range: both carry old
    // seqs that must be remapped onto the new, contiguous numbering.
    push({
      type: 'user/message',
      surfaceOp: { op: 'replace', start: plain[0], end: plain[1] },
      sourceEventSeqs: [plain[0], plain[1]],
      data: { message: { id: 'repl', role: 'user', content: [{ type: 'text', text: 'replacement' }], source: { kind: 'plugin' } } },
    })
    push({
      type: 'tool/result',
      surfaceOp: 'append',
      data: {
        message: { id: 'rr', role: 'user', content: [{ type: 'tool-result', toolCallId: 'cc', content: [] }], source: { kind: 'tool', callId: 'cc' } },
        shadowedRange: { start: plain[2], end: plain[3] },
      },
    })
  }

  return { rows, chunkRows, events, header }
}

/** Write rows as a multi-frame zstd log, self-checking the fixture first. */
function writeLog(file, rows, mtimeMs) {
  const lines = rows.map((r) => JSON.stringify(r))
  const buf = packFrames(lines)
  // Fixture self-check: a magic byte that accidentally appeared inside compressed payload
  // would silently change what the reader sees. Fail loudly here instead.
  const expectedFrames = 1 + Math.ceil((lines.length - 1) / FRAME_LINES)
  assert.equal(frameStarts(buf).length, expectedFrames, 'fixture frame count drifted (magic collision?)')
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, buf)
  if (mtimeMs !== undefined) fs.utimesSync(file, mtimeMs / 1000, mtimeMs / 1000)
  return file
}

let caseSeq = 0
/** A fresh, isolated case directory: nothing here ever lands in $DSH_HOME/sessions. */
function caseDir(name) {
  caseSeq++
  const dir = path.join(tmp, 'cases', caseSeq + '-' + name)
  fs.mkdirSync(dir, { recursive: true })
  return dir
}

function readLines(file) {
  return unpackFrames(fs.readFileSync(file)).split('\n').filter((l) => l.length > 0)
}

function parseRows(file) {
  return readLines(file).map((l) => JSON.parse(l))
}

// Baseline config for the cases that must get past the guards. liveWindowMs: 0 makes every
// file "not live"; the thresholds are lowered so a small synthetic log qualifies.
//
// ★ dryRun: true is pinned HERE on purpose. DEFAULTS.dryRun is false (the plugin ships in
//   "actually do the work" mode), and the two guard cases below assert 'would-vacuum' -- a
//   status that only exists when dryRun is on. Reading the default instead of pinning it made
//   those two cases fail while looking like a logic bug. Pin the mode, assert the mode.
// ★ preBoot: true here models the EXTERNAL runner (lib/clean-cli.mjs). The default config now
//   refuses to rewrite any log while DSH is alive, because a live DSH holds a sequence counter
//   for the session it has loaded and a renumbered file desyncs it (measured 2026-10-18).
//   Tests below exercise the rewriting machinery, so they must claim to be that external run;
//   the gate itself has its own two cases further down.
const CFG = { ...plugin.DEFAULTS, preBoot: true, liveWindowMs: 0, minChunkRows: 20, minRatio: 0.2, dryRun: true }

const isChunk = (type) => plugin.isChunkRow({ type, data: {} })

// ---------------------------------------------------------------- chunk rows

await test('isChunkRow accepts all four fragment row types', async () => {
  for (const type of ['assistant/chunk', 'text-chunks', 'reasoning-chunks', 'tool-call-chunks']) {
    assert.equal(isChunk(type), true, type + ' must be a chunk row')
  }
})

await test("isChunkRow rejects 'chunk-stub' and every other near miss", async () => {
  // A chunk-stub row is a real DSH row that is NOT a streaming fragment: deleting it would
  // destroy content, so the detection set must stay exact rather than pattern-matched.
  for (const type of ['chunk-stub', 'chunk', 'chunks', 'assistant/chunks', 'assistant/message', 'tool/result', 'user/message', 'session', 'text-chunk']) {
    assert.equal(isChunk(type), false, type + ' must NOT be a chunk row')
  }
  assert.equal(plugin.isChunkRow(null), false)
  assert.equal(plugin.isChunkRow(undefined), false)
  assert.equal(plugin.isChunkRow('text-chunks'), false, 'a bare string is not a row')
  assert.equal(plugin.isChunkRow(['text-chunks']), false, 'an array is not a row')
  assert.equal(plugin.CHUNK_TYPES.size, 4, 'the fragment set must stay these four types')
})

// ---------------------------------------------------------------- thresholds

await test('below-threshold: too few chunk rows is left alone', async () => {
  const dir = caseDir('few-chunks')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows({ turns: 2, chunksPerTurn: 2 })   // 8 chunk rows
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, minChunkRows: 2000 })
  assert.equal(r.status, 'below-threshold')
  assert.equal(r.chunks, 8)
  assert.deepEqual(fs.readFileSync(file), before, 'the file must not have been touched')
  assert.equal(fs.existsSync(file + '.orig'), false, 'no backup may be created for a skipped file')
})

await test('below-threshold: a chunk ratio under minRatio is left alone', async () => {
  const dir = caseDir('low-ratio')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, chunkRows } = buildRows({ turns: 40, chunksPerTurn: 1 })  // 80 chunk / 160 events
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const before = fs.readFileSync(file)

  // Enough rows, but the fragments are only a third of the file: not worth rewriting.
  const r = plugin.vacuumOne(file, { ...CFG, minChunkRows: 10, minRatio: 0.9 })
  assert.equal(r.status, 'below-threshold')
  assert.equal(r.chunks, chunkRows)
  assert.ok(r.ratio < 0.9, 'the reported ratio must be the real one, got ' + r.ratio)
  assert.deepEqual(fs.readFileSync(file), before)

  // Control: the same file DOES qualify once the ratio floor is reachable.
  const ok = plugin.vacuumOne(file, { ...CFG, minChunkRows: 10, minRatio: 0.2 })
  assert.equal(ok.status, 'would-vacuum')
})

await test('skipped-live: a session modified inside liveWindowMs is left alone', async () => {
  const dir = caseDir('live')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows()
  writeLog(file, rows, Date.now() - 1000)   // written a second ago
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...plugin.DEFAULTS, preBoot: true, minChunkRows: 20, minRatio: 0.2, dryRun: false })
  assert.equal(r.status, 'skipped-live')
  assert.deepEqual(fs.readFileSync(file), before, 'a live session must never be rewritten')
  assert.equal(fs.existsSync(file + '.orig'), false)
})

await test('skipped-live: the guard is the mtime window, not the file name', async () => {
  const dir = caseDir('live-boundary')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows()
  const old = Date.now() - 60 * 60 * 1000
  writeLog(file, rows, old)

  // Same file, same mtime: only the window differs, so this is the control for the case above.
  assert.equal(plugin.vacuumOne(file, { ...CFG, liveWindowMs: 30 * 60 * 1000 }).status, 'would-vacuum')
  assert.equal(plugin.vacuumOne(file, { ...CFG, liveWindowMs: 2 * 60 * 60 * 1000 }).status, 'skipped-live')
})

// ---------------------------------------------------------------- the .orig guard

// NOTE -- reported bug, deliberately NOT fixed here: lib/index.js contains
//   if (fs.existsSync(file + '.orig') && cfg.skipIfBackedUp !== false) {
//     // already vacuumed ...
//   }
// with an EMPTY body, so the "already backed up, skip it" status does not exist: a session
// is re-processed on every run even when .orig is present. What this case pins is the part
// that is implemented and that matters for safety -- the original backup is never clobbered.
await test('an existing .orig is never overwritten, and the run does not silently skip', async () => {
  const dir = caseDir('orig-exists')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, chunkRows } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const original = fs.readFileSync(file)
  const sentinel = Buffer.from('ORIGINAL-BACKUP-SENTINEL\n', 'utf8')
  fs.writeFileSync(file + '.orig', sentinel)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'vacuumed')
  assert.equal(r.chunks, chunkRows)
  assert.deepEqual(fs.readFileSync(file + '.orig'), sentinel, 'the first backup must survive untouched')
  assert.ok(fs.statSync(file).size < original.length, 'the log itself must still have been vacuumed')
  assert.equal(fs.existsSync(file + '.vacuum-tmp'), false, 'no temp file may be left behind')
})

// ---------------------------------------------------------------- the prune

await test('pruneRows keeps the header and every non-chunk event, with contiguous seq', async () => {
  const { rows, chunkRows, events, header } = buildRows()
  const pruned = plugin.pruneRows(JSON.parse(JSON.stringify(rows)))

  assert.equal(pruned.stats.chunkRows, chunkRows, 'every fragment row must be counted')
  assert.deepEqual(pruned.header, header, 'the session header must be returned verbatim')
  assert.equal(pruned.events.length, events.length, 'exactly the non-chunk events must survive')

  // Order preserved, seq renumbered 0..n-1 with no gaps and no duplicates.
  const oldSeqs = events.map((e) => e.seq)
  assert.deepEqual(pruned.events.map((e) => e.seq), oldSeqs.map((_, i) => i))
  assert.equal(new Set(pruned.events.map((e) => e.seq)).size, pruned.events.length)
  for (const ev of pruned.events) assert.notEqual(ev.type, 'session', 'the header must not be duplicated into the body')

  // Every surviving event is one of the original events, identified by its id.
  const idsOf = (list) => list.map((e) => (e.data && e.data.message ? e.data.message.id : '?')).join(',')
  assert.equal(idsOf(pruned.events), idsOf(events), 'surviving events must keep their order and identity')
})

await test('pruneRows remaps cross-event references onto the new seqs', async () => {
  const { rows, events } = buildRows()
  const pruned = plugin.pruneRows(JSON.parse(JSON.stringify(rows)))
  const byId = new Map(pruned.events.map((e) => [(e.data && e.data.message ? e.data.message.id : '?'), e]))

  const asst = byId.get('a0')
  assert.ok(Array.isArray(asst.sourceEventSeqs), 'sourceEventSeqs must survive the prune')
  assert.equal(asst.sourceEventSeqs.length, 1)
  assert.equal(asst.sourceEventSeqs[0], byId.get('u0').seq, 'a citation must point at the new seq of u0')
  assert.ok(asst.sourceEventSeqs[0] < asst.seq, 'a citation must still point backwards')

  const repl = byId.get('repl')
  assert.deepEqual(repl.surfaceOp, { op: 'replace', start: byId.get('u0').seq, end: byId.get('a0').seq })

  const shadowed = byId.get('rr').data.shadowedRange
  assert.deepEqual(shadowed, { start: byId.get('r0').seq, end: byId.get('u1').seq })

  // And the whole thing must satisfy the loader contract.
  assert.deepEqual(plugin.verifyPruned(pruned), [])
})

// ---------------------------------------------------------------- verify gate

await test('a verify failure means no file is written at all', async () => {
  const dir = caseDir('verify-fail')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows()
  // Attach sourceEventSeqs to a NON-surface event: dropIllegal() keeps it, and
  // verifyPruned() rejects it. So the prune "succeeds" and the write must not.
  rows.splice(3, 0, { type: 'session/note', seq: 999, sourceEventSeqs: [0], data: { note: 'not a surface event' } })
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'verify-failed', 'expected a verify failure, got ' + JSON.stringify(r))
  assert.ok(Array.isArray(r.problems) && r.problems.length > 0, 'the problems must be reported')
  assert.deepEqual(fs.readFileSync(file), before, 'a failed verification must leave the log untouched')
  assert.equal(fs.existsSync(file + '.orig'), false, 'no backup may be written before verification passes')
  assert.equal(fs.existsSync(file + '.vacuum-tmp'), false)
})

// ---------------------------------------------------------------- dry run / real run

await test('dryRun reports would-vacuum and writes nothing', async () => {
  const dir = caseDir('dry-run')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, chunkRows, events } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: true })
  assert.equal(r.status, 'would-vacuum')
  assert.equal(r.chunks, chunkRows)
  assert.equal(r.kept, events.length)
  assert.ok(r.afterMB < r.beforeMB, 'a dry run must still report the saving it would make')
  assert.deepEqual(fs.readFileSync(file), before, 'dryRun must not touch the log')
  assert.equal(fs.existsSync(file + '.orig'), false)
  assert.equal(fs.existsSync(file + '.vacuum-tmp'), false)
})

await test('a real run replaces the log and leaves the original as .orig', async () => {
  const dir = caseDir('real-run')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, chunkRows, events } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'vacuumed')
  assert.equal(r.chunks, chunkRows)
  assert.equal(r.kept, events.length)
  assert.ok(r.afterMB <= r.beforeMB)
  assert.deepEqual(fs.readFileSync(file + '.orig'), before, '.orig must be a byte-exact copy of the original')
  assert.ok(fs.statSync(file).size < before.length, 'the live log must have become smaller')
  assert.equal(fs.existsSync(file + '.vacuum-tmp'), false)
  // The backup is never cleaned up by the plugin; that is the documented contract.
  assert.ok(fs.existsSync(file + '.orig'))
})

await test('the rewritten log is still readable: every frame decompresses and every line parses', async () => {
  const dir = caseDir('reread')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, events, header } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  assert.equal(plugin.vacuumOne(file, { ...CFG, dryRun: false }).status, 'vacuumed')

  const lines = readLines(file)   // throws if any zstd frame is broken
  assert.equal(lines.length, events.length + 1, 'header plus every non-chunk event')
  const parsed = lines.map((l) => JSON.parse(l))   // throws on a torn line
  assert.deepEqual(parsed[0], header, 'the header must stay first and unchanged')
  assert.deepEqual(parsed.slice(1).map((e) => e.seq), events.map((_, i) => i))
  for (const row of parsed) {
    assert.equal(plugin.isChunkRow(row), false, 'no fragment row may survive in the rewritten log')
  }
  // And the plugin's own reader agrees with the suite's independent reader.
  assert.equal(plugin.decompressFrames(fs.readFileSync(file)).text, lines.join('\n') + '\n')
})

await test('the rewritten log keeps every non-chunk event and drops every fragment row', async () => {
  const dir = caseDir('content')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, events, chunkRows } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  assert.equal(plugin.vacuumOne(file, { ...CFG, dryRun: false }).status, 'vacuumed')

  const kept = parseRows(file).slice(1)
  assert.equal(kept.length, events.length)
  assert.equal(events.length + chunkRows + 1, rows.length)
  const idsOf = (list) => list.map((e) => (e.data && e.data.message ? e.data.message.id : '?')).join(',')
  assert.equal(idsOf(kept), idsOf(events), 'the surviving rows must be exactly the original events, in order')
})

// ---------------------------------------------------------------- bad input

await test('a file that is not a zstd frame stream is reported, not rewritten', async () => {
  const dir = caseDir('not-zstd')
  const file = path.join(dir, 'session.jsonl.zstd')
  fs.writeFileSync(file, 'this is not a zstd stream\n')
  // Backdate: the live guard must not be what this case is measuring.
  fs.utimesSync(file, (Date.now() - 3600000) / 1000, (Date.now() - 3600000) / 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'unreadable')
  assert.ok(typeof r.error === 'string' && r.error.length > 0)
  assert.deepEqual(fs.readFileSync(file), before)
})

await test('a log with a corrupt JSON line is reported unparsable, not rewritten', async () => {
  const dir = caseDir('bad-json')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows()
  const lines = rows.map((r) => JSON.stringify(r))
  lines.splice(5, 0, '{"seq": broken json')
  fs.writeFileSync(file, packFrames(lines))
  fs.utimesSync(file, (Date.now() - 3600000) / 1000, (Date.now() - 3600000) / 1000)
  const before = fs.readFileSync(file)

  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'unparsable')
  assert.deepEqual(fs.readFileSync(file), before, 'a torn log must never be rewritten from a partial parse')
  assert.equal(fs.existsSync(file + '.orig'), false)
})

// ---------------------------------------------------------------- discovery / startup

await test('listSessionLogs finds session.jsonl.zstd recursively and nothing else', async () => {
  const root = path.join(caseDir('discover'), 'sessions')
  const { rows } = buildRows({ turns: 1, chunksPerTurn: 1 })
  const a = path.join(root, 'aaaa', 'session.jsonl.zstd')
  const b = path.join(root, 'bbbb', 'nested', 'session.jsonl.zstd')
  writeLog(a, rows)
  writeLog(b, rows)
  fs.writeFileSync(path.join(root, 'bbbb', 'notes.jsonl'), '{}\n')
  fs.writeFileSync(path.join(root, 'session.jsonl.zstd.orig'), 'backup')
  fs.mkdirSync(path.join(root, 'empty'), { recursive: true })

  const found = plugin.listSessionLogs(root).map((f) => path.relative(root, f)).sort()
  assert.deepEqual(found, [path.join('aaaa', 'session.jsonl.zstd'), path.join('bbbb', 'nested', 'session.jsonl.zstd')])
  assert.deepEqual(plugin.listSessionLogs(path.join(root, 'does-not-exist')), [], 'a missing root must not throw')
})

await test('runVacuum scans $DSH_HOME/sessions, honours maxFilesPerRun and logs each file', async () => {
  const root = path.join(tmp, 'sessions')
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  const logFile = path.join(tmp, 'logs', 'session-vacuum.jsonl')
  const { rows } = buildRows()
  const files = ['s1', 's2', 's3'].map((n) => writeLog(path.join(root, n, 'session.jsonl.zstd'), rows, Date.now() - 3600000))
  fs.rmSync(logFile, { force: true })

  const writeConfig = (extra) => fs.writeFileSync(configFile, JSON.stringify({
    enabled: true,
    startDelayMs: 0,
    minIntervalMs: 0,
    minChunkRows: 20,
    minRatio: 0.2,
    liveWindowMs: 0,
    maxFilesPerRun: 12,
    dryRun: true,
    // This block models the external runner (clean-cli.mjs): DSH is stopped, writes are allowed.
    // The in-process refusal has its own cases elsewhere in this file.
    preBoot: true,
    ...extra,
  }))

  // dryRun, one file at a time.
  writeConfig({ maxFilesPerRun: 1 })
  await plugin.runVacuum()
  let logged = fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
  assert.deepEqual(logged.map((l) => l.mark), ['run-start', 'file', 'run-end'])
  assert.equal(logged[1].status, 'would-vacuum')
  assert.equal(logged[0].dryRun, true)
  assert.equal(logged[2].handled, 1)

  // And the cap really is a cap.
  fs.rmSync(logFile, { force: true })
  await plugin.runVacuum()
  logged = fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
  assert.equal(logged.filter((l) => l.mark === 'file').length, 1)

  // With the cap lifted, all three are seen -- and dryRun still wrote nothing.
  fs.rmSync(logFile, { force: true })
  writeConfig({ maxFilesPerRun: 12, dryRun: false })
  await plugin.runVacuum()
  logged = fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
  assert.equal(logged.filter((l) => l.mark === 'file').length, 3)
  assert.ok(logged.filter((l) => l.mark === 'file').every((l) => l.status === 'vacuumed'))
  for (const file of files) {
    assert.ok(fs.existsSync(file + '.orig'), 'a real run must have backed each file up')
    const rows = parseRows(file)
    assert.equal(rows.length, 39, 'header plus the 38 surviving events')
    assert.ok(rows.every((row) => plugin.isChunkRow(row) === false), 'no fragment row may survive')
  }
  assert.equal(fs.existsSync(path.join(tmp, 'sessions', 's1', 'session.jsonl.zstd.vacuum-tmp')), false)
})

// ---------------------------------------------------------------- on-demand cleanup

// One directory holding all three outcomes at once: a file that qualifies, a file DSH may
// be writing right now, and a file under the thresholds. These three cases are exactly what
// used to be invisible -- `run-end` said `handled: 1` and nothing else, so a user watching a
// log with 58 files in it could not tell a working plugin from a no-op.
const ON_DEMAND = {
  enabled: true,
  startDelayMs: 60000,
  minIntervalMs: 0,
  minChunkRows: 20,
  minRatio: 0.2,
  liveWindowMs: 600000,
  maxFilesPerRun: 12,
  dryRun: false,
  // Models the EXTERNAL runner. The in-process refusal is covered by its own case below;
  // these cases are about the mtime guard and the report counters.
  preBoot: true,
  logFile: 'logs/session-vacuum.jsonl',
}
const ON_DEMAND_ROOT = path.join(tmp, 'sessions')
const onDemandConfig = path.join(tmp, 'dsh-session-vacuum', 'config.json')
const onDemandLog = path.join(tmp, 'logs', 'session-vacuum.jsonl')
let staleFile = ''
let liveFile = ''
let belowFile = ''
let thirdFile = ''

// The logs these cases scan live directly under $DSH_HOME/sessions (that is the only
// directory the entry points look at), so the shared directory is rebuilt for each case:
// the two cases above left their own -- much larger -- logs there.
function buildOnDemandDir() {
  fs.rmSync(path.join(tmp, 'sessions'), { recursive: true, force: true })
  const rows = (opts) => buildRows(opts).rows
  staleFile = writeLog(path.join(ON_DEMAND_ROOT, 'stale', 'session.jsonl.zstd'), rows(), Date.now() - 3600000)
  liveFile = writeLog(path.join(ON_DEMAND_ROOT, 'live', 'session.jsonl.zstd'), rows(), Date.now() + 120000)
  belowFile = writeLog(path.join(ON_DEMAND_ROOT, 'below', 'session.jsonl.zstd'), rows({ turns: 2, chunksPerTurn: 2 }), Date.now() - 3600000)
  // A fourth, qualifying log that the file cap leaves untouched. It is made the largest so
  // that which file the cap keeps is deterministic: runVacuum() handles the biggest first.
  thirdFile = writeLog(path.join(ON_DEMAND_ROOT, 'third', 'session.jsonl.zstd'), rows({ turns: 30, chunksPerTurn: 2 }), Date.now() - 3600000)
  return { staleFile, liveFile, belowFile, thirdFile }
}
function writeOnDemandConfig(extra) {
  fs.mkdirSync(path.dirname(onDemandConfig), { recursive: true })
  fs.writeFileSync(onDemandConfig, JSON.stringify({ ...ON_DEMAND, ...extra }))
}
function onDemandLogLines() {
  return fs.readFileSync(onDemandLog, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))
}

await test('run-end counts every skip by reason instead of reporting only `handled`', async () => {
  buildOnDemandDir()
  writeOnDemandConfig({})
  fs.rmSync(onDemandLog, { force: true })

  const results = await plugin.runVacuum(undefined, { fileLimit: 12, trigger: 'tool' })
  assert.equal(Array.isArray(results), true, 'runVacuum must hand back its per-file results')

  const end = onDemandLogLines().find((l) => l.mark === 'run-end')
  assert.ok(end !== undefined, 'a run-end line must be written')
  assert.equal(end.scanned, 4, 'all four logs must have been looked at')
  assert.equal(end.skippedLive, 1, 'the file written in the future must be counted live, got ' + end.skippedLive)
  assert.equal(end.belowThreshold, 1, 'the small log must be counted below-threshold, got ' + end.belowThreshold)
  assert.equal(end.unreadable, 0)
  assert.equal(end.other, 0, 'nothing here fails for another reason')
  assert.equal(end.vacuumed, 2)
  // handled keeps the meaning it always had: the files this run actually acted on.
  assert.equal(end.handled, 2, 'handled must stay "files this run acted on", got ' + end.handled)
  assert.equal(end.handled, end.vacuumed + end.wouldVacuum + end.unreadable + end.other)

  // The counters are the only thing that changed. The pass itself still cleaned the
  // qualifying logs and still refused the live one.
  assert.equal(fs.existsSync(liveFile + '.orig'), false, 'a live log may never be backed up')
  assert.equal(fs.existsSync(belowFile + '.orig'), false, 'a below-threshold log may never be backed up')
  assert.ok(fs.existsSync(staleFile + '.orig'), 'a cleaned log must keep its .orig')
  assert.equal(plugin.vacuumOne(liveFile, plugin.readConfig()).status, 'skipped-live',
    'the live log must still be live after the pass')
})

// The tool's whole point: a report a human can read, naming each file and its reason.
await test('clean_session_logs returns a text report naming each clean and each skip reason', async () => {
  buildOnDemandDir()
  writeOnDemandConfig({ dryRun: false })
  const result = await plugin.runCleanSessionLogs({ dryRun: true })
  assert.equal(typeof result, 'string', 'the report must be plain text, not JSON')

  assert.match(result, /DRY RUN/)
  assert.match(result, /Scanned 4 session log\(s\)/, 'the report must say how many logs were scanned')
  assert.match(result, /Would clean: 2 log\(s\)/)
  assert.match(result, /stale[\\/]session\.jsonl\.zstd/, 'a cleaned log must be named')
  assert.match(result, /MB -> /, 'the report must give before -> after in MB')
  assert.match(result, /skippedLive: 1/, 'the live skip must be broken out by reason')
  assert.match(result, /belowThreshold: 1/, 'the threshold skip must be broken out by reason')
  assert.match(result, /live[\\/]session\.jsonl\.zstd/, 'the skipped log must be named too')
  assert.match(result, /modified inside liveWindowMs/, 'and its reason spelled out')
  assert.match(result, /not enough streaming fragments/, 'and so must the threshold skip')
  assert.match(result, /fragment row\(s\) dropped/)
  // The report must not silently be empty of detail: run-end counters are echoed for the log.
  assert.match(result, /run-end counters .*scanned=4/)
})

await test('the tool report states that the caller session cannot be cleaned from inside DSH', async () => {
  const result = await plugin.runCleanSessionLogs({ dryRun: true })
  assert.match(result, /session you are running in cannot be cleaned from inside a running DSH/i)
  assert.match(result, /corrupt/, 'the report must say why (rewriting it can corrupt it)')
  assert.match(result, /stop DSH/i, 'and must say what to do instead')
  assert.match(result, /清理会话日志\.cmd/, 'naming the offline script to run after stopping DSH')
  assert.match(result, /liveWindowMs/, 'and the idle window that lets a later boot do it')
})

await test('dryRun: true through the tool writes nothing at all', async () => {
  buildOnDemandDir()
  writeOnDemandConfig({ dryRun: false })
  const files = [staleFile, liveFile, belowFile, thirdFile]
  const before = files.map((f) => fs.readFileSync(f))
  const mtimes = files.map((f) => fs.statSync(f).mtimeMs)
  fs.rmSync(onDemandLog, { force: true })

  // dryRun comes only from the argument here: the config says dryRun: false, so a tool that
  // ignored its argument (or fell through to the config) would really rewrite these logs.
  const result = await plugin.runCleanSessionLogs({ dryRun: true })
  assert.match(result, /DRY RUN/, 'the report must say this was a preview')
  assert.match(result, /Would clean: \d+ log\(s\)/, 'a preview of a real pass must still report candidates')

  for (let i = 0; i < files.length; i++) {
    assert.deepEqual(fs.readFileSync(files[i]), before[i], 'dryRun must not change one byte of ' + files[i])
    assert.equal(fs.statSync(files[i]).mtimeMs, mtimes[i], 'dryRun must not even touch ' + files[i])
    assert.equal(fs.existsSync(files[i] + '.orig'), false, 'dryRun must not back anything up')
    assert.equal(fs.existsSync(files[i] + '.vacuum-tmp'), false, 'dryRun must not leave a temp file')
  }
  // Nothing was replaced and nothing was backed up, so no status may claim it was.
  const statuses = onDemandLogLines().filter((l) => l.mark === 'file').map((l) => l.status)
  assert.equal(statuses.includes('vacuumed'), false, 'a dry run must never report vacuumed, got ' + statuses.join(','))
  assert.equal(statuses.includes('would-vacuum'), true, 'a dry run must report would-vacuum')
})

await test('a real pass through the tool cleans what it reports it cleaned', async () => {
  buildOnDemandDir()
  writeOnDemandConfig({ dryRun: false })
  const liveBefore = fs.readFileSync(liveFile)
  const belowBefore = fs.readFileSync(belowFile)
  const thirdBefore = fs.readFileSync(thirdFile)
  const staleBefore = fs.readFileSync(staleFile)

  const result = await plugin.runCleanSessionLogs({})
  assert.doesNotMatch(result, /DRY RUN/, 'a real pass must not claim to be a preview')
  assert.match(result, /Cleaned: 2 log\(s\)/)
  assert.match(result, /stale[\\/]session\.jsonl\.zstd/)
  assert.match(result, /third[\\/]session\.jsonl\.zstd/)

  // What the report claims is what happened, file by file.
  assert.deepEqual(fs.readFileSync(staleFile + '.orig'), staleBefore, '.orig must be the pre-vacuum bytes')
  assert.deepEqual(fs.readFileSync(thirdFile + '.orig'), thirdBefore)
  assert.ok(fs.statSync(staleFile).size < staleBefore.length, 'the cleaned log must have become smaller')
  assert.ok(fs.statSync(thirdFile).size < thirdBefore.length)
  assert.deepEqual(fs.readFileSync(liveFile), liveBefore, 'the live log must be byte-identical')
  assert.deepEqual(fs.readFileSync(belowFile), belowBefore, 'the below-threshold log must be byte-identical')
  assert.equal(fs.existsSync(liveFile + '.orig'), false, 'a live log must never be backed up')
  assert.equal(fs.existsSync(belowFile + '.orig'), false, 'a below-threshold log must never be backed up')
  for (const f of [staleFile, thirdFile]) assert.equal(fs.existsSync(f + '.vacuum-tmp'), false)
})

await test('the on-demand entry point cannot skip the live guard', async () => {
  // The pass above wrote both cleaned logs, so their mtimes are now "now". That is the
  // scenario the guard exists for, and it is also the shape the plugin gets wrong if an
  // on-demand path is ever given a bigger hammer: the very next pass must refuse both.
  writeOnDemandConfig({ dryRun: false })
  const cfg = plugin.readConfig()
  assert.ok(cfg.liveWindowMs > 0, 'the shipped default window must stay non-zero')
  assert.ok(plugin.DEFAULTS.liveWindowMs > 0, 'and it must stay the default')

  const before = fs.readFileSync(staleFile)
  const results = await plugin.runVacuum(undefined, { dryRun: false, fileLimit: 12, trigger: 'tool' })
  const forStale = results.filter((r) => r.file === staleFile)
  assert.equal(forStale.length, 1, 'the file must appear in the results')
  assert.equal(forStale[0].status, 'skipped-live',
    'a log written seconds ago must be refused even when an agent asks for it by hand')
  assert.deepEqual(fs.readFileSync(staleFile), before, 'and it must not be rewritten')
})

await test('inside a live DSH, no log is rewritten even when it looks idle', async () => {
  // The 2026-10-18 incident: a session the GUI keeps open looks idle for two minutes, the mtime
  // guard said "safe", the file was renumbered 0..72555, and DSH still held seq 713504 -- so the
  // next append wrote 713504 into a file numbered from zero. Rewriting a log a live DSH has
  // loaded is never safe, no matter how long it has been quiet.
  const { rows } = buildRows()
  const file = path.join(ON_DEMAND_ROOT, 'loaded-by-dsh', 'session.jsonl.zstd')
  writeLog(file, rows, Date.now() - 3600000) // an hour old: mtime guard would let it through
  writeOnDemandConfig({ preBoot: false, allowInProcessWrite: false, liveWindowMs: 0 })
  const before = fs.readFileSync(file)
  const r = plugin.vacuumOne(file, plugin.readConfig())
  assert.equal(r.status, 'skipped-in-process', 'in-process runs must refuse before the mtime check')
  assert.deepEqual(fs.readFileSync(file), before, 'and nothing may be written')
  assert.equal(fs.existsSync(file + '.orig'), false, 'not even a backup')
})

await test('the in-process refusal is a config switch, and turning it on restores the old path', async () => {
  const { rows } = buildRows()
  const file = path.join(ON_DEMAND_ROOT, 'loaded-by-dsh-2', 'session.jsonl.zstd')
  writeLog(file, rows, Date.now() - 3600000)
  writeOnDemandConfig({ preBoot: false, allowInProcessWrite: true, liveWindowMs: 0, dryRun: true })
  const r = plugin.vacuumOne(file, plugin.readConfig())
  assert.equal(r.status, 'would-vacuum', 'an explicit opt-in still reaches the rewriting path')
})

// ---------------------------------------------------------------- frame boundaries

// ---------------------------------------------------------------- the web route

// A tiny fake for the two objects the webServer hands a route handler. The route itself is
// taken from the plugin, so this exercises the real handler rather than a copy of it.
function fakeRequest(method, body) {
  const handlers = {}
  const req = {
    method,
    on(event, fn) {
      handlers[event] = fn
      return req
    },
  }
  // Deliver the body on the next tick, the way a real stream does -- a handler that never
  // listens for 'end' must not be able to pass this case.
  setImmediate(() => {
    if (handlers.data !== undefined && body !== undefined) handlers.data(Buffer.from(body, 'utf8'))
    if (handlers.end !== undefined) handlers.end()
  })
  return req
}
function fakeResponse() {
  const res = {
    code: null,
    headers: null,
    body: '',
    writeHead(code, headers) {
      res.code = code
      res.headers = headers
      return res
    },
    end(text) {
      res.body = text === undefined ? '' : String(text)
    },
  }
  return res
}

await test('the web route is POST-only, preview-only, and reports the same pass', async () => {
  buildOnDemandDir()
  writeOnDemandConfig({ enabled: true, dryRun: true })
  const route = plugin.vacuumRoute({})
  assert.equal(route.path, plugin.ROUTE_PATH)
  assert.equal(route.kind, 'exact')

  // A GET is refused: this route always runs a scan, so it is not a safe method to guess at.
  let res = fakeResponse()
  await route.handler(fakeRequest('GET'), res)
  assert.equal(res.code, 405, 'GET must be refused, got ' + res.code)
  assert.match(res.body, /POST-only/i)

  // A body that asks for a real pass is refused, whatever the config says.
  res = fakeResponse()
  await route.handler(fakeRequest('POST', JSON.stringify({ dryRun: false })), res)
  assert.equal(res.code, 403, 'a non-dry request must be refused, got ' + res.code)
  assert.match(res.body, /preview-only|refusing/i)

  // And with dryRun: false in the config, the route refuses outright rather than exposing a
  // destructive endpoint. (A real pass belongs to the tool, where an agent owns the call.)
  writeOnDemandConfig({ enabled: true, dryRun: false })
  res = fakeResponse()
  await route.handler(fakeRequest('POST', '{}'), res)
  assert.equal(res.code, 403, 'a profile with dryRun:false must not expose a writing route, got ' + res.code)
  assert.match(res.body, /refusing to run a non-dry pass/i)

  // The preview itself: an empty body is a dry run and the reply is the report.
  writeOnDemandConfig({ enabled: true, dryRun: true })
  res = fakeResponse()
  await route.handler(fakeRequest('POST', ''), res)
  assert.equal(res.code, 200, 'an empty-bodied POST must be a preview, got ' + res.code + ' ' + res.body)
  assert.match(res.body, /DRY RUN/)
  assert.match(res.body, /Scanned \d+ session log\(s\)/)
  assert.match(res.body, /skippedLive: \d+/)

  // A disabled plugin must not scan at all.
  writeOnDemandConfig({ enabled: false, dryRun: true })
  res = fakeResponse()
  await route.handler(fakeRequest('POST', ''), res)
  assert.equal(res.code, 403)
  assert.match(res.body, /disabled/i)
})

// REGRESSION (2026-10-11). Frame boundaries used to be found by scanning for the four
// magic bytes 28 b5 2f fd. Those bytes can occur INSIDE a frame's payload, and the mix-up
// was silent: zstdDecompressSync does not throw on a mis-sliced frame, it returns an empty
// buffer, so the cut landed on a clean line boundary, the truncated row set still parsed,
// still passed verifyPruned(), and the live log was replaced by it with status `vacuumed`.
// Reproduced before the fix: 661 rows read back as 200 and rewritten to 38.
await test('a bogus magic sequence stops the walk instead of cutting a real frame in half', async () => {
  const dir = caseDir('stray-magic')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)
  const clean = fs.readFileSync(file)
  const cleanText = plugin.decompressFrames(clean).text

  // Splice a magic-looking region in front of a real frame in the middle of the file: this
  // is exactly the shape a false positive inside a payload used to produce.
  const starts = frameStarts(clean)
  assert.ok(starts.length >= 4, 'fixture needs several frames for this case')
  const at = starts[Math.floor(starts.length / 2)]
  const junk = Buffer.concat([ZSTD_MAGIC, Buffer.alloc(24, 0x5a)])
  const injected = Buffer.concat([clean.subarray(0, at), junk, clean.subarray(at)])

  const got = plugin.decompressFrames(injected)
  assert.equal(got.truncated, true, 'the walk must admit it could not get to the end')
  assert.ok(got.trailingBytes > junk.length, 'everything from the bogus region on is unread')
  assert.ok(got.text.length < cleanText.length, 'it must not pretend the whole file was read')
  assert.equal(cleanText.startsWith(got.text), true, 'what it did read must be a clean prefix')
})

await test('a truncated tail makes vacuumOne refuse instead of rewriting a half-read log', async () => {
  const dir = caseDir('truncated-tail')
  const file = path.join(dir, 'session.jsonl.zstd')
  const { rows, chunkRows } = buildRows()
  writeLog(file, rows, Date.now() - 60 * 60 * 1000)

  // Sanity: this fixture does qualify, so the refusal below is the tail guard talking and
  // not one of the thresholds.
  assert.ok(chunkRows >= CFG.minChunkRows)

  const whole = fs.readFileSync(file)
  // Cut in the middle of the LAST frame, the shape a crash mid-append leaves behind.
  const starts = frameStarts(whole)
  const lastStart = starts[starts.length - 1]
  fs.writeFileSync(file, whole.subarray(0, lastStart + 4))
  // Backdate: writing the file just now would make it "live", and an NTFS mtime can sit a
  // fraction of a millisecond ahead of Date.now(), so liveWindowMs 0 is not enough.
  const past = (Date.now() - 60 * 60 * 1000) / 1000
  fs.utimesSync(file, past, past)
  const after = fs.readFileSync(file)

  assert.equal(plugin.decompressFrames(after).truncated, true)
  const r = plugin.vacuumOne(file, { ...CFG, dryRun: false })
  assert.equal(r.status, 'unreadable-tail')
  assert.ok(r.trailingBytes > 0)
  assert.deepEqual(fs.readFileSync(file), after, 'a half-read log must never be rewritten')
  assert.equal(fs.existsSync(file + '.orig'), false, 'nothing may be backed up for a log we refused')
})

// ---------------------------------------------------------------- hostile contexts

// A plugin that is merely INCOMPATIBLE with the build it was loaded into must not take the
// whole profile down with it. The proven failure was a third-party plugin
// (cancanqiqi/dsh-human-subagent) calling a service method this build does not have: the
// exception left apply() and the loader refused to start the entire profile. The same shape
// of incompatibility must cost THIS plugin its own activation and nothing else.
await test('apply() returns normally when the context is hostile, and records the deactivation', async () => {
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  const logFile = path.join(tmp, 'logs', 'session-vacuum.jsonl')
  // startDelayMs is short on purpose: if a hostile apply() ever left its timer armed, the
  // scan below would show up as a run-start line and the case would fail.
  fs.writeFileSync(configFile, JSON.stringify({ enabled: true, startDelayMs: 300, dryRun: true }))
  fs.rmSync(logFile, { force: true })

  const logged = () => fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))

  const hostile = [
    { label: 'ctx.effect throws', step: 'ctx.effect', ctx: { effect() { throw new Error('boom-effect') } } },
    { label: 'ctx.effect absent', step: 'services', ctx: {} },
    { label: 'ctx is null', step: 'services', ctx: null },
    // The on-demand interface added a second registration to the same funnel. A build whose
    // tools service is missing must cost this plugin its own activation -- exactly like the
    // cases above -- and never the host boot. (ctx.tools.register throwing is covered by the
    // "route that fails to register" case below, because a throwing register must not be
    // retried through the async path.)
    { label: 'ctx.tools absent', step: 'ctx.tools', ctx: { effect: () => () => {} } },
  ]

  let seen = 0
  for (const h of hostile) {
    let threw = null
    try {
      plugin.apply(h.ctx)
    } catch (err) {
      threw = err
    }
    assert.equal(threw, null, 'apply() must NOT throw with a hostile ctx: ' + h.label)
    const lines = logged().filter((l) => l.mark === 'deactivated')
    assert.equal(lines.length, seen + 1, 'each hostile ctx must leave exactly ONE deactivation line: ' + h.label)
    assert.equal(lines[lines.length - 1].reason, 'registration-failed', h.label)
    assert.equal(lines[lines.length - 1].step, h.step, 'the log must name the step that failed: ' + h.label)
    seen = lines.length
  }

  // "Deactivated" has to mean deactivated: no timer may survive, so nothing may run.
  await new Promise((resolve) => setTimeout(resolve, 700))
  assert.deepEqual(logged().filter((l) => l.mark === 'run-start'), [], 'a deactivated plugin must not scan anything')

  // And with ctx.effect present the very same apply() still mounts the timer and hands the
  // host a disposer: the guard is a failure path, not a new condition on the happy path.
  // (defineTool is supplied because a healthy ctx that cannot compile the tool would -- quite
  // correctly -- be a registration failure and deactivate, which is the case above.)
  let mounted = 0
  let disposeTimer = null
  plugin.apply({
    effect(fn) { mounted += 1; disposeTimer = fn(); return () => {} },
    tools: { defineTool: fakeDefineTool([]), register: () => () => {} },
  })
  assert.equal(mounted, 1, 'a healthy ctx must still get the effect registration')
  assert.equal(typeof disposeTimer, 'function', 'the effect must return a cleanup that clears the timer')
  disposeTimer()
  await new Promise((resolve) => setTimeout(resolve, 400))
  assert.equal(logged().filter((l) => l.mark === 'deactivated').length, seen, 'a healthy ctx must not log a deactivation')
  assert.deepEqual(logged().filter((l) => l.mark === 'run-start'), [], 'the mounted timer was cleared, so nothing ran')
})

// A stand-in for the real defineTool: it records the spec it was handed and returns a
// DISPOSER, exactly like the host's registry does. An identity function here would be a
// trap -- the "definition" handed to register would then be the raw object, the disposer
// collector would (correctly) drop it, and the test would look like it registered a tool
// when nothing was registered at all. That mistake cost real debugging time; do not undo it.
function fakeDefineTool(seen) {
  return (def) => {
    seen.push(def)
    return () => {}
  }
}

// The host injects services on demand: a service NOT named in `inject` is not reachable as a
// plain property (reading it throws "cannot get property ... without inject"). `ctx.inject`
// is the optional form -- it calls back only once that service exists. This helper mimics it,
// so the route cases exercise the real shape instead of a context that could never happen.
function ctxWithInject(services) {
  return {
    inject(names, fn) {
      for (const n of names) if (!(n in services)) return
      return fn(services)
    },
  }
}

await test('a route that fails to register costs the route only, not the plugin', async () => {
  assert.equal(plugin.TOOL_NAME, 'clean_session_logs', 'the tool name a model has to pick')
  assert.equal(plugin.ROUTE_PATH, '/api/local/session-vacuum/run')
  assert.deepEqual(plugin.inject, ['tools'], 'the plugin must declare the service it reads')

  const logFile = path.join(tmp, 'logs', 'session-vacuum.jsonl')
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  fs.writeFileSync(configFile, JSON.stringify({ enabled: true, startDelayMs: 60000, dryRun: true }))
  fs.rmSync(logFile, { force: true })
  const logged = () => fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))

  // 1. webServer.register throws. The tool still works; the plugin must NOT deactivate.
  const specs = []
  let routeHandedOver = false
  const webServer = { register: () => { routeHandedOver = true; throw new Error('boom-route') } }
  plugin.apply(Object.assign({
    effect: () => () => {},
    tools: { defineTool: fakeDefineTool(specs), register: () => () => {} },
  }, ctxWithInject({ webServer })))
  assert.equal(specs.length, 1, 'the tool must still be handed to the registry when the route fails')
  const spec = specs[0]
  assert.equal(spec.name, 'clean_session_logs')
  assert.equal(routeHandedOver, true, 'the route registration must have been attempted')
  const routeFailed = logged().filter((l) => l.mark === 'route-failed')
  assert.equal(routeFailed.length, 1, 'a failed route must be reported, not swallowed silently')
  assert.equal(routeFailed[0].path, plugin.ROUTE_PATH)
  assert.equal(logged().filter((l) => l.mark === 'deactivated').length, 0,
    'a route failure must NOT deactivate the plugin')

  // 2. webServer absent (a headless profile): no route, no complaint, same tool.
  const more = []
  plugin.apply({
    effect: () => () => {},
    tools: { defineTool: fakeDefineTool(more), register: () => () => {} },
  })
  assert.equal(more.length, 1, 'a build with no webServer must still get the tool')
  assert.equal(logged().filter((l) => l.mark === 'route-failed').length, 1,
    'no webServer means nothing was attempted, so nothing new failed')
  assert.equal(logged().filter((l) => l.mark === 'deactivated').length, 0,
    'a missing webServer must not deactivate the plugin')

  // 3. The spec the host receives really runs the guarded pass and reports in text.
  writeOnDemandConfig({ enabled: true, dryRun: true })
  const value = await spec.execute({ dryRun: true })
  assert.equal(typeof value.report, 'string', 'the tool must return its report')
  assert.match(value.report, /DRY RUN/)
  assert.match(value.report, /cannot be cleaned from inside a running DSH/i)

  // Optional, no required arguments, and a schema the registry can compile.
  assert.deepEqual(Object.keys(spec.parameters), ['dryRun'])
  assert.equal(spec.parameters.dryRun.type, 'boolean')
  assert.equal(spec.parameters.dryRun.required, undefined, 'dryRun must stay optional')
  assert.equal(spec.output.schema.properties.report.required, true)

  // 4. A tool that cannot be registered deactivates, like every other registration failure,
  //    and apply() still must not throw.
  let threw = null
  try {
    plugin.apply({
      effect: () => () => {},
      tools: { defineTool: fakeDefineTool([]), register() { throw new Error('boom-tools') } },
    })
  } catch (err) {
    threw = err
  }
  assert.equal(threw, null, 'apply() must never throw out of a failed tool registration')
  const after = logged().filter((l) => l.mark === 'deactivated')
  assert.equal(after.length, 1)
  assert.equal(after[0].step, 'ctx.tools')
})

await test('the tool is registered synchronously, and the bare-object form works with no defineTool', async () => {
  const logFile = path.join(tmp, 'logs', 'session-vacuum.jsonl')
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  fs.writeFileSync(configFile, JSON.stringify({ enabled: true, startDelayMs: 60000, dryRun: true }))
  fs.rmSync(logFile, { force: true })
  const logged = () => fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l))

  // Registration must be synchronous: nothing about apply() may depend on a later tick, or a
  // caller looking at the registry right after apply() sees a plugin that "did nothing".
  // (This build's ctx.tools has no defineTool, so this really is the cold path.)
  const specs = []
  plugin.apply({
    effect: () => () => {},
    tools: { register: () => () => {} },
  }, fakeDefineTool(specs))
  assert.equal(specs.length, 1, 'the tool must reach the registry before apply() returns')
  assert.equal(specs[0].name, 'clean_session_logs')
  assert.equal(typeof specs[0].execute, 'function')
  assert.equal(specs[0].parameters.dryRun.type, 'boolean')
  assert.equal(specs[0].parameters.dryRun.required, undefined)
  assert.equal(specs[0].output.schema.properties.report.required, true)

  // With nothing to compile it at all, the bare object must still be usable.
  for (const nothing of [null, undefined]) {
    const bare = plugin.buildTool(nothing)
    assert.equal(bare.name, 'clean_session_logs')
    assert.equal(typeof bare.execute, 'function', 'the bare-object form must be executable')
    assert.equal(bare.parameters.type, 'object')
    assert.equal(bare.parameters.properties.dryRun.type, 'boolean')
    assert.equal(bare.output.schema.properties.report.type, 'string')
    const value = await bare.execute({ dryRun: true })
    assert.equal(typeof value.report, 'string', 'and must really run the pass')
  }
  assert.equal(logged().filter((l) => l.mark === 'deactivated').length, 0,
    'a missing defineTool is a fallback, not a failure')

  // And if even that fails, the plugin deactivates rather than leaving a live timer behind.
  fs.rmSync(logFile, { force: true })
  let mounted = 0
  let threw = null
  try {
    plugin.apply({
      effect: (fn) => { mounted += 1; fn(); return () => {} },
      tools: { register() { throw new Error('boom-tools') } },
    })
  } catch (err) {
    threw = err
  }
  assert.equal(threw, null, 'apply() must never throw out of a failed tool registration')
  const lines = logged().filter((l) => l.mark === 'deactivated')
  assert.equal(mounted, 1, 'the effect must have been registered before the tool attempt')
  assert.equal(lines.length, 1, 'the failure must land on the deactivation path')
  assert.equal(lines[0].step, 'ctx.tools')
  assert.match(lines[0].error, /boom-tools/)

  // The module really is resolvable at load time in this build, which is what lets the tool
  // land synchronously; the fallback resolver must agree with that.
  const resolved = await plugin.loadToolModule()
  assert.equal(resolved.ok, true, 'dsh-tools must resolve for the plugin dir: ' + resolved.error)
  assert.equal(typeof resolved.defineTool, 'function')
})

await test('disposing the plugin disposes both registrations it made', async () => {
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  fs.writeFileSync(configFile, JSON.stringify({ enabled: true, startDelayMs: 60000, dryRun: true }))
  let toolRegistered = 0
  let disposedTool = 0
  let disposedRoute = 0
  let fiberDisposer = null
  plugin.apply({
    effect: (fn) => {
      // The host calls fn() to mount and the returned callback to unmount. Running fn()
      // here is what makes the disposers in lib/index.js reachable at all.
      fiberDisposer = fn()
      return () => {}
    },
    tools: { defineTool: fakeDefineTool([]), register: () => { toolRegistered += 1; return () => { disposedTool++ } } },
    ...ctxWithInject({ webServer: { register: () => () => { disposedRoute++ } } }),
  })
  assert.equal(toolRegistered, 1, 'a healthy ctx must get the tool')
  assert.equal(typeof fiberDisposer, 'function', 'the effect must return a cleanup')
  assert.equal(disposedTool, 0, 'nothing may be disposed before the fiber ends')
  fiberDisposer()
  assert.equal(disposedTool, 1, 'the tool registration must be removed on dispose')
  assert.equal(disposedRoute, 1, 'the route registration must be removed on dispose')
})

// ---------------------------------------------------------------- round-3 additions

await test('readConfig accepts a config.json written with a UTF-8 BOM', async () => {
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  // Notepad and PowerShell's `Set-Content -Encoding UTF8` both write a BOM. Before the
  // fix, JSON.parse threw on the leading U+FEFF and the whole file was silently ignored
  // (measured on lab2: a config asking for liveWindowMs 30000 behaved as 600000).
  fs.writeFileSync(configFile, '\uFEFF' + JSON.stringify({ enabled: true, minChunkRows: 123, liveWindowMs: 4567, dryRun: true }))
  const cfg = plugin.readConfig()
  assert.equal(cfg.minChunkRows, 123, 'a BOM must not throw the whole config away')
  assert.equal(cfg.liveWindowMs, 4567, 'every value in a BOM-ed config must still be read')
  fs.writeFileSync(configFile, JSON.stringify({ enabled: true, startDelayMs: 60000, dryRun: true }))
})

await test('the log stays pure ASCII and carries a readable relative path', async () => {
  const root = path.join(tmp, 'sessions')
  const logFile = path.join(tmp, 'logs', 'session-vacuum.jsonl')
  const configFile = path.join(tmp, 'dsh-session-vacuum', 'config.json')
  const { rows } = buildRows()
  writeLog(path.join(root, 'ascii-check', 'session.jsonl.zstd'), rows, Date.now() - 3600000)
  fs.rmSync(logFile, { force: true })
  fs.writeFileSync(configFile, JSON.stringify({
    enabled: true, startDelayMs: 0, minIntervalMs: 0, minChunkRows: 20, minRatio: 0.2,
    liveWindowMs: 0, maxFilesPerRun: 12, dryRun: true,
    // external-runner mode: this case asserts the log line, not the in-process gate
    preBoot: true,
  }))
  await plugin.runVacuum()

  // A UTF-8 log read back by a GBK consumer (PowerShell 5.1 Get-Content, cmd type on this
  // machine) turned a Chinese path into mojibake. Escaping everything above ASCII removes
  // the whole class: no decoder can change a byte it never has to interpret.
  const bytes = fs.readFileSync(logFile)
  assert.ok([...bytes].every((b) => b < 0x80), 'the log file must contain no non-ASCII byte')
  const fileLine = fs.readFileSync(logFile, 'utf8').trim().split('\n').map((l) => JSON.parse(l))
    .find((l) => typeof l.rel === 'string' && l.rel.endsWith('ascii-check/session.jsonl.zstd'))
  assert.ok(fileLine, 'the pass must log the file it handled')
  assert.equal(fileLine.rel, 'sessions/ascii-check/session.jsonl.zstd', 'and it must carry an ASCII path relative to $DSH_HOME')

  const escaped = plugin.asciiJson({ p: 'E:\\' + '\u5b9e\u673a\u6d4b\u8bd5' + '\\home' })
  assert.ok([...Buffer.from(escaped)].every((b) => b < 0x80), 'asciiJson must escape every non-ASCII character')
  assert.equal(JSON.parse(escaped).p, 'E:\\' + '\u5b9e\u673a\u6d4b\u8bd5' + '\\home', 'and JSON.parse must give the real path back')
})

// ---------------------------------------------------------------- teardown

fs.rmSync(tmp, { recursive: true, force: true })
console.log('\n' + passed + ' passed, ' + failed + ' failed')
process.exit(failed === 0 ? 0 : 1)
