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
const CFG = { ...plugin.DEFAULTS, liveWindowMs: 0, minChunkRows: 20, minRatio: 0.2 }

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

  const r = plugin.vacuumOne(file, { ...plugin.DEFAULTS, minChunkRows: 20, minRatio: 0.2, dryRun: false })
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

// ---------------------------------------------------------------- frame boundaries

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

// ---------------------------------------------------------------- teardown

fs.rmSync(tmp, { recursive: true, force: true })
console.log('\n' + passed + ' passed, ' + failed + ' failed')
process.exit(failed === 0 ? 0 : 1)
