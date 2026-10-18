#!/usr/bin/env node
// clean-cli.mjs -- run the session-log vacuum when DSH is NOT running.
//
// Why a separate entry point: rewriting a session log that a LIVE DSH has loaded desyncs the
// in-memory sequence counter. Measured 2026-10-18: the file was renumbered 0..72555 while DSH
// still held 713504, so the next append wrote 713504 into a file numbered from zero. The mtime
// guard cannot see this -- a session the GUI keeps open but idle simply does not get modified,
// which looks exactly like "safe".
//
// So the plugin refuses to write from inside DSH by default, and this CLI is the one path that
// is allowed to: it sets preBoot, i.e. "no DSH is running".
//
//   node lib/clean-cli.mjs                       # uses $DSH_HOME or ~/.dsh
//   node lib/clean-cli.mjs --home C:\path\to\.dsh
//   node lib/clean-cli.mjs --dry-run              # report only
//   node lib/clean-cli.mjs --file-limit 24
//
// Exit codes: 0 ok, 1 bad arguments, 2 something failed inside the run.
//
// ASCII only on purpose -- this box runs a GBK code page.

import path from 'node:path'
import os from 'node:os'
import { runVacuum } from './index.js'

function parseArgs(argv) {
    const out = { home: process.env.DSH_HOME || path.join(os.homedir(), '.dsh'), dryRun: false, fileLimit: undefined }
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i]
        if (a === '--home') { out.home = argv[++i] }
        else if (a === '--dry-run' || a === '--dryRun') { out.dryRun = true }
        else if (a === '--file-limit') { const n = Number(argv[++i]); if (Number.isFinite(n) && n > 0) out.fileLimit = n }
        else if (a === '--help' || a === '-h') { printHelp(); process.exit(0) }
        else { console.error('unknown argument: ' + a); printHelp(); process.exit(1) }
    }
    return out
}

function printHelp() {
    console.log([
        'node lib/clean-cli.mjs [--home <DSH_HOME>] [--dry-run] [--file-limit <n>]',
        '',
        'Run this with DSH STOPPED. It is the only supported way to actually rewrite logs;',
        'the in-process plugin only reports.',
    ].join('\n'))
}

const args = parseArgs(process.argv.slice(2))

// DSH resolves DSH_HOME from the environment; do the same before anything touches the disk.
process.env.DSH_HOME = args.home

console.log('session-vacuum (external run)')
console.log('DSH_HOME : ' + args.home)
console.log('mode     : ' + (args.dryRun ? 'dry-run (nothing written)' : 'write'))
console.log('')

let results
try {
    results = await runVacuum(undefined, {
        dryRun: args.dryRun,
        fileLimit: args.fileLimit,
        trigger: 'cli',
        // ★ the whole point of this entry point
        preBoot: true,
        allowInProcessWrite: true,
    })
} catch (err) {
    console.error('run failed: ' + (err && err.message ? err.message : err))
    process.exit(2)
}

const by = new Map()
for (const r of results || []) {
    const k = r.status || 'unknown'
    by.set(k, (by.get(k) || 0) + 1)
}

console.log('files scanned: ' + (results ? results.length : 0))
for (const [status, n] of [...by.entries()].sort()) {
    console.log('  ' + status.padEnd(22) + n)
}

const wrote = (results || []).filter((r) => r.status === 'vacuumed').length
if (args.dryRun) {
    console.log('')
    console.log('dry run: ' + wrote + ' file(s) WOULD be rewritten')
} else {
    console.log('')
    console.log('rewritten: ' + wrote + ' file(s)')
}
process.exit(0)
