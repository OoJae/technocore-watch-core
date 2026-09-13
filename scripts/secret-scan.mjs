#!/usr/bin/env node
// Refuses any file that contains a 64-hex-character run (the shape of a Technocore
// signing seed) unless the exact value is allow-listed as a public TEST vector.
//
//   node scripts/secret-scan.mjs            scan tracked + untracked-not-ignored files
//   node scripts/secret-scan.mjs --staged   scan the staged index (pre-commit hook)
//
// Allow-list entries live in scripts/secret-allowlist.txt, written as the two 32-char
// halves separated by a space so the allow-list itself never contains a 64-hex run.
import { execFileSync } from 'node:child_process';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const staged = process.argv.includes('--staged');
const HEX64 = /(?<![0-9a-fA-F])[0-9a-fA-F]{64}(?![0-9a-fA-F])/g;

function allowlist() {
  const file = join(root, 'scripts', 'secret-allowlist.txt');
  const allowed = new Set();
  if (!existsSync(file)) return allowed;
  for (const raw of readFileSync(file, 'utf8').split('\n')) {
    const line = raw.replace(/#.*$/, '').trim();
    if (!line) continue;
    const [a, b] = line.split(/\s+/);
    if (/^[0-9a-f]{32}$/i.test(a ?? '') && /^[0-9a-f]{32}$/i.test(b ?? '')) {
      allowed.add((a + b).toLowerCase());
    }
  }
  return allowed;
}

function git(args) {
  return execFileSync('git', args, { cwd: root, encoding: 'utf8', maxBuffer: 64 << 20 });
}

function files() {
  if (staged) {
    return git(['diff', '--cached', '--name-only', '--diff-filter=ACMR', '-z']).split('\0').filter(Boolean);
  }
  const tracked = git(['ls-files', '-z']).split('\0');
  const untracked = git(['ls-files', '--others', '--exclude-standard', '-z']).split('\0');
  return [...new Set([...tracked, ...untracked])].filter(Boolean);
}

function content(path) {
  if (staged) {
    return execFileSync('git', ['show', `:${path}`], { cwd: root, maxBuffer: 64 << 20 });
  }
  const abs = join(root, path);
  if (!existsSync(abs) || !statSync(abs).isFile()) return null;
  return readFileSync(abs);
}

export function scanText(text, allowed) {
  const hits = [];
  for (const m of text.matchAll(HEX64)) {
    if (!allowed.has(m[0].toLowerCase())) hits.push({ index: m.index, value: m[0] });
  }
  return hits;
}

function main() {
  const allowed = allowlist();
  let bad = 0;
  for (const path of files()) {
    const buf = content(path);
    if (!buf) continue;
    const text = buf.toString('latin1');
    for (const hit of scanText(text, allowed)) {
      const line = text.slice(0, hit.index).split('\n').length;
      // Never echo the value itself: if it is a real secret, printing it is the leak.
      console.error(`secret-scan: ${path}:${line}: 64-hex string (${hit.value.slice(0, 4)}…) is not an allow-listed test vector`);
      bad++;
    }
  }
  if (bad) {
    console.error(`secret-scan: ${bad} finding(s). Remove the value, or allow-list a PUBLIC test vector in scripts/secret-allowlist.txt.`);
    process.exit(1);
  }
  console.log(`secret-scan: ok (${staged ? 'staged' : 'working tree'})`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) main();
