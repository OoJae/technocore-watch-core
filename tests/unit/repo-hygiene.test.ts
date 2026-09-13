import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
// @ts-expect-error - plain .mjs script without types
import { scanText } from '../../scripts/secret-scan.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p: string) => readFileSync(join(root, p), 'utf8');

describe('repository hygiene', () => {
  it('states the unofficial disclaimer and ships the MIT license', () => {
    expect(read('README.md')).toContain('Unofficial community integration — not affiliated with or endorsed by FLOP Labs.');
    expect(read('LICENSE')).toMatch(/^MIT License/);
    const pkg = JSON.parse(read('package.json')) as Record<string, unknown>;
    expect(pkg.license).toBe('MIT');
    expect(pkg.dependencies).toBeUndefined(); // zero runtime dependencies
    expect(pkg.type).toBe('module');
  });

  it('the secret scanner flags 64-hex strings unless allow-listed', () => {
    const planted = 'ab'.repeat(32); // built at runtime so this file never contains one
    expect(scanText(`seed=${planted}\n`, new Set())).toHaveLength(1);
    expect(scanText(`seed=${planted}\n`, new Set([planted]))).toHaveLength(0);
    expect(scanText(`sha512-${'a'.repeat(63)}`, new Set())).toHaveLength(0);
    expect(scanText(`${'a'.repeat(65)}`, new Set())).toHaveLength(0);
  });

  it('the secret scanner also flags 32-byte seeds written as base64url or base64', () => {
    // Built at runtime from arbitrary bytes, so this file never contains one.
    const bytes = Buffer.from(Array.from({ length: 32 }, (_, i) => (i * 73 + 29) & 0xff));
    const url = bytes.toString('base64url');
    const std = bytes.toString('base64');
    expect(url).toHaveLength(43);
    expect(std).toHaveLength(44);
    expect(scanText(`SIGN_SEED=${url}\n`, new Set())).toHaveLength(1);
    expect(scanText(`"seed": "${std}"`, new Set())).toHaveLength(1);
    expect(scanText(`const k = '${url}';`, new Set())).toHaveLength(1); // random-looking, no name needed
    expect(scanText(`SIGN_SEED=${url}\n`, new Set([url]))).toHaveLength(0); // allow-listed test vector
    // A low-entropy test seed (32 x 0x01) is flagged next to a seed name, not on its own.
    const ones = Buffer.alloc(32, 1).toString('base64url');
    expect(scanText(`signing_key: ${ones}`, new Set())).toHaveLength(1);
    expect(scanText(`task: ${ones}`, new Set())).toHaveLength(0);
    // Not seeds: other lengths, 86-char signatures, kebab-case identifiers, sha512 integrity.
    expect(scanText(`sig=${Buffer.alloc(64, 7).toString('base64url')}`, new Set())).toHaveLength(0);
    expect(scanText(`x ${url.slice(0, 42)} y ${url}A z`, new Set())).toHaveLength(0);
    expect(scanText(`technocore-watch-core-unofficial-community-in`, new Set())).toHaveLength(0);
    expect(scanText(`"integrity": "sha512-${Buffer.alloc(64, 9).toString('base64')}"`, new Set())).toHaveLength(0);
  });

  it('no tracked or pending file contains a non-allow-listed 64-hex string', () => {
    const out = execFileSync(process.execPath, [join(root, 'scripts', 'secret-scan.mjs')], { cwd: root, encoding: 'utf8' });
    expect(out).toContain('secret-scan: ok');
  });
});
