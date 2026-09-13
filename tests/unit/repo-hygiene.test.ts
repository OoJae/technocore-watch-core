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

  it('no tracked or pending file contains a non-allow-listed 64-hex string', () => {
    const out = execFileSync(process.execPath, [join(root, 'scripts', 'secret-scan.mjs')], { cwd: root, encoding: 'utf8' });
    expect(out).toContain('secret-scan: ok');
  });
});
