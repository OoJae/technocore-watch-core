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

  it('package metadata points at the GitHub repo, names the author without an email, and publishes with provenance', () => {
    const pkg = JSON.parse(read('package.json')) as Record<string, unknown>;
    expect(pkg.author).toBe('OoJae (https://github.com/OoJae)');
    expect(pkg.repository).toEqual({ type: 'git', url: 'git+https://github.com/OoJae/technocore-watch-core.git' });
    expect(pkg.homepage).toBe('https://github.com/OoJae/technocore-watch-core#readme');
    expect(pkg.bugs).toEqual({ url: 'https://github.com/OoJae/technocore-watch-core/issues' });
    const publish = read('.github/workflows/publish.yml');
    expect(publish).toContain('npm publish --provenance --access public');
    // Trusted publishing needs npm >= 11.5.1: Node 24 plus an explicit npm upgrade.
    expect(publish).toMatch(/node-version: 24\b/);
    expect(publish).toContain('npm install -g npm@^11.5.1');
    // A tag that is not exactly v<package.json version> refuses; no dependency lifecycle scripts run in a
    // job that can mint an OIDC token; the checks run before anything is published.
    expect(publish).toContain('if [ "$GITHUB_REF_NAME" != "v$version" ]; then');
    expect(publish).toContain('npm ci --ignore-scripts');
    expect(publish).not.toMatch(/npm ci\s*$/m);
    expect(publish).toMatch(/Check the tag matches package\.json[\s\S]*npm ci --ignore-scripts[\s\S]*npm run build[\s\S]*npm run secret-scan[\s\S]*npm run typecheck[\s\S]*npm run test:unit[\s\S]*npm publish --provenance/);
    // The first release uses a short-lived token secret (the package must exist before a trusted publisher can be added).
    expect(publish).toContain('NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}');
  });

  it('workflows use action majors that run on Node 24', () => {
    for (const f of ['.github/workflows/ci.yml', '.github/workflows/publish.yml']) {
      const uses = [...read(f).matchAll(/uses:\s*(\S+)/g)].map((m) => m[1]!);
      expect(uses.length, f).toBeGreaterThan(0);
      for (const u of uses) expect(['actions/checkout@v5', 'actions/setup-node@v5', 'astral-sh/setup-uv@v7'], `${f}: ${u}`).toContain(u);
    }
  });

  it('no tracked or pending file contains an email address', () => {
    const git = (args: string[]) => execFileSync('git', args, { cwd: root, encoding: 'utf8' }).split('\0').filter(Boolean);
    const files = [...new Set([...git(['ls-files', '-z']), ...git(['ls-files', '--others', '--exclude-standard', '-z'])])];
    // Built from parts so this file never matches itself.
    const email = new RegExp(['[A-Za-z0-9._%+-]+', '@', '[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}'].join(''), 'g');
    const hits: string[] = [];
    for (const f of files) {
      let text: string;
      try {
        text = readFileSync(join(root, f), 'utf8');
      } catch {
        continue; // deleted in the working tree
      }
      for (const m of text.matchAll(email)) hits.push(`${f}: ${m[0]}`);
    }
    expect(hits).toEqual([]);
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
