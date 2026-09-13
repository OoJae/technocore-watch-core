// The built CLI runs main() only when it IS the entry point (directly or through a symlink such
// as npm's .bin shim), never merely because a wrapper script shares its name and imports it.
// Needs dist/ (npm test builds first); makes no network requests.
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, symlink, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { tempDir } from './helpers.ts';

const here = dirname(fileURLToPath(import.meta.url));
const BIN = join(here, '..', '..', 'dist', 'cli', 'main.js');
let tmp: Awaited<ReturnType<typeof tempDir>>;

function node(args: string[]): Promise<{ code: number | null; out: string; err: string }> {
  return new Promise((resolve) => {
    const p = spawn(process.execPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let err = '';
    p.stdout.on('data', (d: Buffer) => (out += d.toString()));
    p.stderr.on('data', (d: Buffer) => (err += d.toString()));
    p.on('exit', (code) => resolve({ code, out, err }));
  });
}

beforeAll(async () => {
  if (!existsSync(BIN)) throw new Error('dist/cli/main.js missing: run `npm run build` (npm test does this)');
  tmp = await tempDir('tcw entry ');
});

afterAll(async () => {
  await tmp?.cleanup();
});

describe('CLI entry point detection', () => {
  const args = (data: string) => ['status', '--json', '--data', data, '--origin', 'http://127.0.0.1:9'];

  it('runs when executed directly and through a symlink named technocore-watch', async () => {
    const direct = await node([BIN, ...args(join(tmp.path, 'd1'))]);
    expect(direct.code).toBe(0);
    expect(JSON.parse(direct.out).scopeKey).toBe('default');
    const binDir = join(tmp.path, 'node_modules', '.bin');
    await mkdir(binDir, { recursive: true });
    await symlink(BIN, join(binDir, 'technocore-watch'));
    const linked = await node([join(binDir, 'technocore-watch'), ...args(join(tmp.path, 'd2'))]);
    expect(linked.code).toBe(0);
    expect(JSON.parse(linked.out).scopeKey).toBe('default');
  });

  it('does not run a second time inside a wrapper that merely shares the name', async () => {
    const wrapDir = join(tmp.path, 'plugin', 'bin');
    await mkdir(wrapDir, { recursive: true });
    const wrapper = join(wrapDir, 'technocore-watch');
    await writeFile(
      wrapper,
      `import { main } from ${JSON.stringify(pathToFileURL(BIN).href)};\n` +
        `const code = await main(['bogus-from-wrapper'], { out: (s) => process.stdout.write(s + '\\n'), err: (s) => process.stderr.write(s + '\\n'), env: process.env });\n` +
        `process.stdout.write('wrapper-exit ' + code + '\\n');\n`,
    );
    await writeFile(join(tmp.path, 'plugin', 'package.json'), JSON.stringify({ type: 'module' }));
    const r = await node([wrapper, ...args(join(tmp.path, 'd3'))]);
    expect(r.err).toContain('unknown command');
    expect(r.out.trim()).toBe('wrapper-exit 2'); // no `status` output from an automatic second run
  });
});
