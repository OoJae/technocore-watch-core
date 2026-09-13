// src/protocol is vendored verbatim by other packages (n8n-nodes-technocore), whose lint bans
// timers, process and file-system access. Keep it pure: relative imports inside protocol/ only.
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const dir = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'src', 'protocol');
const files = readdirSync(dir).filter((f) => f.endsWith('.ts'));

describe('protocol/ is pure and vendorable', () => {
  it.each(files)('%s imports only sibling protocol modules and uses no I/O or timers', (file) => {
    const src = readFileSync(join(dir, file), 'utf8');
    const code = src.replace(/\/\/.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
    for (const m of code.matchAll(/(?:import|export)[^'"]*?from\s+['"]([^'"]+)['"]/g)) {
      expect(m[1], `${file} imports ${m[1]}`).toMatch(/^\.\/[a-z-]+\.ts$/);
    }
    expect(code).not.toMatch(/\b(setTimeout|setInterval|setImmediate|process|require|fetch|Buffer|globalThis|Date\.now)\b/);
    expect(code).not.toMatch(/['"]node:/);
  });
});
