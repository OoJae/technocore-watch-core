import { chmodSync, readFileSync, writeFileSync } from 'node:fs';
const bin = new URL('../dist/cli/main.js', import.meta.url);
const src = readFileSync(bin, 'utf8');
if (!src.startsWith('#!')) writeFileSync(bin, '#!/usr/bin/env node\n' + src);
chmodSync(bin, 0o755);
