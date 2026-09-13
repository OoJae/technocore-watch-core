// Child process for the dispose leak check: runs a real watcher (file store, node:http
// transport, long-polls), disposes on request, reports active resources, and must then
// exit ON ITS OWN — no process.exit() — which only happens when no handles are left.
// Plain ESM against the BUILT package (dist/, which `npm test` builds first), so it runs on every
// supported Node version without TypeScript stripping, and exercises what ships.
import { createWatcher, FileStateStore } from '../../dist/index.js';

const [origin, dataDir] = process.argv.slice(2);
if (!origin || !dataDir) throw new Error('usage: dispose-child <origin> <dataDir>');
const w = createWatcher({
  origin,
  scopeKey: 'dispose-child',
  store: new FileStateStore(dataDir),
  coalesce: { quietMs: 100, maxDelayMs: 300, minWakeIntervalMs: 0 },
  limits: { syncIntervalMs: 500 },
});
w.onNotice(async (n) => {
  process.stdout.write(JSON.stringify({ notice: n.totalNew }) + '\n');
  await w.markNotified(n.id);
});
await w.subscribe('dispose-a');
await w.subscribe('dispose-b');
process.stdout.write(JSON.stringify({ ready: true }) + '\n');

process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  if (!chunk.includes('dispose')) return;
  process.stdin.destroy();
  void (async () => {
    const before = process.getActiveResourcesInfo();
    await w.dispose();
    const after = process.getActiveResourcesInfo();
    process.stdout.write(JSON.stringify({ disposed: true, before, after }) + '\n');
  })();
});
