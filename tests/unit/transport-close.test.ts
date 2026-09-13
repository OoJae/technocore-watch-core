// NodeHttpTransport.close() resolves only once its sockets are really closed, so a disposed
// watcher leaves no TCP handles behind the moment dispose() resolves. The server runs in a child
// process so every TCPSocketWrap in this process belongs to the transport under test.
import { spawn, type ChildProcess } from 'node:child_process';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { NodeHttpTransport } from '../../src/transport/http.ts';

let server: ChildProcess;
let port = 0;

const SERVER = `
const http = require('node:http');
const s = http.createServer((req, res) => {
  if (req.url.startsWith('/hold')) return; // a long-poll that never answers
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end('{"ok":true}');
});
s.keepAliveTimeout = 60000;
s.listen(0, '127.0.0.1', () => process.stdout.write(String(s.address().port) + '\\n'));
`;

const tcpHandles = () => process.getActiveResourcesInfo().filter((r) => r === 'TCPSocketWrap').length;

beforeAll(async () => {
  server = spawn(process.execPath, ['-e', SERVER], { stdio: ['ignore', 'pipe', 'inherit'] });
  port = await new Promise<number>((resolve) => server.stdout!.once('data', (d: Buffer) => resolve(Number(String(d).trim()))));
});

afterAll(() => {
  server?.kill('SIGKILL');
});

describe('NodeHttpTransport.close', () => {
  it('awaits the close of keep-alive and in-flight sockets', async () => {
    const baseline = tcpHandles();
    for (let round = 0; round < 6; round++) {
      const t = new NodeHttpTransport();
      const ctrl = new AbortController();
      // Two finished keep-alive requests (idle pooled sockets) and two held in flight.
      for (let i = 0; i < 2; i++) {
        const res = await t.get({ url: `http://127.0.0.1:${port}/r/x?i=${i}`, signal: ctrl.signal });
        expect(await res.text()).toBe('{"ok":true}');
      }
      const held = [0, 1].map((i) => t.get({ url: `http://127.0.0.1:${port}/hold?i=${i}`, signal: ctrl.signal }).catch((e: unknown) => e));
      await new Promise((r) => setTimeout(r, 100));
      expect(tcpHandles()).toBeGreaterThan(baseline);
      // As in dispose(): the pool aborts its long-polls first (destroying their sockets, whose
      // handles close asynchronously), then closes the transport.
      if (round % 2 === 1) ctrl.abort();
      await t.close();
      expect(tcpHandles()).toBe(baseline);
      await Promise.all(held);
    }
  });
});
