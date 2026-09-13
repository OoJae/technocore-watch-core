// Test writer: posts directly to the LOCAL server (never through the proxy, never to
// production). Signed posts use a fixed PUBLIC TEST-ONLY seed: 32 bytes of 0x01.
import { createPrivateKey, createPublicKey, sign } from 'node:crypto';
import * as http from 'node:http';
import { sweepText } from '../../src/protocol/sweep.ts';

function assertLocal(origin: string): URL {
  const u = new URL(origin);
  if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') throw new Error(`writer refuses non-local origin ${origin}`);
  return u;
}

export function postJson(origin: string, room: string, body: Record<string, string>): Promise<{ status: number; text: string }> {
  const u = assertLocal(origin);
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: u.hostname, port: u.port, method: 'POST', path: `/r/${room}?format=json`, agent: false, headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) } },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, text: Buffer.concat(chunks).toString('utf8') }));
      },
    );
    req.on('error', reject);
    req.end(payload);
  });
}

export async function post(origin: string, room: string, text: string, from = 'writer'): Promise<number> {
  const r = await postJson(origin, room, { from, text });
  if (r.status !== 200) throw new Error(`post failed ${r.status}: ${r.text.slice(0, 200)}`);
  return (JSON.parse(r.text) as { posted: { seq: number } }).posted.seq;
}

export async function postMany(origin: string, room: string, n: number, text: (i: number) => string, concurrency = 1): Promise<number[]> {
  const seqs: number[] = [];
  let next = 0;
  const worker = async () => {
    while (next < n) {
      const i = next++;
      seqs.push(await post(origin, room, text(i)));
    }
  };
  await Promise.all(Array.from({ length: Math.max(1, concurrency) }, worker));
  return seqs.sort((a, b) => a - b);
}

// ---- signed lane (TEST-ONLY key) ----------------------------------------------------------

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function base58(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  let out = '';
  while (n > 0n) {
    out = B58[Number(n % 58n)] + out;
    n /= 58n;
  }
  for (const b of bytes) {
    if (b !== 0) break;
    out = '1' + out;
  }
  return out;
}

/** PUBLIC TEST-ONLY seed (32 x 0x01). Never use for anything real. */
const TEST_SEED = Buffer.alloc(32, 1);
const PKCS8_ED25519_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex');

export function testIdentity() {
  const key = createPrivateKey({ key: Buffer.concat([PKCS8_ED25519_PREFIX, TEST_SEED]), format: 'der', type: 'pkcs8' });
  const jwk = createPublicKey(key).export({ format: 'jwk' }) as { x: string };
  const pub = Buffer.from(jwk.x, 'base64url');
  const did = `did:key:z${base58(Buffer.concat([Buffer.from([0xed, 0x01]), pub]))}`;
  return { key, did };
}

let lastNonce = 0;

export async function postSigned(origin: string, room: string, text: string): Promise<number> {
  const { key, did } = testIdentity();
  lastNonce = Math.max(Date.now(), lastNonce + 1);
  const nonce = String(lastNonce);
  const body = sweepText(text);
  const sig = sign(null, Buffer.from(`${room}|${nonce}|${body}`, 'utf8'), key).toString('base64url');
  const r = await postJson(origin, room, { did, sig, nonce, text });
  if (r.status !== 200) throw new Error(`signed post failed ${r.status}: ${r.text.slice(0, 300)}`);
  return (JSON.parse(r.text) as { posted: { seq: number } }).posted.seq;
}
