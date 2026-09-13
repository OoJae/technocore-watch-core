import { describe, expect, it } from 'vitest';
import { parseExportLine, parseReadView, parseRefusal, quoteNonceNumbers, ProtocolError } from '../../src/protocol/parse.ts';
import { assertRoomName, isValidRoomName, roomClasses } from '../../src/protocol/names.ts';

const headers = (h: Record<string, string>) => ({ get: (n: string) => h[n.toLowerCase()] ?? null });

describe('names', () => {
  it('mirrors NAME_RE with fullmatch semantics', () => {
    expect(isValidRoomName('lobby')).toBe(true);
    expect(isValidRoomName('a'.repeat(48))).toBe(true);
    expect(isValidRoomName('a'.repeat(49))).toBe(false);
    expect(isValidRoomName('Lobby')).toBe(false);
    expect(isValidRoomName('-x')).toBe(false);
    expect(isValidRoomName('abc\n')).toBe(false);
    expect(isValidRoomName('a/b')).toBe(false);
    expect(isValidRoomName('')).toBe(false);
    expect(() => assertRoomName('../etc')).toThrow(TypeError);
  });

  it('composes class markers by prefix', () => {
    expect(roomClasses('p-x')).toEqual({ private: true, mailbox: false, ownable: false, ephemeral: false });
    expect(roomClasses('mb-p-x')).toMatchObject({ private: true, mailbox: true });
    expect(roomClasses('e-p-x')).toMatchObject({ private: true, ephemeral: true });
    expect(roomClasses('pastel')).toEqual({ private: false, mailbox: false, ownable: false, ephemeral: false });
    expect(roomClasses('p-')).toMatchObject({ private: true });
    expect(roomClasses('d-team').ownable).toBe(true);
    expect(roomClasses('x-p-y').private).toBe(false);
  });
});

describe('parseReadView', () => {
  it('keeps 19-digit nonces exact (past 2^53)', () => {
    const body = `{\n "room": "lobby",\n "count": 1,\n "first_seq": 7,\n "last_seq": 7,\n "generation": 3,\n "messages": [\n  {\n   "seq": 7,\n   "ts": "2026-09-13T12:00:00.000001Z",\n   "from": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",\n   "text": "hi",\n   "nonce": 9223372036854775807,\n   "sig": "${'A'.repeat(86)}"\n  }\n ]\n}\n`;
    const v = parseReadView(body);
    expect(v.messages[0]!.nonce).toBe('9223372036854775807');
    expect(v.generation).toBe(3);
    expect(v.first_seq).toBe(7);
  });

  it('does not let message text reshape the nonce rewrite', () => {
    const hostile = 'x", "nonce": 12345678901234567890, "y": "';
    const body = JSON.stringify({ room: 'r', count: 1, first_seq: 1, last_seq: 1, generation: 1, messages: [{ seq: 1, ts: 't', from: 'a', text: hostile }] });
    const v = parseReadView(body);
    expect(v.messages[0]!.text).toBe(hostile);
    expect(v.messages[0]!.nonce).toBeUndefined();
    // compact export form
    const line = JSON.stringify({ seq: 2, ts: 't', from: 'a', text: '\\"nonce\\":1' }).replace('}', ',"nonce":1757000000000123}');
    expect(parseExportLine(line)!.nonce).toBe('1757000000000123');
    expect(quoteNonceNumbers('{"a":1}')).toBe('{"a":1}');
  });

  it('decodes escaped key spellings', () => {
    const body = '{"room":"r","count":1,"last_seq":1,"messages":[{"seq":1,"ts":"t","from":"a","text":"x","\\u006eonce":18446744073709551615}]}';
    expect(parseReadView(body).messages[0]!.nonce).toBe('18446744073709551615');
  });

  it('treats generation and wait_held as optional', () => {
    const v = parseReadView('{"room":"r","count":0,"first_seq":null,"last_seq":5,"messages":[],"wait_held":false}');
    expect(v.generation).toBeUndefined();
    expect(v.wait_held).toBe(false);
    expect(v.last_seq).toBe(5);
  });

  it('rejects malformed views', () => {
    expect(() => parseReadView('nope')).toThrow(ProtocolError);
    expect(() => parseReadView('[]')).toThrow(ProtocolError);
    expect(() => parseReadView('{"room":"r","last_seq":1,"messages":[{"seq":"1","ts":"t","from":"a","text":"x"}]}')).toThrow(ProtocolError);
    expect(() => parseReadView('{"room":"r","last_seq":2,"messages":[{"seq":2,"ts":"t","from":"a","text":"x"},{"seq":1,"ts":"t","from":"a","text":"x"}]}')).toThrow(/ascending/);
  });

  it('skips torn export lines', () => {
    expect(parseExportLine('{"seq":1,"ts":"t","from":"a","te')).toBeNull();
    expect(parseExportLine('')).toBeNull();
    expect(parseExportLine('{"seq":1,"ts":"t","from":"a","text":"ok"}')!.seq).toBe(1);
  });
});

describe('parseRefusal', () => {
  it('reads the 429 body first (the server repeats Retry-After there)', () => {
    const body =
      '429 rate limited: the read budget for your IP (10/min) is spent.\nretry after: 6s — the bucket refills continuously (one token every 6s), so waiting longer buys a bigger burst, up to 10.\n';
    expect(parseRefusal(429, body, headers({ 'retry-after': '99' }))).toEqual({ kind: 'rate', bucket: 'read', perMinute: 10, retryAfterS: 6 });
  });

  it('falls back to the header, then to 60s, and caps hostile values', () => {
    expect(parseRefusal(429, 'slow down', headers({ 'retry-after': '12' }))).toMatchObject({ retryAfterS: 12 });
    expect(parseRefusal(429, 'slow down', headers({}))).toMatchObject({ retryAfterS: 60 });
    expect(parseRefusal(429, 'retry after: 999999s', headers({}))).toMatchObject({ retryAfterS: 3600 });
    expect(parseRefusal(429, '429 room-creation budget spent: ...\nretry after: 4320s', headers({}))).toMatchObject({ bucket: 'rooms' });
  });

  it('maps the other statuses', () => {
    expect(parseRefusal(400, "400 bad name 'X'\nmore", headers({}))).toEqual({ kind: 'bad-request', message: "400 bad name 'X'" });
    expect(parseRefusal(403, '403 mailbox', headers({})).kind).toBe('forbidden');
    expect(parseRefusal(409, '409', headers({})).kind).toBe('conflict');
    expect(parseRefusal(413, '413', headers({})).kind).toBe('too-large');
    expect(parseRefusal(422, '422 duplicate text', headers({})).kind).toBe('duplicate');
    expect(parseRefusal(503, 'x', headers({}))).toEqual({ kind: 'server', status: 503 });
  });
});
