import { describe, expect, it } from 'vitest';
import { main, parseArgs } from '../../src/cli/main.ts';

describe('CLI argument handling', () => {
  it('parses commands, positionals, repeated and boolean flags', () => {
    const p = parseArgs(['inbox', 'lobby', '--max', '5', '--peek', '--scope=a', '--scope', 'b']);
    expect(p.command).toBe('inbox');
    expect(p.positionals).toEqual(['lobby']);
    expect(p.flags.get('max')).toEqual(['5']);
    expect(p.flags.get('peek')).toEqual(['true']);
    expect(p.flags.get('scope')).toEqual(['a', 'b']);
  });

  it('rejects bad input with exit code 2 and never touches the network', async () => {
    const out: string[] = [];
    const err: string[] = [];
    const io = { out: (s: string) => out.push(s), err: (s: string) => err.push(s), env: {} };
    expect(await main([], io)).toBe(2);
    expect(await main(['subscribe', '../x', '--data', '/nonexistent-never-created', '--origin', 'http://127.0.0.1:9'], io)).toBe(2);
    expect(await main(['inbox', '--max', '0', '--data', '/nonexistent-never-created', '--origin', 'http://127.0.0.1:9'], io)).toBe(2);
    expect(await main(['bogus'], io)).toBe(2);
    expect(await main(['status', '--origin', 'ftp://x'], io)).toBe(1);
    expect(err.join('\n')).toContain('valid room name');
  });

  it('validates the flood pacing flags before taking the poller lock', async () => {
    const err: string[] = [];
    const io = { out: () => undefined, err: (s: string) => err.push(s), env: {} };
    const base = ['--data', '/nonexistent-never-created', '--origin', 'http://127.0.0.1:9'];
    expect(await main(['run', '--flood-pacing', 'maybe', ...base], io)).toBe(2);
    expect(await main(['run', '--repoll-lead-ms', '10001', ...base], io)).toBe(2);
    expect(await main(['monitor', '--flood-repoll-min-ms', '99', ...base], io)).toBe(2);
    expect(err).toEqual([
      '--flood-pacing must be on or off',
      '--repoll-lead-ms must be a number in [0, 10000]',
      '--flood-repoll-min-ms must be a number in [100, 10000]',
    ]);
  });
});
