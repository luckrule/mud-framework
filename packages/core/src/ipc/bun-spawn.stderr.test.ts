import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { spawnBunServer } from './bun-spawn.ts';

/**
 * The child writes `count` lines to stderr and exits non-zero: the shape of a
 * Server that dies before the handshake, which is the only case the captured
 * tail exists for.
 */
const EXIT_TIMEOUT_MS = 5000;

function writer(lines: number): string {
  return [
    `for (let i = 0; i < ${String(lines)}; i++) process.stderr.write('line' + i + '\\n');`,
    'process.exit(3);',
  ].join(' ');
}

describe('spawnBunServer stderr capture', () => {
  test('inherits stderr and reports no tail by default', async () => {
    const server = spawnBunServer({
      command: [process.execPath, '-e', writer(2)],
    });

    const exit = await withDeadline(
      server.exited,
      EXIT_TIMEOUT_MS,
      'server.exited did not settle',
    );

    // The default must stay byte-for-byte the old behaviour: no pipe, so no
    // tail field at all rather than an empty one.
    expect(exit).toEqual({ code: 3, signal: null, observed: true });
    expect('stderrTail' in exit).toBe(false);
    expect(server.lastExit()?.stderrTail).toBeUndefined();
  });

  test('keeps only the last N lines when a tail is requested', async () => {
    const server = spawnBunServer({
      command: [process.execPath, '-e', writer(50)],
      stderrTailLines: 3,
    });

    const exit = await withDeadline(
      server.exited,
      EXIT_TIMEOUT_MS,
      'server.exited did not settle',
    );

    expect(exit.code).toBe(3);
    expect(exit.stderrTail).toEqual(['line47', 'line48', 'line49']);
  });

  test('resolves the tail with the exit, not after the channel close', async () => {
    // `exited` is what a failure path awaits, so the tail has to be final by
    // then: Bun reports the exit independently of the pipe, and a snapshot
    // taken at exit time would routinely be empty.
    const server = spawnBunServer({
      command: [process.execPath, '-e', writer(5)],
      stderrTailLines: 20,
    });

    const exit = await withDeadline(
      server.exited,
      EXIT_TIMEOUT_MS,
      'server.exited did not settle',
    );

    expect(exit.stderrTail).toEqual([
      'line0',
      'line1',
      'line2',
      'line3',
      'line4',
    ]);
  });

  test('rejects a tail size that cannot describe a bounded tail', () => {
    for (const value of [0, -1, 1.5]) {
      expect(() =>
        spawnBunServer({
          command: [process.execPath, '-e', 'process.exit(0);'],
          stderrTailLines: value,
        }),
      ).toThrow(/stderrTailLines must be a positive integer/);
    }
  });
});
