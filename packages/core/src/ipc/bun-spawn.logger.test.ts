import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import type { LogFields, Logger } from '../logging/types.ts';
import { spawnBunServer } from './bun-spawn.ts';

/**
 * The child drops the IPC pipe before exiting, which is the only way
 * `IPC_SERVER_EXITED` is produced: a process that exits after its channel is
 * already closed. The delay keeps "disconnect" ahead of "exit" instead of
 * racing them on the same event.
 */
const CHILD_EXIT_CODE = 4;
const DISCONNECT_THEN_EXIT = [
  'process.disconnect();',
  `setTimeout(() => process.exit(${String(CHILD_EXIT_CODE)}), 20);`,
].join(' ');

const DISCONNECT_REASON = 'ipc channel disconnected';

const EXIT_TIMEOUT_MS = 2000;

interface WarnCall {
  readonly fields: LogFields;
  readonly message: string;
}

function throwingLogger(warns: WarnCall[]): Logger {
  const logger: Logger = {
    debug: () => undefined,
    info: () => undefined,
    error: () => undefined,
    warn(fields, message) {
      warns.push({ fields, message });
      throw new Error('logger must not escape');
    },
    child() {
      return logger;
    },
  };
  return logger;
}

type SpawnOptions = Bun.Spawn.BaseOptions<'ignore', 'inherit', 'inherit'> & {
  cmd: string[];
};

describe('spawnBunServer', () => {
  test('a throwing logger cannot change the exit path', async () => {
    // `onExit` and the awaited `exited` promise observe the same event, and in
    // this runtime the awaited continuation records first. `IPC_SERVER_EXITED`
    // is logged from the callback, so the wrapper defers the promise
    // `spawnBunServer` awaits until that callback has run: the throw is then
    // raised where a real caller would see it escape the exit handler.
    // `Bun.spawn` is patched for this case only; the bounded exit wait keeps
    // the `finally` below reachable even when no exit event arrives.
    const originalSpawn = Bun.spawn;
    const exitObserved = Promise.withResolvers<void>();
    let exitHandlerError: unknown;
    Bun.spawn = ((options: SpawnOptions) => {
      const subprocess = originalSpawn({
        ...options,
        onExit: (child, code, signal, error) => {
          try {
            void options.onExit?.(child, code, signal, error);
          } catch (thrown) {
            exitHandlerError = thrown;
          } finally {
            exitObserved.resolve();
          }
        },
      });
      return new Proxy(subprocess, {
        get(target, property) {
          if (property === 'exited') {
            return exitObserved.promise.then(() => target.exitCode);
          }
          const value: unknown = Reflect.get(target, property, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      });
    }) as unknown as typeof Bun.spawn;

    const warns: WarnCall[] = [];
    try {
      const server = spawnBunServer({
        command: [process.execPath, '-e', DISCONNECT_THEN_EXIT],
        logger: throwingLogger(warns),
      });
      const closes: string[] = [];
      server.channel.onClose((reason) => {
        closes.push(reason);
      });

      // `server.exited` is served by the patched spawn above. A lost exit event
      // would otherwise hang the case and skip the `finally` that restores
      // `Bun.spawn`; the deadline turns that hang into an ordinary test failure.
      const exit = await withDeadline(
        server.exited,
        EXIT_TIMEOUT_MS,
        'server.exited did not settle',
      );

      expect(exitHandlerError).toBeUndefined();
      expect(warns).toHaveLength(1);
      expect(warns[0]?.fields).toMatchObject({
        code: 'IPC_SERVER_EXITED',
        exitCode: CHILD_EXIT_CODE,
        signal: null,
        reason: DISCONNECT_REASON,
      });

      expect(exit).toEqual({
        code: CHILD_EXIT_CODE,
        signal: null,
        observed: true,
      });
      expect(server.lastExit()).toEqual(exit);
      expect(closes).toEqual([DISCONNECT_REASON]);

      // The failed log does not turn killing an already ended process into a
      // new failure mode.
      expect(() => {
        server.kill();
      }).not.toThrow();
    } finally {
      Bun.spawn = originalSpawn;
    }
  });
});
