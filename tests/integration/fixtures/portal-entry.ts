import { join } from 'node:path';

import { WsTransport } from '../../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from '../../../packages/core/src/index.ts';
import type { Logger } from '../../../packages/core/src/index.ts';

/**
 * Real Portal entry for the portal-death integration test: an independent OS
 * process that spawns a real Server, binds a real WebSocket port, and then
 * stays alive until the test signals it.
 *
 * argv: [databasePath, mode]. `graceful` installs TERM/INT/HUP handlers that
 * close the layer and exit 0, following the SIGTERM shutdown pattern documented
 * in docs/INTEGRATION.md and extending it to INT/HUP; `default` installs
 * nothing, so the OS default action terminates this process and the Server can
 * only learn about the death through IPC disconnect or the parent-liveness
 * poll.
 *
 * stdout carries this file's handshake line; the Server inherits this stdout
 * and interleaves its own JSON log lines, including before the handshake, so
 * the test skips them until it finds `event: "portal-ready"`. All diagnostics
 * from this file go to stderr, including the Server pid announced before the
 * handshake: if the handshake is lost or unparsable, that line is the only way
 * the test can still find and kill the Server. Setting
 * `PORTAL_FIXTURE_BROKEN_HANDSHAKE=1` replaces the handshake with a deliberately
 * unparsable line and keeps the process alive, so the test's failure path has a
 * real Server to clean up.
 */

const SERVER_FIXTURE = join(import.meta.dir, 'server-entry.ts');

const SIGNALS = ['SIGHUP', 'SIGINT', 'SIGTERM'] as const;
const MODES = ['graceful', 'default'] as const;

type Mode = (typeof MODES)[number];

interface ReadyLine {
  readonly event: 'portal-ready';
  readonly mode: Mode;
  readonly portalPid: number;
  readonly serverPid: number;
  readonly wsPort: number;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readMode(raw: string | undefined): Mode {
  if (raw === undefined) {
    throw new Error(
      `portal-entry requires a mode argument; expected one of ${MODES.join(', ')}`,
    );
  }
  const mode = MODES.find((candidate) => candidate === raw);
  if (mode === undefined) {
    throw new Error(
      `unknown portal mode ${JSON.stringify(raw)}; expected one of ${MODES.join(', ')}`,
    );
  }
  return mode;
}

/** The last `server ready` log line is the process the Portal is serving with. */
function serverPidFrom(
  lines: readonly Record<string, unknown>[],
): number | undefined {
  let pid: number | undefined;
  for (const line of lines) {
    if (line.msg === 'server ready' && typeof line.pid === 'number') {
      pid = line.pid;
    }
  }
  return pid;
}

/**
 * The exit code is the contract the test reads, so it must not be delayed by a
 * handle a finished layer forgot; that is why this is `process.exit`, not
 * `process.exitCode`.
 */
function exitProcess(code: number): never {
  // oxlint-disable-next-line unicorn/no-process-exit -- the signal-handler path must exit with the documented code even if a stray handle remains
  process.exit(code);
}

function reportShutdownFailure(error: unknown): never {
  console.error('portal shutdown failed:', messageOf(error));
  return exitProcess(1);
}

function installGracefulHandlers(layer: ConnectionLayer): void {
  const closeThenExit = (): void => {
    void layer.stop().then(() => exitProcess(0), reportShutdownFailure);
  };
  for (const signal of SIGNALS) {
    process.once(signal, closeThenExit);
  }
}

async function main(): Promise<void> {
  const databasePath = process.argv[2];
  if (databasePath === undefined) {
    throw new Error('portal-entry requires a database path argument');
  }
  const mode = readMode(process.argv[3]);

  const lines: Record<string, unknown>[] = [];
  const logger: Logger = createJsonLogger({
    level: 'debug',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (isRecord(parsed)) {
        lines.push(parsed);
      }
    },
  });

  const executor = new IpcExecutor({
    spawn: () =>
      spawnBunServer({
        command: [
          process.execPath,
          'run',
          SERVER_FIXTURE,
          'echo',
          databasePath,
        ],
        logger,
      }),
    timeoutMs: 2000,
    logger,
  });
  const transport = new WsTransport({ port: 0, logger });
  const layer = new ConnectionLayer({ transport, executor, logger });

  await layer.start();
  const wsPort = transport.port;
  const serverPid = serverPidFrom(lines);
  if (wsPort === undefined || serverPid === undefined) {
    await layer.stop().catch(() => undefined);
    throw new Error('portal started without a bound port or a Server pid');
  }

  if (mode === 'graceful') {
    installGracefulHandlers(layer);
  }

  // stdout is the handshake channel, so the pid the test needs if that
  // handshake never becomes usable goes to stderr.
  console.error(`portal server pid ${serverPid}`);

  if (process.env.PORTAL_FIXTURE_BROKEN_HANDSHAKE === '1') {
    await Bun.stdout.write(
      'portal handshake failed: intentional fixture fault\n',
    );
    await new Promise<never>(() => undefined);
  }

  const ready: ReadyLine = {
    event: 'portal-ready',
    mode,
    portalPid: process.pid,
    serverPid,
    wsPort,
  };
  try {
    await Bun.stdout.write(`${JSON.stringify(ready)}\n`);
  } catch (error) {
    await layer.stop().catch(() => undefined);
    throw error;
  }

  // The WebSocket listener holds the event loop; this await only keeps the
  // entry from returning and starting a normal exit. The test ends this
  // process with a signal.
  await new Promise<never>(() => undefined);
}

try {
  await main();
} catch (error) {
  console.error('portal process failed:', messageOf(error));
  process.exitCode = 1;
}
