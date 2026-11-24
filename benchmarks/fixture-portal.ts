import { writeFileSync } from 'node:fs';
import { join } from 'node:path';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';
import type { Logger } from 'mud-framework-core';
import { isRecord } from 'mud-framework-test-support';

/**
 * Portal half of the e2e benchmark: a real process that spawns a real Server,
 * binds a real WebSocket port (`0`, so the OS picks it) and reports both the
 * port and the Server pid on stdout. The benchmark process is the WS client,
 * so the measured path is exactly the deployment shape: client -> Portal ->
 * IPC -> Server -> SQLite.
 *
 * argv: [databasePath, pidPath]. The pid file is written before the handshake
 * line so the benchmark can still clean up a Server whose readiness line was
 * lost. Logs are dropped: a sink that writes would land in the measured path.
 * SIGINT/SIGTERM close the layer and exit 0, following docs/INTEGRATION.md.
 */

const SERVER_ENTRY = join(import.meta.dir, 'fixture-server.ts');
const REQUEST_TIMEOUT_MS = 10_000;
const START_TIMEOUT_MS = 20_000;
const SIGNALS = ['SIGINT', 'SIGTERM'] as const;

interface Handshake {
  readonly event: 'bench-portal-ready';
  readonly wsPort: number;
  readonly serverPid: number;
  readonly portalPid: number;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function exitProcess(code: number): never {
  // oxlint-disable-next-line unicorn/no-process-exit -- the benchmark reads the exit code, and a stray handle must not delay it
  process.exit(code);
}

/**
 * The Portal's own logger is silent, but `server ready` carries the pid that
 * cleanup needs, so that one field is taken from the stream without writing
 * any line.
 */
function portalLogger(): { logger: Logger; serverPids: number[] } {
  const serverPids: number[] = [];
  const logger = createJsonLogger({
    level: 'info',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (
        isRecord(parsed) &&
        parsed.msg === 'server ready' &&
        typeof parsed.pid === 'number'
      ) {
        serverPids.push(parsed.pid);
      }
    },
  });
  return { logger, serverPids };
}

async function main(): Promise<void> {
  const databasePath = process.argv[2];
  const pidPath = process.argv[3];
  if (databasePath === undefined || pidPath === undefined) {
    throw new Error('fixture-portal requires [databasePath, pidPath]');
  }

  const { logger, serverPids } = portalLogger();
  const executor = new IpcExecutor({
    // `process.execPath` is this Bun binary: the IPC serialization requires a
    // Bun child, not Node (docs/INTEGRATION.md).
    spawn: () =>
      spawnBunServer({
        command: [process.execPath, 'run', SERVER_ENTRY, databasePath],
        logger,
      }),
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: START_TIMEOUT_MS,
    restartBackoffMs: 50,
    logger,
  });
  const transport = new WsTransport({ port: 0, logger });
  const layer = new ConnectionLayer({
    transport,
    executor,
    logger,
    shutdownTimeoutMs: 5000,
  });

  await layer.start();
  const wsPort = transport.port;
  const serverPid = serverPids.at(-1);
  if (wsPort === undefined || serverPid === undefined) {
    await layer.stop().catch(() => undefined);
    throw new Error('Portal started without a bound port or a Server pid');
  }

  writeFileSync(pidPath, `${serverPid}\n`);

  for (const signal of SIGNALS) {
    process.once(signal, () => {
      void layer.stop().then(
        () => exitProcess(0),
        (error: unknown) => {
          process.stderr.write(
            `bench portal shutdown failed: ${messageOf(error)}\n`,
          );
          exitProcess(1);
        },
      );
    });
  }

  const handshake: Handshake = {
    event: 'bench-portal-ready',
    wsPort,
    serverPid,
    portalPid: process.pid,
  };
  await Bun.stdout.write(`${JSON.stringify(handshake)}\n`);

  // The WebSocket listener holds the event loop; this keeps the entry from
  // returning and starting a normal exit. The benchmark ends this process with
  // a signal.
  await new Promise<never>(() => undefined);
}

try {
  await main();
} catch (error) {
  process.stderr.write(`bench portal failed: ${messageOf(error)}\n`);
  process.exitCode = 1;
}
