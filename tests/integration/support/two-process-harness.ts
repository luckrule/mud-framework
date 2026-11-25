import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import {
  isRecord,
  nextMessage,
  openClient,
  parseFrame,
  sleep,
} from 'mud-framework-test-support';

import { WsTransport } from '../../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from '../../../packages/core/src/index.ts';
import type { Logger } from '../../../packages/core/src/index.ts';

/**
 * Exercises spawn, handshake, exit and restart using a real Server child rather
 * than an in-memory channel pair. The Server entry is `fixtures/server-entry.ts`;
 * its behaviour is selected by argv.
 */
const FIXTURE = join(import.meta.dir, '..', 'fixtures', 'server-entry.ts');

export type Behavior =
  | 'echo'
  | 'crash'
  | 'crash-once'
  | 'boot-fail'
  | 'extension-trial'
  | 'required-extension-fail';

export interface LogCapture {
  readonly logger: Logger;
  readonly lines: Record<string, unknown>[];
}

export interface Portal {
  readonly executor: IpcExecutor;
  readonly transport: WsTransport;
  readonly layer: ConnectionLayer;
  readonly logs: LogCapture;
  /** How many times the executor called `spawn`, so a restart loop is visible. */
  spawns: () => number;
}

function capturingLogger(): LogCapture {
  const lines: Record<string, unknown>[] = [];
  const logger = createJsonLogger({
    level: 'debug',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (isRecord(parsed)) {
        lines.push(parsed);
      }
    },
  });
  return { logger, lines };
}

export function createPortal(options: {
  databasePath: string;
  behavior: Behavior;
  timeoutMs?: number;
  startTimeoutMs?: number;
  restartBackoffMs?: number;
}): Portal {
  const logs = capturingLogger();
  // `process.execPath` is the running Bun binary; the child must be Bun because
  // the IPC serialization is Bun-only (docs/INTEGRATION.md).
  const command = [
    process.execPath,
    'run',
    FIXTURE,
    options.behavior,
    options.databasePath,
  ];
  const spawnCounter = { count: 0 };
  const executor = new IpcExecutor({
    spawn: () => {
      spawnCounter.count += 1;
      return spawnBunServer({ command });
    },
    timeoutMs: options.timeoutMs ?? 2000,
    startTimeoutMs: options.startTimeoutMs,
    restartBackoffMs: options.restartBackoffMs,
    logger: logs.logger,
  });
  const transport = new WsTransport({ port: 0 });
  const layer = new ConnectionLayer({
    transport,
    executor,
    logger: logs.logger,
  });
  return {
    executor,
    transport,
    layer,
    logs,
    spawns: () => spawnCounter.count,
  };
}

/** One request on an already-open socket, which survives a Server restart. */
export async function callOnSocket(
  socket: WebSocket,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const reply = nextMessage(socket);
  socket.send(JSON.stringify(request));
  return parseFrame(await reply);
}

/** One request over a fresh socket, so each call also crosses a new connection. */
export async function call(
  url: string,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const socket = await openClient(url);
  try {
    return await callOnSocket(socket, request);
  } finally {
    socket.close();
  }
}

/** One line per `store-once` handler execution, written by the fixture. */
export function attemptLog(databasePath: string): string[] {
  return readFileSync(`${databasePath}.attempts`, 'utf8')
    .split('\n')
    .filter((line) => line.length > 0);
}

/**
 * The fixture writes the startup failure reason next to the database because
 * its stderr is inherited and therefore unreadable here. `start()` rejects on
 * its own deadline, which can fire while the last child is still exiting, so
 * poll briefly instead of assuming the file is already there.
 */
export async function startupFailure(databasePath: string): Promise<string> {
  const path = `${databasePath}.startup-error`;
  const deadline = Date.now() + 2000;
  while (!existsSync(path)) {
    if (Date.now() >= deadline) {
      throw new Error(`no startup failure reason was written to ${path}`);
    }
    await sleep(10);
  }
  return readFileSync(path, 'utf8');
}

export function urlOf(portal: Portal): string {
  const port = portal.transport.port;
  if (port === undefined) {
    throw new Error('expected the transport to bind a port');
  }
  return `ws://127.0.0.1:${port}/`;
}

/** Every Server process pid the Portal saw reach `ready`, in order. */
export function serverPids(logs: LogCapture): number[] {
  const pids: number[] = [];
  for (const line of logs.lines) {
    const pid = line.pid;
    if (line.msg === 'server ready' && typeof pid === 'number') {
      pids.push(pid);
    }
  }
  return pids;
}

export function replayCounts(logs: LogCapture): number[] {
  const counts: number[] = [];
  for (const line of logs.lines) {
    const replayCount = line.replayCount;
    if (line.msg === 'request replayed' && typeof replayCount === 'number') {
      counts.push(replayCount);
    }
  }
  return counts;
}

/**
 * Always releases the Portal, then any Server pid it ever reported. A test that
 * fails its assertions must not leave a child behind for the rest of the suite.
 */
export async function stopQuietly(portal: Portal): Promise<void> {
  try {
    await portal.layer.stop();
  } catch {
    // The failure being reported is the test's own; a shutdown error here would
    // replace it with a less useful one.
  }
  for (const pid of serverPids(portal.logs)) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone, which is the expected case.
    }
  }
}

/** `start()` either settles or it does not; a hang is the failure under test. */
export async function settleWithin(
  promise: Promise<unknown>,
  timeoutMs: number,
): Promise<'resolved' | 'rejected' | 'timeout'> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => {
      resolve('timeout');
    }, timeoutMs);
  });
  const settled = promise.then(
    (): 'resolved' => 'resolved',
    (): 'rejected' => 'rejected',
  );
  try {
    return await Promise.race([settled, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}
