import { parseJsonLine } from 'mud-framework-test-support';

import type { SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { IpcExecutor } from '../portal/executor/ipc-executor.ts';
import type { FakeServerOptions } from './fake-server-process.ts';
import { FakeServerProcess } from './fake-server-process.ts';
import { readyMessage } from './ipc-messages.ts';

export interface HarnessOptions {
  timeoutMs: number;
  /** Replaces the collecting logger: `logs()` then stays empty. */
  logger?: Logger;
  maxReplays?: number;
  autoClose?: boolean;
  autoPrepare?: boolean;
  crashOnActivate?: boolean;
  restartBackoffMs?: number;
  maxRestartBackoffMs?: number;
  shutdownTimeoutMs?: number;
  stableResetMs?: number;
  killExits?: boolean;
  exitOnShutdown?: boolean;
  startTimeoutMs?: number;
  maxPending?: number;
  closeChannelOnExit?: boolean;
  /** Answer `activate` with `ready` for the initial spawn. */
  readyOnActivate?: boolean;
  /** Hold delivered requests until `shutdown` for the initial spawn. */
  holdRequests?: boolean;
  /** Shared event log forwarded to every fake this harness spawns. */
  timeline?: string[];
  /** Label for the initial fake's timeline entries; defaults per pid. */
  label?: string;
}

export interface Harness {
  executor: IpcExecutor;
  servers: FakeServerProcess[];
  logs: () => Record<string, unknown>[];
  /**
   * Builds a fake process with the harness defaults plus overrides, recording
   * it in `servers`. `switchTo` supplies its own spawn, so a test uses this to
   * describe the replacement instead of relying on the initial factory.
   */
  makeServer: (overrides?: Partial<FakeServerOptions>) => SpawnedServer;
}

export function harness(options: Partial<HarnessOptions> = {}): Harness {
  const lines: string[] = [];
  const logger: Logger =
    options.logger ??
    createJsonLogger({
      level: 'debug',
      sink: (line) => {
        lines.push(line);
      },
    });
  const servers: FakeServerProcess[] = [];
  const makeServer = (
    overrides: Partial<FakeServerOptions> = {},
  ): SpawnedServer => {
    const server = new FakeServerProcess({
      autoCloseOnShutdown: options.autoClose ?? true,
      exitOnShutdown: options.exitOnShutdown,
      autoPrepare: options.autoPrepare,
      crashOnActivate: options.crashOnActivate,
      killExits: options.killExits,
      closeChannelOnExit: options.closeChannelOnExit,
      readyOnActivate: options.readyOnActivate,
      holdRequests: options.holdRequests,
      timeline: options.timeline,
      label: options.label,
      ...overrides,
    });
    servers.push(server);
    return server.handle;
  };
  const executor = new IpcExecutor({
    spawn: () => makeServer(),
    timeoutMs: options.timeoutMs ?? 1000,
    maxReplays: options.maxReplays,
    restartBackoffMs: options.restartBackoffMs,
    maxRestartBackoffMs: options.maxRestartBackoffMs,
    shutdownTimeoutMs: options.shutdownTimeoutMs,
    stableResetMs: options.stableResetMs,
    startTimeoutMs: options.startTimeoutMs,
    maxPending: options.maxPending,
    logger,
  });
  return {
    executor,
    servers,
    logs: () => lines.map((line) => parseJsonLine(line, 'log line')),
    makeServer,
  };
}

export function serverAt(h: Harness, index: number): FakeServerProcess {
  const server = h.servers[index];
  if (server === undefined) {
    throw new Error(`server ${index} was never spawned`);
  }
  return server;
}

/** Starts the executor and completes the handshake with one Server. */
export async function started(
  h: Harness,
  caps?: number,
): Promise<FakeServerProcess> {
  const startPromise = h.executor.start();
  const server = serverAt(h, 0);
  server.send(readyMessage(caps));
  await startPromise;
  return server;
}
