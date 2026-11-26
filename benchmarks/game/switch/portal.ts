import { fileURLToPath } from 'node:url';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';

import { PORTAL_SHUTDOWN_TIMEOUT_MS } from './budget.ts';

/**
 * The benchmark Portal: the same assembly as `examples/game-slice/src/portal.ts`
 * plus a loopback-only control endpoint. The endpoint is what lets the
 * coordinator trigger a real `switchTo` while clients are connected; it is not
 * part of the framework and is deliberately not reachable from outside
 * 127.0.0.1. The port is OS-assigned so parallel runs cannot collide.
 */
const DEFAULT_PORTAL_PORT = 8080;
const SHUTDOWN_SIGNALS = ['SIGINT', 'SIGTERM'] as const;
const SERVER_ENTRY = fileURLToPath(
  new URL('../../../examples/game-slice/src/server.ts', import.meta.url),
);

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function readPort(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0 || value > 65535) {
    throw new Error(
      `${name} must be an integer between 0 and 65535, got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

function write(line: string): void {
  process.stdout.write(`${line}\n`);
}

function writeError(line: string): void {
  process.stderr.write(`${line}\n`);
}

function json(status: number, body: Record<string, unknown>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

async function main(): Promise<void> {
  const portalPort = readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT);

  const executor = new IpcExecutor({
    spawn: () =>
      spawnBunServer({
        command: [process.execPath, 'run', SERVER_ENTRY],
      }),
    timeoutMs: 5000,
    startTimeoutMs: 10_000,
    shutdownTimeoutMs: PORTAL_SHUTDOWN_TIMEOUT_MS,
  });
  // The coordinator derives its stop budget from this line, so the benchmark
  // never waits on a stale copy of core's close budget.
  write(`portal: executor close budget ${executor.closeBudgetMs} ms`);
  const transport = new WsTransport({
    port: portalPort,
    hostname: '127.0.0.1',
  });
  const layer = new ConnectionLayer({
    transport,
    executor,
    shutdownTimeoutMs: PORTAL_SHUTDOWN_TIMEOUT_MS,
  });

  // One switch at a time: the coordinator only sends one, but a second caller
  // must be refused instead of racing the first through `switchTo`.
  let switching = false;
  const control = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    async fetch(request) {
      const path = new URL(request.url).pathname;
      if (path !== '/switch') {
        return json(404, { ok: false, error: 'not found' });
      }
      if (request.method !== 'POST') {
        return new Response('method not allowed', {
          status: 405,
          headers: { allow: 'POST' },
        });
      }
      if (switching) {
        return json(409, { ok: false, error: 'a switch is already running' });
      }
      switching = true;
      const startedAt = Bun.nanoseconds();
      try {
        await executor.switchTo({
          spawn: () =>
            spawnBunServer({
              command: [process.execPath, 'run', SERVER_ENTRY],
            }),
        });
        return json(200, {
          ok: true,
          durationNs: Bun.nanoseconds() - startedAt,
        });
      } catch (error) {
        return json(500, {
          ok: false,
          durationNs: Bun.nanoseconds() - startedAt,
          error: messageOf(error),
        });
      } finally {
        switching = false;
      }
    },
  });

  let stopping = false;
  const lifetime = Promise.withResolvers<void>();
  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    write(`portal: shutting down (${signal})`);
    try {
      await control.stop(true);
      await layer.stop();
      process.exitCode = 0;
    } catch (error) {
      writeError(`portal: shutdown failed: ${messageOf(error)}`);
      process.exitCode = 1;
    } finally {
      lifetime.resolve();
    }
  };

  for (const signal of SHUTDOWN_SIGNALS) {
    process.once(signal, () => {
      void shutdown(signal);
    });
  }

  try {
    await layer.start();
  } catch (error) {
    await control.stop(true);
    throw error;
  }
  const boundPort = transport.port;
  if (boundPort === undefined) {
    await control.stop(true);
    await layer.stop().catch(() => undefined);
    throw new Error('transport started without binding a port');
  }
  if (typeof control.port !== 'number') {
    await control.stop(true);
    await layer.stop().catch(() => undefined);
    throw new Error('control endpoint started without binding a port');
  }
  write(`portal: listening on ws://127.0.0.1:${boundPort}/`);
  write(`portal: control on http://127.0.0.1:${control.port}/switch`);

  await lifetime.promise;
}

try {
  await main();
} catch (error) {
  writeError(`portal failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
