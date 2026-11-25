import { fileURLToPath } from 'node:url';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';

const DEFAULT_PORTAL_PORT = 8080;
const DEFAULT_HEALTH_PORT = 8081;
const SHUTDOWN_SIGNALS = ['SIGINT', 'SIGTERM'] as const;

const SERVER_ENTRY = fileURLToPath(new URL('./server.ts', import.meta.url));

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function readPort(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1 || value > 65535) {
    throw new Error(
      `${name} must be an integer between 1 and 65535, got ${JSON.stringify(raw)}`,
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

async function main(): Promise<void> {
  const portalPort = readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT);
  const healthPort = readPort('HEALTH_PORT', DEFAULT_HEALTH_PORT);

  const executor = new IpcExecutor({
    // The Server is a sibling file, not a hard-coded deployment path; the
    // Portal resolves it from its own module URL so the example runs from any
    // working directory.
    spawn: () =>
      spawnBunServer({
        command: [process.execPath, 'run', SERVER_ENTRY],
      }),
    timeoutMs: 5000,
  });
  const transport = new WsTransport({ port: portalPort });
  const layer = new ConnectionLayer({ transport, executor });

  // The HTTP endpoint is application code: core exposes only the snapshot
  // (SPEC 6.6). Starting it before the layer means a probe during startup sees
  // 503 instead of a connection refusal.
  const health = Bun.serve({
    hostname: '127.0.0.1',
    port: healthPort,
    fetch: () => {
      const snapshot = layer.health();
      return Response.json(
        {
          status: snapshot.ready ? 'ok' : 'unavailable',
          health: snapshot,
        },
        { status: snapshot.ready ? 200 : 503 },
      );
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
      await layer.stop();
      await health.stop(true);
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
    await health.stop(true);
    throw error;
  }

  write(
    `portal: ws://127.0.0.1:${transport.port}/ health http://127.0.0.1:${healthPort}/`,
  );

  // The WebSocket listener and the health endpoint hold the event loop open.
  // The signal handler closes both and then resolves this wait, so the process
  // exits with the code the shutdown path set instead of being killed by the
  // signal's default action.
  await lifetime.promise;
}

try {
  await main();
} catch (error) {
  writeError(`portal failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
