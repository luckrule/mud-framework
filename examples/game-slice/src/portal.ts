import { fileURLToPath } from 'node:url';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';

/**
 * The connection-holding process: it owns the WebSocket listener and spawns
 * the Server, which owns all storage writes. `PORTAL_PORT=0` asks the OS for a
 * port; the actual address is printed so tests and the demo can parse it.
 */
const DEFAULT_PORTAL_PORT = 8080;
const SHUTDOWN_SIGNALS = ['SIGINT', 'SIGTERM'] as const;
const SERVER_ENTRY_OVERRIDE = process.env.GAME_SLICE_SERVER_ENTRY?.trim();
const SERVER_ENTRY =
  SERVER_ENTRY_OVERRIDE === undefined || SERVER_ENTRY_OVERRIDE.length === 0
    ? fileURLToPath(new URL('./server.ts', import.meta.url))
    : SERVER_ENTRY_OVERRIDE;

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

async function main(): Promise<void> {
  const portalPort = readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT);

  const executor = new IpcExecutor({
    // The Server is a sibling file resolved from this module, so the example
    // runs from any working directory.
    spawn: () =>
      spawnBunServer({
        command: [process.execPath, 'run', SERVER_ENTRY],
      }),
    timeoutMs: 5000,
    startTimeoutMs: 10_000,
  });
  const transport = new WsTransport({
    port: portalPort,
    hostname: '127.0.0.1',
  });
  const layer = new ConnectionLayer({
    transport,
    executor,
    // Bounds each shutdown step; `layer.stop()` itself never waits forever.
    shutdownTimeoutMs: 5000,
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

  await layer.start();
  const boundPort = transport.port;
  if (boundPort === undefined) {
    await layer.stop().catch(() => undefined);
    throw new Error('transport started without binding a port');
  }
  write(`portal: listening on ws://127.0.0.1:${boundPort}/`);

  // The listener and the IPC link hold the event loop open. Shutdown closes
  // them and then resolves this wait, so the process exits with the code the
  // shutdown path set.
  await lifetime.promise;
}

try {
  await main();
} catch (error) {
  writeError(`portal failed: ${messageOf(error)}`);
  process.exitCode = 1;
}
