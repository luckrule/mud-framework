import { fileURLToPath } from 'node:url';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';
import type { SpawnedServer } from 'mud-framework-core';

import type { GameVersion } from './game.ts';
import { serverPidOf } from './portal-log.ts';

/**
 * The connection-holding process: it owns the WebSocket listener and spawns
 * the Server, which owns all storage writes. `PORTAL_PORT=0` asks the OS for a
 * port; the actual address is printed so tests and the demo can parse it.
 *
 * With `GAME_SLICE_CONTROL=1`, a second loopback-only HTTP listener exposes
 * `POST /switch` for the hot-switch drill. It is off by default so a Portal
 * reused by another run cannot be switched by any local process; it is not
 * part of the framework and cannot reach beyond 127.0.0.1. The control port is
 * OS-assigned so parallel runs cannot collide.
 */
const DEFAULT_PORTAL_PORT = 8080;
/** The Server a fresh Portal starts; a switch replaces it. */
const INITIAL_VERSION: GameVersion = 'v1';
const CONTROL_ENABLED = process.env.GAME_SLICE_CONTROL === '1';
const SHUTDOWN_SIGNALS = ['SIGINT', 'SIGTERM'] as const;
const SERVER_ENTRY_OVERRIDE = process.env.GAME_SLICE_SERVER_ENTRY?.trim();
const SERVER_ENTRY =
  SERVER_ENTRY_OVERRIDE === undefined || SERVER_ENTRY_OVERRIDE.length === 0
    ? fileURLToPath(new URL('./server.ts', import.meta.url))
    : SERVER_ENTRY_OVERRIDE;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

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
  try {
    process.stdout.write(`${line}\n`);
  } catch {
    // The stdout error listener owns the shutdown for a broken pipe.
  }
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

/** The optional body of `POST /switch`; anything else is a 400. */
function readVersion(body: unknown): GameVersion | undefined {
  if (!isRecord(body)) {
    return undefined;
  }
  const version = body.version;
  return version === 'v1' || version === 'v2' ? version : undefined;
}

function elapsedMs(startedAt: number): number {
  return Number(((Bun.nanoseconds() - startedAt) / 1e6).toFixed(1));
}

function spawnServer(version: GameVersion): () => SpawnedServer {
  return () =>
    spawnBunServer({
      // The Server is a sibling file resolved from this module, so the example
      // runs from any working directory. The version is argv, not env: a
      // `switchTo` spawn inherits the Portal's env and could not differ there.
      command: [process.execPath, 'run', SERVER_ENTRY, version],
    });
}

async function main(): Promise<void> {
  const portalPort = readPort('PORTAL_PORT', DEFAULT_PORTAL_PORT);

  // The default executor logger is the same JSON sink the rest of the Portal
  // writes; tapping it keeps the log lines identical while remembering the pid
  // the control endpoint hands back to the drill.
  const serverPid: { current: number | undefined } = { current: undefined };
  const logger = createJsonLogger({
    bindings: { component: 'ipc-executor' },
    sink: (line) => {
      void Bun.stdout.write(`${line}\n`).catch(() => undefined);
      const pid = serverPidOf(line);
      if (pid !== undefined) {
        serverPid.current = pid;
      }
    },
  });

  const executor = new IpcExecutor({
    spawn: spawnServer(INITIAL_VERSION),
    logger,
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

  // One switch at a time: a second caller must be refused instead of racing
  // the first through `switchTo`.
  let switching = false;
  const control = CONTROL_ENABLED
    ? Bun.serve({
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
          let body: unknown;
          try {
            body = await request.json();
          } catch {
            return json(400, { ok: false, error: 'body must be JSON' });
          }
          const version = readVersion(body);
          if (version === undefined) {
            return json(400, {
              ok: false,
              error: 'body must be {"version":"v1"|"v2"}',
            });
          }
          if (switching) {
            return json(409, {
              ok: false,
              error: 'a switch is already running',
            });
          }
          switching = true;
          const fromPid = serverPid.current;
          const startedAt = Bun.nanoseconds();
          try {
            await executor.switchTo({ spawn: spawnServer(version) });
            return json(200, {
              ok: true,
              durationMs: elapsedMs(startedAt),
              fromPid,
              toPid: serverPid.current,
            });
          } catch (error) {
            return json(500, {
              ok: false,
              durationMs: elapsedMs(startedAt),
              fromPid,
              toPid: serverPid.current,
              error: messageOf(error),
            });
          } finally {
            switching = false;
          }
        },
      })
    : undefined;

  let stopping = false;
  let closeFailed = false;
  const lifetime = Promise.withResolvers<void>();
  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    write(`portal: shutting down (${signal})`);
    let failed = false;
    if (control !== undefined) {
      // A failing control close must not skip the layer stop: the Server and
      // the listener still have to be released.
      try {
        await control.stop(true);
      } catch (error) {
        failed = true;
        writeError(`portal: control stop failed: ${messageOf(error)}`);
      }
    }
    try {
      await layer.stop();
    } catch (error) {
      failed = true;
      writeError(`portal: shutdown failed: ${messageOf(error)}`);
    } finally {
      process.exitCode = failed || closeFailed ? 1 : 0;
      lifetime.resolve();
    }
  };

  // A closed stdout pipe (the parent died or stopped reading) must not leave
  // the Portal running orphaned with its Server still writing storage. The
  // diagnostic goes to stderr because stdout is the stream that broke.
  process.stdout.on('error', () => {
    closeFailed = true;
    writeError('portal: stdout closed; shutting down');
    void shutdown('SIGTERM');
  });

  for (const signal of SHUTDOWN_SIGNALS) {
    process.once(signal, () => {
      void shutdown(signal);
    });
  }

  // Every startup failure, including a broken stdout on the listening line,
  // releases both listeners before the error reaches the top-level catch.
  try {
    await layer.start();
    const boundPort = transport.port;
    if (boundPort === undefined) {
      throw new Error('transport started without binding a port');
    }
    if (control !== undefined && typeof control.port !== 'number') {
      throw new Error('control endpoint started without binding a port');
    }
    write(`portal: listening on ws://127.0.0.1:${boundPort}/`);
    if (control !== undefined && typeof control.port === 'number') {
      write(`portal: control on http://127.0.0.1:${control.port}/switch`);
    }
  } catch (error) {
    if (control !== undefined) {
      await control.stop(true).catch(() => undefined);
    }
    await layer.stop().catch(() => undefined);
    throw error;
  }

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
