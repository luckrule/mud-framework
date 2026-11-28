import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  createJsonLogger,
  createProcessChannel,
  runServerProcess,
} from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

import { gameExtension } from '../../../examples/game-slice/src/game.ts';
import { initializeWorld } from '../../../examples/game-slice/src/world.ts';
import { messageOf } from '../../../packages/core/src/shared/validation.ts';
import {
  buildServerStats,
  createCountingChannel,
  createRecordingLogger,
  scanSenderSessions,
  writeServerStats,
} from './server-stats.ts';
import type { MutableHandlerStats, MutableIpcStats } from './server-stats.ts';
import { Histogram } from './support.ts';

/**
 * Benchmark-local Server entry: the game slice with the real process channel
 * wrapped in frame/byte counters and a logger that records sender handler
 * durations. The Portal selects it through `GAME_SLICE_SERVER_ENTRY`, so the
 * example Portal is reused instead of copied.
 */

const DEFAULT_DATA_DIRECTORY = fileURLToPath(
  new URL('../../../examples/game-slice/data/', import.meta.url),
);
const DATABASE_FILENAME = 'game.sqlite';

function dataDirectory(): string {
  const override = process.env.GAME_SLICE_DATA_DIR;
  return override !== undefined && override.length > 0
    ? override
    : DEFAULT_DATA_DIRECTORY;
}

async function openBackend(): Promise<SqliteBackend> {
  const directory = dataDirectory();
  mkdirSync(directory, { recursive: true });
  const backend = SqliteBackend.open({
    filename: join(directory, DATABASE_FILENAME),
  });
  try {
    await initializeWorld(backend);
  } catch (error) {
    await backend.close().catch(() => undefined);
    throw error;
  }
  return backend;
}

const ipcStats: MutableIpcStats = {
  outboundFrames: 0,
  outboundBytes: 0,
  inboundFrames: 0,
  inboundBytes: 0,
};
const handlerStats: MutableHandlerStats = { completed: 0, failed: 0 };
const durationHistogram = new Histogram();
const statsFile = process.env.BENCH_SERVER_STATS?.trim();
const senders = scanSenderSessions(process.env.BENCH_SENDER_SESSIONS);

const logger = createRecordingLogger(
  createJsonLogger({ bindings: { component: 'bench-server' } }),
  senders,
  handlerStats,
  durationHistogram,
);
const channel = createCountingChannel(
  createProcessChannel({ logger }),
  ipcStats,
);

try {
  await runServerProcess({
    createBackend: openBackend,
    extensions: [gameExtension()],
    timeoutMs: 5000,
    logger,
    channel,
  });
} catch (error) {
  process.stderr.write(
    `bench server stopped abnormally: ${messageOf(error)}\n`,
  );
  process.exitCode = 1;
}

if (statsFile !== undefined && statsFile.length > 0) {
  try {
    writeServerStats(
      statsFile,
      buildServerStats(ipcStats, handlerStats, durationHistogram),
    );
  } catch (error) {
    process.stderr.write(
      `bench server could not write ${statsFile}: ${messageOf(error)}\n`,
    );
    process.exitCode = 1;
  }
}
