import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { describe, expect, test } from 'bun:test';
import { createJsonLogger } from 'mud-framework-core';

import { createInMemoryChannelPair } from '../../../packages/core/src/ipc/channel.ts';
import {
  buildServerStats,
  createCountingChannel,
  createRecordingLogger,
  readServerStatsFile,
  writeServerStats,
} from './server-stats.ts';
import type { MutableHandlerStats, MutableIpcStats } from './server-stats.ts';
import { Histogram } from './support.ts';

const SENDER = 'player:load-0-0';

function loggerHarness(): {
  readonly logger: ReturnType<typeof createRecordingLogger>;
  readonly handler: MutableHandlerStats;
  readonly histogram: Histogram;
} {
  const handler: MutableHandlerStats = { completed: 0, failed: 0 };
  const histogram = new Histogram();
  const logger = createRecordingLogger(
    createJsonLogger({ sink: () => undefined }),
    new Set([SENDER]),
    handler,
    histogram,
  );
  return { logger, handler, histogram };
}

describe('recording logger', () => {
  test('records completed requests and failures at every level', () => {
    const { logger, handler, histogram } = loggerHarness();

    logger.debug({ sessionId: SENDER, durationMs: 3 }, 'request completed');
    logger.info({ sessionId: SENDER, durationMs: 1 }, 'request completed');
    // Failed requests use failureLogLevel(), which is warn or error.
    logger.warn({ sessionId: SENDER, durationMs: 2 }, 'request failed');
    logger.error({ sessionId: SENDER, durationMs: 1 }, 'request failed');

    expect(handler).toEqual({ completed: 2, failed: 2 });
    expect(histogram.count).toBe(2);
  });

  test('ignores requests from non-sender sessions', () => {
    const { logger, handler, histogram } = loggerHarness();

    logger.debug(
      { sessionId: 'player:load-9-9', durationMs: 1 },
      'request completed',
    );
    logger.warn(
      { sessionId: 'player:load-9-9', durationMs: 1 },
      'request failed',
    );

    expect(handler).toEqual({ completed: 0, failed: 0 });
    expect(histogram.count).toBe(0);
  });

  test('counts a login through the sessionChange it declared', () => {
    const { logger, handler, histogram } = loggerHarness();

    logger.debug(
      {
        sessionChange: { type: 'bind', sessionId: SENDER },
        durationMs: 0,
      },
      'request completed',
    );

    expect(handler).toEqual({ completed: 1, failed: 0 });
    expect(histogram.count).toBe(1);
  });
});

describe('counting channel', () => {
  test('counts frames and JSON bytes in both directions', () => {
    const [portal, server] = createInMemoryChannelPair();
    const ipc: MutableIpcStats = {
      outboundFrames: 0,
      outboundBytes: 0,
      inboundFrames: 0,
      inboundBytes: 0,
    };
    const counting = createCountingChannel(server, ipc);
    const received: unknown[] = [];
    counting.onMessage((message) => {
      received.push(message);
    });

    const hello = { type: 'hello' } as const;
    counting.send(hello);

    const request = {
      type: 'request',
      requestId: 'request-1',
      connectionId: 'connection-1',
      clientRequestId: 'client-1',
      method: 'game.look',
      payload: undefined,
      timeoutMs: 5000,
      replayCount: 0,
    } as const;
    portal.send(request);

    expect(ipc.outboundFrames).toBe(1);
    expect(ipc.outboundBytes).toBe(Buffer.byteLength(JSON.stringify(hello)));
    expect(ipc.inboundFrames).toBe(1);
    expect(ipc.inboundBytes).toBe(Buffer.byteLength(JSON.stringify(request)));
    expect(received).toEqual([request]);
  });
});

describe('server stats file', () => {
  test('writes and reads the same counters back', () => {
    const directory = mkdtempSync(join(tmpdir(), 'mud-server-stats-'));
    try {
      const histogram = new Histogram();
      histogram.record(1_500_000);
      const ipc: MutableIpcStats = {
        outboundFrames: 7,
        outboundBytes: 700,
        inboundFrames: 3,
        inboundBytes: 300,
      };
      const handler: MutableHandlerStats = { completed: 2, failed: 1 };
      const stats = buildServerStats(ipc, handler, histogram);
      const file = join(directory, 'server-stats.json');

      writeServerStats(file, stats);

      const read = readServerStatsFile(file);
      expect(read).toEqual({ ok: true, stats });
      expect(readServerStatsFile(join(directory, 'missing.json')).ok).toBe(
        false,
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
