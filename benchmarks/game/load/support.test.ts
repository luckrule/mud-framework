import { describe, expect, test } from 'bun:test';

import {
  loadPortalExtraEnv,
  planGenerators,
  senderSessionIds,
} from './orchestrator.ts';
import {
  EVENT_WINDOW_MS,
  Histogram,
  eventWindowIndex,
  goStartMsOf,
  maxEventWindowIndex,
  measurementWindow,
  parseLoadResultJson,
  parseSenderCount,
  parseServerStatsEnabled,
  readMainConfig,
  senderClientsFor,
  serverStatsLabel,
} from './support.ts';
import type { MainConfig } from './support.ts';

describe('sender allocation', () => {
  test('LOAD_SENDERS defaults to every client', () => {
    expect(parseSenderCount(50, undefined)).toBe(50);
    expect(parseSenderCount(50, '')).toBe(50);
    expect(parseSenderCount(50, '  ')).toBe(50);
  });

  test('LOAD_SENDERS reads an explicit count', () => {
    expect(parseSenderCount(50, '1')).toBe(1);
    expect(parseSenderCount(50, ' 7 ')).toBe(7);
  });

  test('LOAD_SENDERS rejects zero, non-integers and more senders than clients', () => {
    expect(() => parseSenderCount(10, '0')).toThrow('LOAD_SENDERS');
    expect(() => parseSenderCount(10, '1.5')).toThrow('LOAD_SENDERS');
    expect(() => parseSenderCount(10, 'abc')).toThrow('LOAD_SENDERS');
    expect(() => parseSenderCount(10, '-1')).toThrow('LOAD_SENDERS');
    expect(() => parseSenderCount(10, '11')).toThrow(
      'must not exceed LOAD_CLIENTS',
    );
  });

  test('sender clients are the first N global clients across generators', () => {
    // Generators at offsets 0/4/7 with 4/3/3 clients; 6 senders in total.
    expect(senderClientsFor(0, 4, 6)).toBe(4);
    expect(senderClientsFor(4, 3, 6)).toBe(2);
    expect(senderClientsFor(7, 3, 6)).toBe(0);
  });

  test('a generator before the sender boundary keeps all its clients as senders', () => {
    expect(senderClientsFor(0, 3, 100)).toBe(3);
    expect(senderClientsFor(3, 3, 3)).toBe(0);
    expect(senderClientsFor(0, 0, 5)).toBe(0);
  });
});

function loadConfig(clients: number, senders: number): MainConfig {
  return {
    shared: {
      roomSize: 10,
      durationSec: 1,
      warmupSec: 0,
      thinkMs: 0,
      mode: 'saturate',
      mix: [{ command: 'look', weight: 1 }],
      requestTimeoutMs: 1000,
      runTag: 'test',
      externalUrl: undefined,
    },
    clients,
    senders,
    generators: 3,
    dataDirectory: undefined,
    keep: false,
    serverStats: true,
  };
}

describe('generator plans', () => {
  test('allocates senders by global order across generator boundaries', () => {
    const plans = planGenerators(loadConfig(10, 6), '/tmp/load-results');

    expect(
      plans.map((plan) => [
        plan.index,
        plan.clients,
        plan.senders,
        plan.offset,
      ]),
    ).toEqual([
      [0, 4, 4, 0],
      [1, 3, 2, 4],
      [2, 3, 0, 7],
    ]);
    expect(senderSessionIds(plans)).toEqual([
      'player:load-0-0',
      'player:load-0-1',
      'player:load-0-2',
      'player:load-0-3',
      'player:load-1-0',
      'player:load-1-1',
    ]);
  });
});

function emptyResultHistogram(): unknown {
  return new Histogram().toJson();
}

interface LoadResultFixture extends Record<string, unknown> {
  readonly events: Record<string, unknown>;
}

function oldResultWithoutSenders(): LoadResultFixture {
  const histogram = emptyResultHistogram();
  const command = (): unknown => ({ count: 0, errors: 0, histogram });
  return {
    version: 1,
    generator: 0,
    clients: 2,
    setupMs: 5,
    measuredFromMs: 10,
    measuredUntilMs: 20,
    commands: {
      look: command(),
      who: command(),
      move: command(),
      say: command(),
    },
    events: {
      saysAttempted: 0,
      saysSent: 0,
      saysFailed: 0,
      received: 0,
      gaps: 0,
      duplicates: 0,
      staleRoom: 0,
      lateEpoch: 0,
      unparsed: 0,
      histogram,
    },
    errors: {},
    rooms: { square: 2 },
  };
}

describe('load result parsing', () => {
  test('a result without senders is read as an all-sender run', () => {
    const parsed = parseLoadResultJson(oldResultWithoutSenders());

    expect(parsed.senders).toBe(2);
  });

  test('an explicit sender count is preserved', () => {
    const result = { ...oldResultWithoutSenders(), senders: 1 };
    expect(parseLoadResultJson(result).senders).toBe(1);
  });

  test('a sender count above clients is rejected', () => {
    const result = { ...oldResultWithoutSenders(), senders: 3 };
    expect(() => parseLoadResultJson(result)).toThrow(
      'senders (3) exceeds clients (2)',
    );
  });

  test('a result without windows stays compatible', () => {
    const parsed = parseLoadResultJson(oldResultWithoutSenders());

    expect(parsed.events.windows).toBeUndefined();
  });

  test('a window inside the measurement window is accepted', () => {
    const result = oldResultWithoutSenders();
    const fromMs = 10;
    const parsed = parseLoadResultJson({
      ...result,
      events: {
        ...result.events,
        windows: [
          {
            index: 0,
            fromMs,
            untilMs: fromMs + EVENT_WINDOW_MS,
            histogram: emptyResultHistogram(),
          },
        ],
      },
    });

    expect(parsed.events.windows?.[0]?.index).toBe(0);
  });

  test('the endpoint bucket is allowed but a larger index is rejected', () => {
    const result = oldResultWithoutSenders();
    const at = (index: number): unknown => ({
      ...result,
      events: {
        ...result.events,
        windows: [
          {
            index,
            fromMs: 10 + index * EVENT_WINDOW_MS,
            untilMs: 10 + (index + 1) * EVENT_WINDOW_MS,
            histogram: emptyResultHistogram(),
          },
        ],
      },
    });

    expect(() => parseLoadResultJson(at(1))).not.toThrow();
    expect(() => parseLoadResultJson(at(2))).toThrow('exceeds');
    expect(() => parseLoadResultJson(at(100_000))).toThrow('exceeds');
  });

  test('a window with misaligned bounds is rejected', () => {
    const result = oldResultWithoutSenders();
    expect(() =>
      parseLoadResultJson({
        ...result,
        events: {
          ...result.events,
          windows: [
            {
              index: 0,
              fromMs: 11,
              untilMs: 11 + EVENT_WINDOW_MS,
              histogram: emptyResultHistogram(),
            },
          ],
        },
      }),
    ).toThrow('expected');
  });
});

describe('LOAD_SERVER_STATS', () => {
  test('defaults on only when unset', () => {
    expect(parseServerStatsEnabled(undefined)).toBe(true);
  });

  test('rejects empty and whitespace values', () => {
    for (const raw of ['', '  ', '\t']) {
      expect(() => parseServerStatsEnabled(raw)).toThrow('LOAD_SERVER_STATS');
    }
  });

  test('accepts the documented on and off spellings, case-insensitively', () => {
    for (const raw of ['true', '1', 'on', 'TRUE', 'On']) {
      expect(parseServerStatsEnabled(raw)).toBe(true);
    }
    for (const raw of ['false', '0', 'off', 'FALSE', 'Off']) {
      expect(parseServerStatsEnabled(raw)).toBe(false);
    }
  });

  test('rejects any other value instead of silently flipping the mode', () => {
    for (const raw of ['yes', 'no', '2', 'enabled', 'offf']) {
      expect(() => parseServerStatsEnabled(raw)).toThrow('LOAD_SERVER_STATS');
    }
  });

  test('readMainConfig wires the flag and rejects invalid values', () => {
    const previous = process.env.LOAD_SERVER_STATS;
    try {
      delete process.env.LOAD_SERVER_STATS;
      expect(readMainConfig().serverStats).toBe(true);
      process.env.LOAD_SERVER_STATS = 'off';
      expect(readMainConfig().serverStats).toBe(false);
      process.env.LOAD_SERVER_STATS = 'ON';
      expect(readMainConfig().serverStats).toBe(true);
      process.env.LOAD_SERVER_STATS = '';
      expect(() => readMainConfig()).toThrow('LOAD_SERVER_STATS');
      process.env.LOAD_SERVER_STATS = 'bogus';
      expect(() => readMainConfig()).toThrow('LOAD_SERVER_STATS');
    } finally {
      if (previous === undefined) {
        delete process.env.LOAD_SERVER_STATS;
      } else {
        process.env.LOAD_SERVER_STATS = previous;
      }
    }
  });

  test('the default forwards the counting entry and stats variables', () => {
    const env = loadPortalExtraEnv({
      serverStats: true,
      serverStatsFile: '/tmp/server-stats.json',
      senderSessions: ['player:load-0-0', 'player:load-0-1'],
    });

    expect(env.GAME_SLICE_SERVER_ENTRY?.endsWith('/bench-server.ts')).toBe(
      true,
    );
    expect(env.BENCH_SERVER_STATS).toBe('/tmp/server-stats.json');
    expect(env.BENCH_SENDER_SESSIONS).toBe('player:load-0-0,player:load-0-1');
  });

  test('off blanks the three benchmark Server variables', () => {
    const env = loadPortalExtraEnv({ serverStats: false });

    expect(env).toEqual({
      GAME_SLICE_SERVER_ENTRY: '',
      BENCH_SERVER_STATS: '',
      BENCH_SENDER_SESSIONS: '',
    });
  });

  test('off overrides values inherited from the parent process', () => {
    const keys = [
      'GAME_SLICE_SERVER_ENTRY',
      'BENCH_SERVER_STATS',
      'BENCH_SENDER_SESSIONS',
    ] as const;
    const previous = new Map(
      keys.map((key) => [key, process.env[key]] as const),
    );
    try {
      process.env.GAME_SLICE_SERVER_ENTRY = '/tmp/parent-bench-server.ts';
      process.env.BENCH_SERVER_STATS = '/tmp/parent-stats.json';
      process.env.BENCH_SENDER_SESSIONS = 'player:load-9-9';

      const env = {
        ...process.env,
        ...loadPortalExtraEnv({ serverStats: false }),
      };
      for (const key of keys) {
        expect(env[key]).toBe('');
      }
    } finally {
      for (const key of keys) {
        const value = previous.get(key);
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }
    }
  });

  test('the header does not claim server stats for an external Portal', () => {
    expect(serverStatsLabel(undefined, true)).toBe('on');
    expect(serverStatsLabel(undefined, false)).toBe('off');
    expect(serverStatsLabel('ws://127.0.0.1:1/', true)).toBe(
      'n/a (external portal)',
    );
    expect(serverStatsLabel('ws://127.0.0.1:1/', false)).toBe(
      'n/a (external portal)',
    );
  });
});

describe('event latency windows', () => {
  const base = 1_700_000_000_000;

  test('uses fixed 5-minute buckets from the measurement start', () => {
    expect(EVENT_WINDOW_MS).toBe(300_000);
    expect(eventWindowIndex(base, base)).toBe(0);
    expect(eventWindowIndex(base + 299_999, base)).toBe(0);
    expect(eventWindowIndex(base + 300_000, base)).toBe(1);
    expect(eventWindowIndex(base + 600_000, base)).toBe(2);
  });

  test('a short run keeps a single window', () => {
    expect(eventWindowIndex(base + 299_999, base)).toBe(0);
  });

  test('a timestamp before the measurement start is clamped to window 0', () => {
    expect(eventWindowIndex(base - 1, base)).toBe(0);
  });

  test('the allowed index covers the endpoint bucket only', () => {
    const fromMs = 0;
    expect(maxEventWindowIndex(fromMs, 0)).toBe(0);
    expect(maxEventWindowIndex(fromMs, 1)).toBe(1);
    expect(maxEventWindowIndex(fromMs, 300_000)).toBe(1);
    expect(maxEventWindowIndex(fromMs, 300_001)).toBe(2);
    expect(maxEventWindowIndex(fromMs, 600_000)).toBe(2);
  });
});

describe('measurement window', () => {
  test('the shared go timestamp wins over each generator receipt time', () => {
    const line = 'go 1700000000000';
    const startMs = goStartMsOf(line);
    if (startMs === undefined) {
      throw new Error('test fixture did not parse');
    }
    // Two generators read the same stdin line 1 ms and 50 ms later. Neither
    // receipt time may enter the window.
    const windowForReceipt = (
      _receivedAtMs: number,
    ): ReturnType<typeof measurementWindow> => measurementWindow(startMs, 0, 5);

    expect(windowForReceipt(1_700_000_000_000)).toEqual(
      windowForReceipt(1_700_000_000_050),
    );
    expect(windowForReceipt(1_700_000_000_050)).toEqual({
      fromMs: 1_700_000_000_000,
      untilMs: 1_700_000_005_000,
    });
  });

  test('warmup shifts both window edges from the shared start', () => {
    expect(measurementWindow(1000, 3, 5)).toEqual({
      fromMs: 4000,
      untilMs: 9000,
    });
    expect(measurementWindow(1000, 0, 5)).toEqual({
      fromMs: 1000,
      untilMs: 6000,
    });
  });

  test('the go parser requires a non-negative timestamp', () => {
    expect(goStartMsOf('go 123')).toBe(123);
    expect(goStartMsOf('  go 123  ')).toBe(123);
    expect(goStartMsOf('go')).toBeUndefined();
    expect(goStartMsOf('go abc')).toBeUndefined();
    expect(goStartMsOf('go -1')).toBeUndefined();
  });
});
