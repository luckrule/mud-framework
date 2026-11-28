import { describe, expect, test } from 'bun:test';

import { planGenerators, senderSessionIds } from './orchestrator.ts';
import {
  Histogram,
  goStartMsOf,
  measurementWindow,
  parseLoadResultJson,
  parseSenderCount,
  senderClientsFor,
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

function oldResultWithoutSenders(): Record<string, unknown> {
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
