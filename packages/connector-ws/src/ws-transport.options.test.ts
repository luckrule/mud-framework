import { describe, expect, test } from 'bun:test';

import { WsTransport } from './ws-transport.ts';
import type { WsTransportOptions } from './ws-transport.ts';

function createTransport(options: WsTransportOptions = {}): WsTransport {
  return new WsTransport(options);
}

type InvalidOption = readonly [
  value: unknown,
  Expected: typeof RangeError | typeof TypeError,
];

type FieldCase = readonly [
  name: string,
  invalid: readonly InvalidOption[],
  accepted: readonly unknown[],
  options: (value: unknown) => WsTransportOptions,
];

/**
 * One row per numeric option: the accepted values are the boundaries the field
 * allows, and every rejected value keeps its own error type.
 */
const FIELD_CASES: readonly FieldCase[] = [
  [
    'port',
    [
      [-1, RangeError],
      [65_536, RangeError],
      [1.5, RangeError],
      [Number.NaN, RangeError],
      [Number.POSITIVE_INFINITY, RangeError],
      ['80', TypeError],
    ],
    [0],
    (value) => ({ port: value as number }),
  ],
  [
    'maxMessageBytes',
    [
      [0, RangeError],
      [-1, RangeError],
      [1.5, RangeError],
      [Number.NaN, RangeError],
      [Number.POSITIVE_INFINITY, RangeError],
      ['8', TypeError],
    ],
    [1],
    (value) => ({ maxMessageBytes: value as number }),
  ],
  [
    'idleTimeoutSeconds',
    [
      [-1, RangeError],
      [1.5, RangeError],
      [Number.NaN, RangeError],
      [Number.POSITIVE_INFINITY, RangeError],
      ['30', TypeError],
    ],
    [0],
    (value) => ({ idleTimeoutSeconds: value as number }),
  ],
  [
    'sendTimeoutMs',
    [
      [0, RangeError],
      [-1, RangeError],
      [Number.NaN, RangeError],
      [Number.NEGATIVE_INFINITY, RangeError],
      ['1000', TypeError],
    ],
    [Number.POSITIVE_INFINITY, 1],
    (value) => ({ sendTimeoutMs: value as number }),
  ],
  [
    'maxQueuedFrames',
    [
      [0, RangeError],
      [-1, RangeError],
      [1.5, RangeError],
      [Number.NaN, RangeError],
      [Number.POSITIVE_INFINITY, RangeError],
      [Number.NEGATIVE_INFINITY, RangeError],
      ['1', TypeError],
    ],
    [1],
    (value) => ({ maxQueuedFrames: value as number }),
  ],
  [
    'maxQueuedBytes',
    [
      [0, RangeError],
      [-1, RangeError],
      [1.5, RangeError],
      [Number.NaN, RangeError],
      [Number.POSITIVE_INFINITY, RangeError],
      [Number.NEGATIVE_INFINITY, RangeError],
      ['1', TypeError],
    ],
    // At least maxMessageBytes: a smaller queue cannot hold one frame.
    [1024 * 1024],
    (value) => ({ maxQueuedBytes: value as number }),
  ],
];

describe('WsTransport option validation', () => {
  test.each(FIELD_CASES)(
    'rejects an invalid %s and accepts a legal value',
    (name, invalid, accepted, options) => {
      for (const [value, Expected] of invalid) {
        const call = () => createTransport(options(value));
        expect(call).toThrow(Expected);
        expect(call).toThrow(new RegExp(name));
        expect(call).toThrow(new RegExp(String(value)));
      }
      for (const value of accepted) {
        expect(() => createTransport(options(value))).not.toThrow();
      }
    },
  );

  test('exposes the validated send bound so the layer can honour it', () => {
    // SPEC 3.8: the connection layer holds a Session's serial slot at least as
    // long as this transport may take to fail a write, so the exposed value
    // must be the one the send queue actually uses.
    expect(createTransport({ sendTimeoutMs: 60_000 }).sendTimeoutMs).toBe(
      60_000,
    );
    expect(
      createTransport({ sendTimeoutMs: Number.POSITIVE_INFINITY })
        .sendTimeoutMs,
    ).toBe(Number.POSITIVE_INFINITY);
    expect(createTransport().sendTimeoutMs).toBe(30_000);
  });

  test('rejects a queue smaller than one legal maximum-size frame', () => {
    const tooSmall = { maxMessageBytes: 1024, maxQueuedBytes: 1023 };
    expect(() => createTransport(tooSmall)).toThrow(RangeError);
    expect(() => createTransport(tooSmall)).toThrow(/maxQueuedBytes/);
    expect(() => createTransport(tooSmall)).toThrow(/maxMessageBytes/);
    expect(() => createTransport(tooSmall)).toThrow(/1023/);
    expect(() => createTransport(tooSmall)).toThrow(/1024/);
    // Equal is the smallest queue that can hold a maximum-size frame.
    expect(() =>
      createTransport({ maxMessageBytes: 1024, maxQueuedBytes: 1024 }),
    ).not.toThrow();
  });

  test('raises the default queue to a maxMessageBytes above 4 MiB', () => {
    // The default is the larger of 4 MiB and maxMessageBytes, so a caller that
    // allows bigger frames does not get a queue that rejects them.
    const big = 8 * 1024 * 1024;
    expect(() => createTransport({ maxMessageBytes: big })).not.toThrow();
    // An explicit queue below the message ceiling is still refused.
    expect(() =>
      createTransport({ maxMessageBytes: big, maxQueuedBytes: big - 1 }),
    ).toThrow(RangeError);
  });

  test('rejects an empty hostname or path, and a path without a leading slash', () => {
    expect(() => createTransport({ hostname: '' })).toThrow(RangeError);
    expect(() => createTransport({ hostname: '' })).toThrow(/hostname/);
    expect(() => createTransport({ hostname: 0 as unknown as string })).toThrow(
      TypeError,
    );
    expect(() => createTransport({ path: '' })).toThrow(RangeError);
    expect(() => createTransport({ path: 'ws' })).toThrow(RangeError);
    expect(() => createTransport({ path: 'ws' })).toThrow(/path/);
    expect(() => createTransport({ path: 'ws' })).toThrow(/"ws"/);
    expect(() => createTransport({ path: 0 as unknown as string })).toThrow(
      TypeError,
    );
    expect(() =>
      createTransport({ hostname: 'localhost', path: '/' }),
    ).not.toThrow();
    expect(() =>
      createTransport({ hostname: '0.0.0.0', path: '/ws' }),
    ).not.toThrow();
  });
});
