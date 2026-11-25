import { describe, expect, test } from 'bun:test';

import { WsTransport } from './ws-transport.ts';
import type { WsTransportOptions } from './ws-transport.ts';

function createTransport(options: WsTransportOptions = {}): WsTransport {
  return new WsTransport(options);
}

describe('WsTransport option validation', () => {
  test('rejects port outside an integer 0..65535, and accepts 0', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [-1, RangeError],
        [65_536, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['80', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () => createTransport({ port: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/port/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createTransport({ port: 0 })).not.toThrow();
  });

  test('rejects maxMessageBytes that is not a positive integer', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['8', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () => createTransport({ maxMessageBytes: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/maxMessageBytes/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createTransport({ maxMessageBytes: 1 })).not.toThrow();
  });

  test('rejects idleTimeoutSeconds that is not a non-negative integer, and accepts 0', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [-1, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['30', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () =>
        createTransport({ idleTimeoutSeconds: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/idleTimeoutSeconds/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createTransport({ idleTimeoutSeconds: 0 })).not.toThrow();
  });

  test('rejects sendTimeoutMs that is not positive or Infinity, and accepts Infinity', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [Number.NaN, RangeError],
        [Number.NEGATIVE_INFINITY, RangeError],
        ['1000', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () => createTransport({ sendTimeoutMs: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/sendTimeoutMs/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() =>
      createTransport({ sendTimeoutMs: Number.POSITIVE_INFINITY }),
    ).not.toThrow();
    expect(() => createTransport({ sendTimeoutMs: 1 })).not.toThrow();
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
