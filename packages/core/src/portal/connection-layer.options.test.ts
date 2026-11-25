import { describe, expect, test } from 'bun:test';

import { scriptedExecutor } from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import { ConnectionLayer } from './connection-layer.ts';
import type { ConnectionLayerOptions } from './connection-layer.ts';

function createLayer(
  options: Partial<ConnectionLayerOptions> = {},
): ConnectionLayer {
  return new ConnectionLayer({
    transport: new FakeTransport(),
    executor: scriptedExecutor([]).executor,
    ...options,
  });
}

describe('ConnectionLayer option validation', () => {
  test('rejects maxConnections that is not a positive integer', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['10', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () => createLayer({ maxConnections: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/maxConnections/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ maxConnections: 1 })).not.toThrow();
  });

  test('rejects maxQueuedPerConnection that is not a positive integer, and accepts 1', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['5', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () =>
        createLayer({ maxQueuedPerConnection: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/maxQueuedPerConnection/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ maxQueuedPerConnection: 1 })).not.toThrow();
  });

  test('rejects shutdownTimeoutMs below 1, NaN or Infinity, and accepts 1', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [0.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['100', TypeError],
      ];
    for (const [value, Expected] of invalid) {
      const call = () => createLayer({ shutdownTimeoutMs: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/shutdownTimeoutMs/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ shutdownTimeoutMs: 1 })).not.toThrow();
  });
});
