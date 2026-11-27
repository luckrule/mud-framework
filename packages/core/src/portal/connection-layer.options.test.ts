import { describe, expect, test } from 'bun:test';

import { scriptedExecutor } from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import type { Transport } from '../transport/types.ts';
import {
  ConnectionLayer,
  UNDECLARED_EVENT_SEND_TIMEOUT_MS,
} from './connection-layer.ts';
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

/**
 * Reads the resolved event write bound. The value is private because nothing
 * outside the delivery path may set it, but the resolution rules (explicit
 * option, then transport declaration, then default) are exactly what these
 * tests pin down.
 */
function eventSendTimeoutOf(layer: unknown): number {
  return (layer as { eventSendTimeoutMs: number }).eventSendTimeoutMs;
}

function layerWithTransport(transport: Transport): ConnectionLayer {
  return new ConnectionLayer({
    transport,
    executor: scriptedExecutor([]).executor,
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

  test('rejects maxQueuedEvents that is not a positive integer, and accepts 1', () => {
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
      const call = () => createLayer({ maxQueuedEvents: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/maxQueuedEvents/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ maxQueuedEvents: 1 })).not.toThrow();
  });

  test('rejects maxQueuedEventBytes that is not a positive integer, and accepts 1', () => {
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
      const call = () => createLayer({ maxQueuedEventBytes: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/maxQueuedEventBytes/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ maxQueuedEventBytes: 1 })).not.toThrow();
  });

  test('rejects eventBacklogWarnBytes that is not a positive integer, and accepts 1', () => {
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
        createLayer({ eventBacklogWarnBytes: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/eventBacklogWarnBytes/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ eventBacklogWarnBytes: 1 })).not.toThrow();
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

  test('rejects eventSendTimeoutMs below 1, NaN or Infinity, and accepts 1', () => {
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
      const call = () => createLayer({ eventSendTimeoutMs: value as number });
      expect(call).toThrow(Expected);
      expect(call).toThrow(/eventSendTimeoutMs/);
      expect(call).toThrow(new RegExp(String(value)));
    }
    expect(() => createLayer({ eventSendTimeoutMs: 1 })).not.toThrow();
  });
});

describe('ConnectionLayer event send bound resolution', () => {
  test('takes the transport declared sendTimeoutMs when no option is set', () => {
    const layer = layerWithTransport(
      new FakeTransport({ sendTimeoutMs: 60_000 }),
    );

    expect(eventSendTimeoutOf(layer)).toBe(60_000);
  });

  test('prefers the explicit layer option over the transport declaration', () => {
    const layer = new ConnectionLayer({
      transport: new FakeTransport({ sendTimeoutMs: 60_000 }),
      executor: scriptedExecutor([]).executor,
      eventSendTimeoutMs: 25,
    });

    expect(eventSendTimeoutOf(layer)).toBe(25);
  });

  test('ignores an Infinity declaration: an unbounded wait would pin the serial slot', () => {
    const layer = layerWithTransport(
      new FakeTransport({ sendTimeoutMs: Number.POSITIVE_INFINITY }),
    );

    // WsTransport accepts Infinity to disable its own send timeout, so the
    // layer must not adopt it as the event bound.
    expect(eventSendTimeoutOf(layer)).toBe(UNDECLARED_EVENT_SEND_TIMEOUT_MS);
  });

  test('ignores an unusable declaration (0, negative, NaN)', () => {
    for (const declared of [0, -1, Number.NaN]) {
      const layer = layerWithTransport(
        new FakeTransport({ sendTimeoutMs: declared }),
      );
      expect(eventSendTimeoutOf(layer)).toBe(UNDECLARED_EVENT_SEND_TIMEOUT_MS);
    }
  });

  test('falls back to the undeclared bound when the transport declares nothing', () => {
    expect(eventSendTimeoutOf(createLayer())).toBe(
      UNDECLARED_EVENT_SEND_TIMEOUT_MS,
    );
  });
});
