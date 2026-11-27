import { describe, expect, test } from 'bun:test';

import { scriptedExecutor } from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import type { Transport } from '../transport/types.ts';
import {
  ConnectionLayer,
  EVENT_SEND_SLACK_MS,
  MAX_TIMER_DELAY_MS,
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
 * Reads the resolved downlink write bound. The value is private because nothing
 * outside the delivery and reply paths may set it, but the resolution rules
 * (explicit option, then transport declaration, then default) are exactly what
 * these tests pin down.
 */
function sendTimeoutOf(layer: unknown): number {
  return (layer as { sendTimeoutMs: number }).sendTimeoutMs;
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

  test('rejects shutdownTimeoutMs and eventSendTimeoutMs above the timer ceiling', () => {
    // Bun clamps a delay above 2^31-1 to 1ms, so an operator who asks for a
    // 24.8-day wait would get the shortest possible one: every queued request
    // dropped at shutdown, or a write bound that releases the writer at once.
    // Rejected rather than capped: the value is the operator's stated intent.
    const aboveCeiling = 2 ** 31;
    for (const [name, build] of [
      [
        'shutdownTimeoutMs',
        (value: number) => createLayer({ shutdownTimeoutMs: value }),
      ],
      [
        'eventSendTimeoutMs',
        (value: number) => createLayer({ eventSendTimeoutMs: value }),
      ],
    ] as const) {
      const call = () => build(aboveCeiling);
      expect(call).toThrow(RangeError);
      expect(call).toThrow(new RegExp(name));
      expect(call).toThrow(new RegExp(String(MAX_TIMER_DELAY_MS)));
    }
    // The largest usable delay is still accepted.
    expect(() =>
      createLayer({
        shutdownTimeoutMs: MAX_TIMER_DELAY_MS,
        eventSendTimeoutMs: MAX_TIMER_DELAY_MS,
      }),
    ).not.toThrow();
    expect(
      sendTimeoutOf(createLayer({ eventSendTimeoutMs: MAX_TIMER_DELAY_MS })),
    ).toBe(MAX_TIMER_DELAY_MS);
  });

  test('a normal delay is unaffected by the ceiling', () => {
    expect(sendTimeoutOf(createLayer({ eventSendTimeoutMs: 5000 }))).toBe(5000);
    expect(() => createLayer({ shutdownTimeoutMs: 5000 })).not.toThrow();
  });
});

describe('ConnectionLayer event send bound resolution', () => {
  test('bounds the write strictly wider than the transport declaration', () => {
    // The transport's own timeout is the verdict that explains a stalled write,
    // so this layer's fallback must outlast it rather than race it. Equal
    // bounds would let the less specific "did not settle" win (SPEC 3.8).
    const layer = layerWithTransport(
      new FakeTransport({ sendTimeoutMs: 60_000 }),
    );

    const bound = sendTimeoutOf(layer);
    expect(bound).toBeGreaterThan(60_000);
    expect(bound).toBe(60_000 + EVENT_SEND_SLACK_MS);
  });

  test('an explicit layer option replaces the transport derivation verbatim', () => {
    // The option is the layer's own bound, not a competitor to the transport's:
    // it takes no slack, so an operator who asks for 25ms gets 25ms.
    const layer = new ConnectionLayer({
      transport: new FakeTransport({ sendTimeoutMs: 60_000 }),
      executor: scriptedExecutor([]).executor,
      eventSendTimeoutMs: 25,
    });

    expect(sendTimeoutOf(layer)).toBe(25);
  });

  test('ignores an Infinity declaration: an unbounded wait would pin the serial slot', () => {
    const layer = layerWithTransport(
      new FakeTransport({ sendTimeoutMs: Number.POSITIVE_INFINITY }),
    );

    // WsTransport accepts Infinity to disable its own send timeout, so the
    // layer must not adopt it as the event bound. The fallback is complete on
    // its own and takes no slack: there is no transport timer to outlast.
    expect(sendTimeoutOf(layer)).toBe(UNDECLARED_EVENT_SEND_TIMEOUT_MS);
  });

  test('ignores an unusable declaration (0, negative, NaN)', () => {
    for (const declared of [0, -1, Number.NaN]) {
      const layer = layerWithTransport(
        new FakeTransport({ sendTimeoutMs: declared }),
      );
      expect(sendTimeoutOf(layer)).toBe(UNDECLARED_EVENT_SEND_TIMEOUT_MS);
    }
  });

  test('falls back to the undeclared bound when the transport declares nothing', () => {
    expect(sendTimeoutOf(createLayer())).toBe(UNDECLARED_EVENT_SEND_TIMEOUT_MS);
  });

  test('keeps a near-maximum declaration inside the usable timer range', () => {
    // Adding slack to a declaration near the 32-bit timer ceiling would push
    // the delay past it, and Bun clamps that to 1ms — the widest bound would
    // become the narrowest. The sum is capped instead.
    const layer = layerWithTransport(
      new FakeTransport({ sendTimeoutMs: 2_147_483_600 }),
    );

    expect(sendTimeoutOf(layer)).toBe(2_147_483_647);
  });
});
