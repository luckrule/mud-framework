import { describe, expect, test } from 'bun:test';

import { SendQueue } from './send-queue.ts';
import type {
  SendQueueLimits,
  SendQueueOverflow,
  SendQueueSink,
  WriteStatus,
} from './send-queue.ts';
import { tracked } from './ws-transport.support.ts';

/** Scripted `ws.send`: pops one status per write and records the payloads. */
function fakeSink(statuses: readonly WriteStatus[]): {
  sink: SendQueueSink;
  written: (string | Uint8Array)[];
  abandoned: Error[];
  overflows: SendQueueOverflow[];
} {
  const written: (string | Uint8Array)[] = [];
  const abandoned: Error[] = [];
  const overflows: SendQueueOverflow[] = [];
  let index = 0;
  return {
    written,
    abandoned,
    overflows,
    sink: {
      write: (payload) => {
        written.push(payload);
        const status = statuses[index] ?? 1;
        index += 1;
        return status;
      },
      abandon: (error) => {
        abandoned.push(error);
      },
      overflow: (info) => {
        overflows.push(info);
      },
    },
  };
}

/** Room for ordinary tests; capacity tests override one limit at a time. */
function limits(overrides: Partial<SendQueueLimits> = {}): SendQueueLimits {
  return {
    maxQueuedFrames: 100,
    maxQueuedBytes: 1_000_000,
    ...overrides,
  };
}

/** Yields to the microtask queue and one timer tick, without awaiting in a loop. */
async function settle(): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

describe('SendQueue', () => {
  test('resolves a payload the socket accepted outright', async () => {
    const { sink, written } = fakeSink([5]);
    const queue = new SendQueue('c1', 1000, limits(), sink);

    await queue.send('hello');

    expect(written).toEqual(['hello']);
    expect(queue.depth).toBe(0);
  });

  test('holds the promise while the socket buffer is full', async () => {
    const { sink, written } = fakeSink([-1]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const pending = tracked(queue.send('hello'));
    await settle();

    expect(written).toEqual(['hello']);
    expect(pending.state()).toBe('pending');
    expect(queue.depth).toBe(1);
  });

  test('does not write past a full buffer, and drain releases the queue', async () => {
    const { sink, written } = fakeSink([-1, 5, 5]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const first = tracked(queue.send('a'));
    const second = tracked(queue.send('b'));
    await settle();

    // The second payload must not reach `ws.send` while the head is held: Bun
    // already accepted the first frame and the buffer has no room.
    expect(written).toEqual(['a']);
    expect(first.state()).toBe('pending');
    expect(second.state()).toBe('pending');

    queue.drain();
    await settle();

    expect(written).toEqual(['a', 'b']);
    expect(first.state()).toBe('resolved');
    expect(second.state()).toBe('resolved');
  });

  test('keeps FIFO order across a blocked window', async () => {
    const { sink, written } = fakeSink([-1, 1, 1, 1, 1]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const order: string[] = [];
    for (const label of ['one', 'two', 'three', 'four']) {
      void queue.send(label).then(() => {
        order.push(label);
        return undefined;
      });
    }
    await settle();
    expect(written).toEqual(['one']);

    queue.drain();
    await settle();

    expect(written).toEqual(['one', 'two', 'three', 'four']);
    expect(order).toEqual(['one', 'two', 'three', 'four']);
  });

  test('rejects a payload Bun dropped instead of queuing it', async () => {
    const { sink } = fakeSink([0]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const dropped = tracked(queue.send('a'));
    await settle();

    expect(dropped.error()?.message).toMatch(/dropped a 1 byte/);
    expect(queue.depth).toBe(0);
  });

  test('rejects a zero-byte payload Bun dropped, then keeps draining', async () => {
    const { sink, written } = fakeSink([0, 1]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const dropped = tracked(queue.send(new Uint8Array(0)));
    await settle();

    expect(dropped.state()).toBe('rejected');
    expect(dropped.error()?.message).toMatch(/dropped a 0 byte/);
    expect(queue.depth).toBe(0);

    // A dropped frame must not wedge the queue behind it.
    await queue.send('next');
    expect(written).toEqual([new Uint8Array(0), 'next']);
    expect(queue.depth).toBe(0);
  });

  test('a drain arriving mid-write does not reorder the queue', async () => {
    // `write` calls back into `drain` while the flush loop is still running,
    // which is what a synchronous Bun drain would look like.
    const written: string[] = [];
    const queue: SendQueue = new SendQueue('c1', 10_000, limits(), {
      write: (payload) => {
        written.push(String(payload));
        queue.drain();
        return 1;
      },
      abandon: () => undefined,
      overflow: () => undefined,
    });

    await queue.send('a');
    await queue.send('b');

    expect(written).toEqual(['a', 'b']);
    expect(queue.depth).toBe(0);
  });

  test('fails the queue and reports once when the frame limit is reached', async () => {
    const { sink, written, abandoned, overflows } = fakeSink([-1]);
    const queue = new SendQueue(
      'c1',
      10_000,
      limits({ maxQueuedFrames: 2 }),
      sink,
    );

    const first = tracked(queue.send('a'));
    const second = tracked(queue.send('bb'));
    await settle();

    // The head is accepted-but-blocked and the second frame waits behind it.
    expect(queue.depth).toBe(2);
    expect(queue.queuedBytes).toBe(3);
    expect(written).toEqual(['a']);

    const overflow = tracked(queue.send('ccc'));
    await settle();

    expect(queue.depth).toBe(0);
    expect(queue.queuedBytes).toBe(0);
    for (const entry of [first, second, overflow]) {
      expect(entry.state()).toBe('rejected');
      expect(entry.error()?.message).toMatch(/send queue overflow/);
    }
    expect(overflows).toHaveLength(1);
    expect(overflows[0]).toMatchObject({
      limit: 'maxQueuedFrames',
      incomingBytes: 3,
      queuedFrames: 2,
      queuedBytes: 3,
      maxQueuedFrames: 2,
      maxQueuedBytes: 1_000_000,
    });
    // Overflow is a close path, not a timeout path.
    expect(abandoned).toHaveLength(0);

    const later = tracked(queue.send('later'));
    await settle();
    expect(later.state()).toBe('rejected');
    expect(later.error()?.message).toMatch(/send queue overflow/);
    expect(overflows).toHaveLength(1);
    expect(written).toEqual(['a']);
  });

  test('fails the queue when a frame would exceed the byte limit', async () => {
    const { sink, written, overflows } = fakeSink([-1]);
    const queue = new SendQueue(
      'c1',
      10_000,
      limits({ maxQueuedBytes: 8 }),
      sink,
    );

    const first = tracked(queue.send('你好')); // 6 UTF-8 bytes
    await settle();
    expect(queue.queuedBytes).toBe(6);

    const second = tracked(queue.send(new Uint8Array(3))); // 6 + 3 > 8
    await settle();

    expect(written).toEqual(['你好']);
    expect(queue.depth).toBe(0);
    expect(queue.queuedBytes).toBe(0);
    expect(first.state()).toBe('rejected');
    expect(second.state()).toBe('rejected');
    expect(overflows).toHaveLength(1);
    expect(overflows[0]).toMatchObject({
      limit: 'maxQueuedBytes',
      incomingBytes: 3,
      queuedFrames: 1,
      queuedBytes: 6,
      maxQueuedBytes: 8,
    });
  });

  test('releases string and binary bytes as frames settle', async () => {
    const { sink } = fakeSink([-1, -1, 5]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const first = tracked(queue.send('abc')); // 3 bytes
    const second = tracked(queue.send(new Uint8Array([1, 2]))); // 2 bytes
    await settle();

    expect(queue.depth).toBe(2);
    expect(queue.queuedBytes).toBe(5);

    queue.drain();
    await settle();

    // The first settled; the second is the accepted-but-blocked head now.
    expect(first.state()).toBe('resolved');
    expect(second.state()).toBe('pending');
    expect(queue.depth).toBe(1);
    expect(queue.queuedBytes).toBe(2);

    queue.drain();
    await settle();

    expect(second.state()).toBe('resolved');
    expect(queue.depth).toBe(0);
    expect(queue.queuedBytes).toBe(0);
  });

  test('accounts with the caller-measured size when one is passed', async () => {
    // The transport measures the payload for its message ceiling anyway; the
    // queue must bill that number, not measure the frame a second time.
    const { sink } = fakeSink([-1, -1]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const first = tracked(queue.send('abc', 10));
    const second = tracked(queue.send(new Uint8Array(3), 7));
    await settle();

    expect(queue.depth).toBe(2);
    expect(queue.queuedBytes).toBe(17);
    queue.fail(new Error('closed'));
    await settle();
    expect(first.state()).toBe('rejected');
    expect(second.state()).toBe('rejected');
  });

  test('falls back to measuring the payload when no size is passed', async () => {
    const { sink } = fakeSink([-1, -1]);
    const queue = new SendQueue('c1', 10_000, limits(), sink);

    const first = tracked(queue.send('你好'));
    const second = tracked(queue.send(new Uint8Array(4)));
    await settle();

    // 6 UTF-8 bytes for the string, 4 for the array.
    expect(queue.queuedBytes).toBe(10);
    queue.fail(new Error('closed'));
    await settle();
    expect(first.state()).toBe('rejected');
    expect(second.state()).toBe('rejected');
  });

  test('rejects capacity limits that are not positive integers', () => {
    const invalid: readonly [unknown, typeof RangeError | typeof TypeError][] =
      [
        [0, RangeError],
        [-1, RangeError],
        [1.5, RangeError],
        [Number.NaN, RangeError],
        [Number.POSITIVE_INFINITY, RangeError],
        ['1', TypeError],
      ];
    for (const name of ['maxQueuedFrames', 'maxQueuedBytes'] as const) {
      for (const [value, Expected] of invalid) {
        const { sink } = fakeSink([]);
        const create = () =>
          new SendQueue(
            'c1',
            10_000,
            { ...limits(), [name]: value as number },
            sink,
          );
        expect(create).toThrow(Expected);
        expect(create).toThrow(new RegExp(name));
        expect(create).toThrow(new RegExp(String(value)));
      }
    }
  });
});
