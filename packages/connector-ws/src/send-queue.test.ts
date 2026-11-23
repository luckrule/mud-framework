import { describe, expect, test } from 'bun:test';

import { SendQueue } from './send-queue.ts';
import type { SendQueueSink, WriteStatus } from './send-queue.ts';

/** Scripted `ws.send`: pops one status per write and records the payloads. */
function fakeSink(statuses: readonly WriteStatus[]): {
  sink: SendQueueSink;
  written: (string | Uint8Array)[];
  abandoned: Error[];
} {
  const written: (string | Uint8Array)[] = [];
  const abandoned: Error[] = [];
  let index = 0;
  return {
    written,
    abandoned,
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
    },
  };
}

/** Yields to the microtask queue and one timer tick, without awaiting in a loop. */
async function settle(): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

function tracked(promise: Promise<void>): {
  promise: Promise<void>;
  state: () => 'pending' | 'resolved' | 'rejected';
  error: () => Error | undefined;
} {
  let state: 'pending' | 'resolved' | 'rejected' = 'pending';
  let error: Error | undefined;
  void promise.then(
    () => {
      state = 'resolved';
      return undefined;
    },
    (rejection: unknown) => {
      state = 'rejected';
      error = rejection instanceof Error ? rejection : new Error('non-Error');
      return undefined;
    },
  );
  return { promise, state: () => state, error: () => error };
}

describe('SendQueue', () => {
  test('resolves a payload the socket accepted outright', async () => {
    const { sink, written } = fakeSink([5]);
    const queue = new SendQueue('c1', 1000, sink);

    await queue.send('hello', 5);

    expect(written).toEqual(['hello']);
    expect(queue.depth).toBe(0);
  });

  test('holds the promise while the socket buffer is full', async () => {
    const { sink, written } = fakeSink([-1]);
    const queue = new SendQueue('c1', 10_000, sink);

    const pending = tracked(queue.send('hello', 5));
    await settle();

    expect(written).toEqual(['hello']);
    expect(pending.state()).toBe('pending');
    expect(queue.depth).toBe(1);
  });

  test('does not write past a full buffer, and drain releases the queue', async () => {
    const { sink, written } = fakeSink([-1, 5, 5]);
    const queue = new SendQueue('c1', 10_000, sink);

    const first = tracked(queue.send('a', 1));
    const second = tracked(queue.send('b', 1));
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
    const queue = new SendQueue('c1', 10_000, sink);

    const order: string[] = [];
    for (const label of ['one', 'two', 'three', 'four']) {
      void queue.send(label, 3).then(() => {
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

  test('rejects the timed-out payload and asks for the connection to drop', async () => {
    const { sink, abandoned } = fakeSink([-1]);
    const queue = new SendQueue('c1', 10, sink);

    const pending = tracked(queue.send('hello', 5));
    await settle();
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 40);
    });

    expect(pending.state()).toBe('rejected');
    expect(pending.error()?.message).toMatch(/timed out after 10ms/);
    expect(abandoned).toHaveLength(1);
    expect(queue.depth).toBe(0);
  });

  test('rejects every outstanding payload when the connection closes', async () => {
    const { sink } = fakeSink([-1]);
    const queue = new SendQueue('c1', 10_000, sink);

    const first = tracked(queue.send('a', 1));
    const second = tracked(queue.send('b', 1));
    const third = tracked(queue.send('c', 1));
    await settle();

    queue.fail(new Error('connection "c1" closed (1001 bye)'));
    await settle();

    expect(first.error()?.message).toMatch(/closed/);
    expect(second.error()?.message).toMatch(/closed/);
    expect(third.error()?.message).toMatch(/closed/);
    expect(queue.depth).toBe(0);
  });

  test('rejects a later send once the queue has failed', async () => {
    const { sink } = fakeSink([1]);
    const queue = new SendQueue('c1', 10_000, sink);

    queue.fail(new Error('connection "c1" closed'));
    const rejected = tracked(queue.send('a', 1));
    await settle();

    expect(rejected.error()?.message).toMatch(/closed/);
  });

  test('rejects a payload Bun dropped instead of queuing it', async () => {
    const { sink } = fakeSink([0]);
    const queue = new SendQueue('c1', 10_000, sink);

    const dropped = tracked(queue.send('a', 1));
    await settle();

    expect(dropped.error()?.message).toMatch(/dropped a 1 byte/);
    expect(queue.depth).toBe(0);
  });

  test('a drain arriving mid-write does not reorder the queue', async () => {
    // `write` calls back into `drain` while the flush loop is still running,
    // which is what a synchronous Bun drain would look like.
    const written: string[] = [];
    const queue: SendQueue = new SendQueue('c1', 10_000, {
      write: (payload) => {
        written.push(String(payload));
        queue.drain();
        return 1;
      },
      abandon: () => undefined,
    });

    await queue.send('a', 1);
    await queue.send('b', 1);

    expect(written).toEqual(['a', 'b']);
    expect(queue.depth).toBe(0);
  });
});
