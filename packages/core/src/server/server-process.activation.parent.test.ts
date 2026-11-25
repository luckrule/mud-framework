import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { rejectionOf, tick } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import {
  activate,
  activationHarness,
  hello,
  types,
} from './server-process.activation.support.ts';
import { ServerChannelClosedError } from './server-process.ts';

describe('parent liveness before ready', () => {
  test('rejects within a bounded time when the parent disappears while prepared', async () => {
    let parentPid = 1000;
    const h = activationHarness({
      parentCheckIntervalMs: 5,
      readParentPid: () => parentPid,
    });

    hello(h);
    expect(h.created()).toBe(0);

    parentPid = 2000;
    const error = await withDeadline(
      rejectionOf(h.done),
      1000,
      'runServerProcess did not return after the parent disappeared',
    );

    if (!(error instanceof ServerChannelClosedError)) {
      throw new Error(`expected ServerChannelClosedError, got ${error.name}`);
    }
    expect(error.message).toContain('parent process is gone');
    // ADR-0007: prepare holds no storage, so a parent that vanished cannot
    // leave a writer behind — and the process must not keep running either.
    expect(h.created()).toBe(0);
    const logged = h.logs().filter((line) => line.code === 'IPC_PARENT_GONE');
    expect(logged).toHaveLength(1);
  });

  test('closes the backend it just opened when the parent disappears during the start', async () => {
    let parentPid = 1000;
    const gate = Promise.withResolvers<void>();
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: async () => {
        await gate.promise;
        return backend;
      },
      parentCheckIntervalMs: 5,
      readParentPid: () => parentPid,
    });

    hello(h);
    activate(h);
    await tick();
    parentPid = 2000;
    // Let the poll fire while the backend is still opening.
    await withDeadline(
      (async () => {
        while (h.logs().every((line) => line.code !== 'IPC_PARENT_GONE')) {
          await tick();
        }
      })(),
      1000,
      'the parent-gone poll did not fire',
    );
    gate.resolve();

    const error = await withDeadline(
      rejectionOf(h.done),
      2000,
      'runServerProcess did not return after the parent disappeared',
    );
    expect(error).toBeInstanceOf(ServerChannelClosedError);
    // No `ready`: there is no Portal left to announce it to.
    expect(types(h)).toEqual(['prepared']);
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });
});
