import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { rejectionOf, tick } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { startupFailure } from '../testing/server-process.ts';
import {
  activate,
  activationHarness,
  hello,
  types,
} from './server-process.activation.support.ts';
import { ServerChannelClosedError } from './server-process.ts';
import { ServerStartupError } from './server-startup.ts';

describe('runServerProcess two-phase activation', () => {
  test('fails without opening the backend when the link closes during prepare', async () => {
    const h = activationHarness();

    hello(h);
    h.portal.close('portal gone');

    const error = await rejectionOf(h.done);
    if (!(error instanceof ServerChannelClosedError)) {
      throw new Error(`expected ServerChannelClosedError, got ${error.name}`);
    }
    expect(error.reason).toBe('portal gone');
    expect(h.created()).toBe(0);
  });

  test('reports a startup failure after activate as ServerStartupError', async () => {
    const h = activationHarness({
      createBackend: () => Promise.reject(new Error('disk unavailable')),
    });

    hello(h);
    activate(h);

    const error = await startupFailure(h.done);
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.message).toContain('disk unavailable');
    // The Portal saw the Server was prepared, but never ready.
    expect(types(h)).toEqual(['prepared']);
  });

  test('fails the startup when the ready barrier cannot be sent', async () => {
    // A send that throws while the link stays open: the old code logged
    // `IPC_SEND_FAILED`, marked the process active and parked forever, leaving
    // the backend open until Portal's start timeout killed it.
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: () => Promise.resolve(backend),
      wrapChannel: (channel) => ({
        ...channel,
        send: (message) => {
          if (message.type === 'ready') {
            throw new Error('wire refused ready');
          }
          channel.send(message);
        },
      }),
    });

    hello(h);
    activate(h);

    const error = await withDeadline(
      startupFailure(h.done),
      1000,
      'runServerProcess did not fail after the ready send threw',
    );
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.reason).toContain('ready could not be sent');
    expect(error.reason).toContain('wire refused ready');
    // Never active: Portal saw the barrier it was waiting for fail, so no
    // request may be accepted and no second `ready` may appear.
    expect(types(h)).toEqual(['prepared']);
    expect(
      h.logs().filter((line) => line.msg === 'server process ready'),
    ).toEqual([]);
    // One failure line, not one per step: the same contract `startServer`
    // failures follow.
    expect(
      h.logs().filter((line) => line.code === 'SERVER_STARTUP_FAILED'),
    ).toHaveLength(1);
    // The storage this start opened must not outlive it (INV-002).
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
    // The process is gone: the link was closed on the failure path.
    expect(() => h.portal.send({ type: 'hello' })).toThrow(/closed/);
  });

  test('fails the startup when the prepared barrier cannot be sent', async () => {
    const h = activationHarness({
      wrapChannel: (channel) => ({
        ...channel,
        send: (message) => {
          if (message.type === 'prepared') {
            throw new Error('wire refused prepared');
          }
          channel.send(message);
        },
      }),
    });

    hello(h);

    const error = await withDeadline(
      startupFailure(h.done),
      1000,
      'runServerProcess did not fail after the prepared send threw',
    );
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.reason).toContain('prepared could not be sent');
    // ADR-0007: prepare never opened storage, so there is nothing to close and
    // nothing was created.
    expect(h.created()).toBe(0);
    expect(types(h)).toEqual([]);
  });

  test('closes storage and fails when the link dies while the start is running', async () => {
    const gate = Promise.withResolvers<void>();
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: async () => {
        await gate.promise;
        return backend;
      },
    });

    hello(h);
    activate(h);
    await tick();
    h.portal.close('portal gone');
    gate.resolve();

    const error = await rejectionOf(h.done);
    expect(error).toBeInstanceOf(ServerChannelClosedError);
    expect(error.message).toContain('portal gone');
    // The storage opened during the start must not outlive it (INV-002).
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });
});
