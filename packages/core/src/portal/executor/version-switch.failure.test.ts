import { describe, expect, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
  flushTicks,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
  waitFor,
} from '../../testing/ipc-executor.ts';
import { isServerSwitchError, ServerSwitchError } from './switch-failure.ts';
import type { SwitchFailureDetails } from './switch-failure.ts';
import { labelledServer, switchable } from './version-switch.support.ts';

/** The structured failure of a rejection, failing the test if it carries none. */
async function failureDetails(
  switching: Promise<void>,
): Promise<SwitchFailureDetails> {
  const error = await rejectionOf(switching);
  if (!isServerSwitchError(error)) {
    throw new Error(`expected a ServerSwitchError, got: ${error.message}`);
  }
  return error.details;
}

describe('switchTo failure details', () => {
  test('reports the failure before the old server was touched as still serving', async () => {
    const h = switchable();
    const old = await started(h);

    const details = await failureDetails(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
      }),
    );

    // `startTimeoutMs` in `switchable` is 500ms; this path must not wait for it
    // to distinguish anything, it just records where it stopped.
    expect(details.stage).toBe('preparing');
    expect(details.previousServing).toBe(true);
    expect(details.rolledBack).toBe(false);
    expect(details.promoted).toBe(false);
    expect(details.pendingExits).toBe(0);

    // The structured view matches the behaviour the tests already assert.
    expect(old.killCount).toBe(0);
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    old.send(successMessage('after', 'still-old'));
    expect(await outcome).toEqual({ ok: true, result: 'still-old' });
    await h.executor.close();
  });

  test('reports the failure after the old server retired as rolled back', async () => {
    const h = switchable({ startTimeoutMs: 40 });
    await started(h);

    const details = await failureDetails(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      }),
    );

    expect(details.stage).toBe('activating');
    expect(details.previousServing).toBe(false);
    expect(details.rolledBack).toBe(true);
    expect(details.promoted).toBe(false);

    // The rollback really happened: a third process takes over and serves.
    await waitFor(() => h.servers.length === 3);
    const restarted = serverAt(h, 2);
    restarted.send(readyMessage());
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(restarted)).toContain('after');
    restarted.send(successMessage('after', 'rolled-back'));
    expect(await outcome).toEqual({ ok: true, result: 'rolled-back' });
    await h.executor.close();
  });

  test('reports the switch as neither serving nor rolled back when close() aborts it', async () => {
    // `close()` owns the cleanup, so a caller must not read the rejection as
    // "retry now": neither the old Server nor a rollback is available.
    const h = switchable({ startTimeoutMs: 5000, shutdownTimeoutMs: 30 });
    await started(h);

    const switching = h.executor.switchTo({
      spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
    });
    const closing = h.executor.close();
    const details = await failureDetails(switching);
    await closing;

    expect(details.rolledBack).toBe(false);
    expect(details.previousServing).toBe(false);
    expect(details.closeAborted).toBe(true);
  });

  test('keeps the message text the failure had before the fields existed', async () => {
    // The structured fields are additive: a caller that matches the message
    // must not have to change, and the error is still an Error.
    const h = switchable();
    await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
      }),
    );

    expect(error).toBeInstanceOf(ServerSwitchError);
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toMatch(/did not prepare/);
    await h.executor.close();
  });

  test('guards a structured failure by shape as well as class', () => {
    // A consumer may receive the rejection through a bundler boundary where the
    // class identity differs; the shape still has to be recognised.
    const error = new ServerSwitchError('failed', {
      stage: 'preparing',
      previousServing: true,
      rolledBack: false,
      promoted: false,
      closeAborted: false,
      pendingExits: 0,
    });

    expect(isServerSwitchError(error)).toBe(true);
    expect(isServerSwitchError(new Error('failed'))).toBe(false);
    expect(isServerSwitchError('failed')).toBe(false);
  });
});
