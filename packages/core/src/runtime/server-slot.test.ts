import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import type { ServerExitInfo } from '../ipc/spawn-port.ts';
import type { ServerState } from './server-slot.ts';
import { ServerSlot } from './server-slot.ts';

const EXITED: ServerExitInfo = { code: 0, signal: null, observed: true };

function fakeChannel(): IpcChannel {
  return {
    send: () => {},
    onMessage: () => {},
    onClose: () => {},
    close: () => {},
  };
}

/** Every field is present, so a test only names the ones it depends on. */
function fakeServer(overrides: Partial<ServerState> = {}): ServerState {
  return {
    generation: 1,
    channel: fakeChannel(),
    kill: () => {},
    exitPromise: Promise.resolve(EXITED),
    lastExit: () => undefined,
    ready: false,
    closed: false,
    exited: false,
    shutdownAcknowledged: false,
    pid: 1001,
    staged: false,
    retiring: false,
    declaredMaxDurationMs: undefined,
    reportedCodeVersion: undefined,
    shutdownSent: false,
    closedSignal: Promise.withResolvers<void>(),
    ...overrides,
  };
}

describe('ServerSlot status transitions', () => {
  test('a fresh slot is idle with no server and no switch', () => {
    const slot = new ServerSlot();

    expect(slot.status).toBe('idle');
    expect(slot.server).toBeUndefined();
    expect(slot.switching).toBeUndefined();
    expect(slot.isTerminal).toBe(false);
  });

  test('beginStart moves to starting and does not create a server', () => {
    const slot = new ServerSlot();

    slot.beginStart();

    expect(slot.status).toBe('starting');
    expect(slot.server).toBeUndefined();
  });

  test('trackSpawning records the server and keeps the starting status', () => {
    const slot = new ServerSlot();
    const server = fakeServer();

    slot.beginStart();
    slot.trackSpawning(server);

    expect(slot.status).toBe('starting');
    expect(slot.server).toBe(server);
  });

  test('markReady records the server and moves to ready', () => {
    const slot = new ServerSlot();
    const server = fakeServer();

    slot.markReady(server);

    expect(slot.status).toBe('ready');
    expect(slot.server).toBe(server);
  });

  test('beginSwitch records the operation and moves to switching', () => {
    const slot = new ServerSlot();
    const op = { staged: fakeServer(), previous: fakeServer() };

    slot.beginSwitch(op);

    expect(slot.status).toBe('switching');
    expect(slot.switching).toBe(op);
  });

  test('adopt promotes the replacement and keeps the switch in flight', () => {
    const slot = new ServerSlot();
    const op = { staged: fakeServer(), previous: fakeServer() };
    const replacement = fakeServer({ generation: 2 });

    slot.beginSwitch(op);
    slot.adopt(replacement);

    expect(slot.status).toBe('ready');
    expect(slot.server).toBe(replacement);
    expect(slot.switching).toBe(op);
  });

  test('resumeServing returns to ready and keeps the serving server', () => {
    const slot = new ServerSlot();
    const previous = fakeServer();
    const op = { staged: fakeServer(), previous };

    slot.markReady(previous);
    slot.beginSwitch(op);
    slot.resumeServing();

    expect(slot.status).toBe('ready');
    expect(slot.server).toBe(previous);
  });

  test('markClosing moves to closing', () => {
    const slot = new ServerSlot();

    slot.markClosing();

    expect(slot.status).toBe('closing');
  });

  test('markClosed moves to closed', () => {
    const slot = new ServerSlot();

    slot.markClosed();

    expect(slot.status).toBe('closed');
  });

  test('markFailed moves to failed', () => {
    const slot = new ServerSlot();

    slot.markFailed();

    expect(slot.status).toBe('failed');
  });

  test('markStartingAfterLoss moves a failed slot back to starting', () => {
    const slot = new ServerSlot();
    const lost = fakeServer();

    slot.markReady(lost);
    slot.markFailed();
    slot.markStartingAfterLoss();

    expect(slot.status).toBe('starting');
    // The dead server is still the identity the crash path compares against.
    expect(slot.server).toBe(lost);
  });
});

describe('ServerSlot isTerminal', () => {
  test('idle is not terminal', () => {
    expect(new ServerSlot().isTerminal).toBe(false);
  });

  test('starting is not terminal', () => {
    const slot = new ServerSlot();
    slot.beginStart();

    expect(slot.isTerminal).toBe(false);
  });

  test('ready is not terminal', () => {
    const slot = new ServerSlot();
    slot.markReady(fakeServer());

    expect(slot.isTerminal).toBe(false);
  });

  test('switching is not terminal', () => {
    const slot = new ServerSlot();
    slot.beginSwitch({ staged: fakeServer(), previous: fakeServer() });

    expect(slot.isTerminal).toBe(false);
  });

  test('closing is terminal', () => {
    const slot = new ServerSlot();
    slot.markClosing();

    expect(slot.isTerminal).toBe(true);
  });

  test('closed is terminal', () => {
    const slot = new ServerSlot();
    slot.markClosed();

    expect(slot.isTerminal).toBe(true);
  });

  test('failed is terminal', () => {
    const slot = new ServerSlot();
    slot.markFailed();

    expect(slot.isTerminal).toBe(true);
  });
});

describe('ServerSlot nextGeneration', () => {
  test('generations start at one and increase strictly', () => {
    const slot = new ServerSlot();

    const first = slot.nextGeneration();
    const second = slot.nextGeneration();
    const third = slot.nextGeneration();

    expect(first).toBe(1);
    expect(second).toBeGreaterThan(first);
    expect(third).toBeGreaterThan(second);
  });

  test('generations are not reset by a status change', () => {
    const slot = new ServerSlot();
    slot.nextGeneration();
    slot.markReady(fakeServer({ generation: 1 }));
    slot.markClosed();

    expect(slot.nextGeneration()).toBe(2);
  });
});

describe('ServerSlot releaseSwitch', () => {
  test('releasing the in-flight operation clears it', () => {
    const slot = new ServerSlot();
    const op = { staged: fakeServer(), previous: fakeServer() };
    slot.beginSwitch(op);

    slot.releaseSwitch(op);

    expect(slot.switching).toBeUndefined();
  });

  test('releasing another operation leaves the in-flight one in place', () => {
    const slot = new ServerSlot();
    const op = { staged: fakeServer(), previous: fakeServer() };
    const other = { staged: fakeServer(), previous: fakeServer() };
    slot.beginSwitch(op);

    slot.releaseSwitch(other);

    expect(slot.switching).toBe(op);
  });

  test('releasing a switch does not change the status', () => {
    const slot = new ServerSlot();
    const op = { staged: fakeServer(), previous: fakeServer() };
    slot.beginSwitch(op);

    slot.releaseSwitch(op);

    // The switch's own finish step still has to report its outcome.
    expect(slot.status).toBe('switching');
  });
});

describe('ServerSlot clearIfCurrent', () => {
  test('clears the current server and reports true', () => {
    const slot = new ServerSlot();
    const server = fakeServer();
    slot.markReady(server);

    expect(slot.clearIfCurrent(server)).toBe(true);
    expect(slot.server).toBeUndefined();
  });

  test('leaves a promoted replacement in place and reports false', () => {
    const slot = new ServerSlot();
    const stale = fakeServer({ generation: 1 });
    const replacement = fakeServer({ generation: 2 });
    slot.markReady(stale);
    slot.adopt(replacement);

    expect(slot.clearIfCurrent(stale)).toBe(false);
    expect(slot.server).toBe(replacement);
  });

  test('reports false when no server is current', () => {
    const slot = new ServerSlot();

    expect(slot.clearIfCurrent(fakeServer())).toBe(false);
    expect(slot.server).toBeUndefined();
  });
});

describe('ServerSlot assertNoOtherLiveServer', () => {
  test('throws when the current server is live and not the one being activated', () => {
    const slot = new ServerSlot();
    const live = fakeServer({ generation: 3 });
    const activating = fakeServer({ generation: 4 });
    slot.markReady(live);

    expect(() => slot.assertNoOtherLiveServer(activating)).toThrow();
  });

  test('throws when the staged server is live', () => {
    const slot = new ServerSlot();
    const staged = fakeServer({ generation: 5 });
    slot.beginSwitch({ staged, previous: undefined });

    expect(() => slot.assertNoOtherLiveServer()).toThrow();
  });

  test('throws when the previous server is live', () => {
    const slot = new ServerSlot();
    const previous = fakeServer({ generation: 6 });
    slot.beginSwitch({ staged: undefined, previous });

    expect(() => slot.assertNoOtherLiveServer()).toThrow();
  });

  test('passes when every candidate has closed or exited', () => {
    const slot = new ServerSlot();
    slot.beginSwitch({
      staged: fakeServer({ generation: 7, closed: true }),
      previous: fakeServer({ generation: 6, exited: true }),
    });

    expect(() => slot.assertNoOtherLiveServer()).not.toThrow();
  });

  test('passes when the only live candidate is the one being activated', () => {
    const slot = new ServerSlot();
    const current = fakeServer({ generation: 8 });
    slot.markReady(current);

    expect(() => slot.assertNoOtherLiveServer(current)).not.toThrow();
  });

  test('the error names the blocking generation and pid', () => {
    const slot = new ServerSlot();
    const live = fakeServer({ generation: 11, pid: 4242 });
    slot.markReady(live);

    expect(() =>
      slot.assertNoOtherLiveServer(fakeServer({ generation: 12 })),
    ).toThrow(/generation 11, pid 4242/);
  });

  test('the error reports an unknown pid as unknown', () => {
    const slot = new ServerSlot();
    const live = fakeServer({ generation: 13, pid: undefined });
    slot.markReady(live);

    expect(() => slot.assertNoOtherLiveServer()).toThrow(
      /generation 13, pid unknown/,
    );
  });

  test('passes in the state the switch reaches before activate', () => {
    // What `retirePrevious` leaves behind: the old process is still the current
    // Server but its exit was confirmed. This is why the assertion is a
    // defensive gate rather than a reachable failure branch.
    const slot = new ServerSlot();
    const previous = fakeServer({ generation: 20, exited: true });
    const staged = fakeServer({ generation: 21 });
    slot.markReady(previous);
    slot.beginSwitch({ staged, previous });

    expect(() => slot.assertNoOtherLiveServer(staged)).not.toThrow();
  });

  test('throws in the same state when the previous exit was not confirmed', () => {
    // The future orchestration change the gate exists for: activate while the
    // retired process might still be writing.
    const slot = new ServerSlot();
    const previous = fakeServer({ generation: 22 });
    const staged = fakeServer({ generation: 23 });
    slot.markReady(previous);
    slot.beginSwitch({ staged, previous });

    expect(() => slot.assertNoOtherLiveServer(staged)).toThrow(/generation 22/);
  });
});
