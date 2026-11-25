import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../../ipc/channel.ts';
import type { ServerExitInfo } from '../../ipc/spawn-port.ts';
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
    shutdownSent: false,
    closedSignal: Promise.withResolvers<void>(),
    ...overrides,
  };
}

interface Fixture {
  slot: ServerSlot;
  previous: ServerState;
  staged: ServerState;
  replacement: ServerState;
  op: { staged: ServerState; previous: ServerState };
}

function fixture(): Fixture {
  const previous = fakeServer({ generation: 1 });
  const staged = fakeServer({ generation: 2 });
  const replacement = fakeServer({ generation: 3 });
  return {
    slot: new ServerSlot(),
    previous,
    staged,
    replacement,
    op: { staged, previous },
  };
}

type ServerRef = 'none' | 'previous' | 'staged' | 'replacement';

interface TransitionRow {
  name: string;
  apply: (f: Fixture) => void;
  status: ServerSlot['status'];
  server: ServerRef;
  switching: boolean;
  terminal: boolean;
}

const TRANSITIONS: readonly TransitionRow[] = [
  {
    name: 'a fresh slot is idle',
    apply: () => {},
    status: 'idle',
    server: 'none',
    switching: false,
    terminal: false,
  },
  {
    name: 'beginStart moves to starting without a server',
    apply: (f) => f.slot.beginStart(),
    status: 'starting',
    server: 'none',
    switching: false,
    terminal: false,
  },
  {
    name: 'trackSpawning records the server and keeps starting',
    apply: (f) => {
      f.slot.beginStart();
      f.slot.trackSpawning(f.staged);
    },
    status: 'starting',
    server: 'staged',
    switching: false,
    terminal: false,
  },
  {
    name: 'markReady records the server and moves to ready',
    apply: (f) => f.slot.markReady(f.staged),
    status: 'ready',
    server: 'staged',
    switching: false,
    terminal: false,
  },
  {
    name: 'beginSwitch records the operation and moves to switching',
    apply: (f) => f.slot.beginSwitch(f.op),
    status: 'switching',
    server: 'none',
    switching: true,
    terminal: false,
  },
  {
    name: 'markReady promotes the replacement and keeps the switch in flight',
    apply: (f) => {
      f.slot.beginSwitch(f.op);
      f.slot.markReady(f.replacement);
    },
    status: 'ready',
    server: 'replacement',
    switching: true,
    terminal: false,
  },
  {
    name: 'resumeServing returns to ready and keeps the serving server',
    apply: (f) => {
      f.slot.markReady(f.previous);
      f.slot.beginSwitch(f.op);
      f.slot.resumeServing();
    },
    status: 'ready',
    server: 'previous',
    switching: true,
    terminal: false,
  },
  {
    name: 'markClosing moves to closing and becomes terminal',
    apply: (f) => f.slot.markClosing(),
    status: 'closing',
    server: 'none',
    switching: false,
    terminal: true,
  },
  {
    name: 'markClosed moves to closed and is terminal',
    apply: (f) => f.slot.markClosed(),
    status: 'closed',
    server: 'none',
    switching: false,
    terminal: true,
  },
  {
    name: 'markFailed moves to failed and is terminal',
    apply: (f) => f.slot.markFailed(),
    status: 'failed',
    server: 'none',
    switching: false,
    terminal: true,
  },
  {
    name: 'beginStart moves a failed slot back to starting and keeps the dead server',
    apply: (f) => {
      f.slot.markReady(f.previous);
      f.slot.markFailed();
      f.slot.beginStart();
    },
    status: 'starting',
    server: 'previous',
    switching: false,
    terminal: false,
  },
];

describe('ServerSlot status transitions', () => {
  for (const row of TRANSITIONS) {
    test(row.name, () => {
      const f = fixture();
      row.apply(f);

      expect(f.slot.status).toBe(row.status);
      expect(f.slot.server).toBe(
        row.server === 'none' ? undefined : f[row.server],
      );
      expect(f.slot.switching).toBe(row.switching ? f.op : undefined);
      expect(f.slot.isTerminal).toBe(row.terminal);
    });
  }
});

describe('ServerSlot nextGeneration', () => {
  test('starts at one, increases strictly and survives a status change', () => {
    const f = fixture();

    const first = f.slot.nextGeneration();
    const second = f.slot.nextGeneration();
    f.slot.markReady(f.staged);
    f.slot.markClosed();

    expect(first).toBe(1);
    expect(second).toBeGreaterThan(first);
    expect(f.slot.nextGeneration()).toBeGreaterThan(second);
  });
});

interface ReleaseRow {
  name: string;
  release: 'current' | 'other';
  switching: boolean;
}

const RELEASE_CASES: readonly ReleaseRow[] = [
  {
    name: 'releasing the in-flight operation clears it',
    release: 'current',
    switching: false,
  },
  {
    name: 'releasing another operation leaves the in-flight one in place',
    release: 'other',
    switching: true,
  },
];

describe('ServerSlot releaseSwitch', () => {
  for (const row of RELEASE_CASES) {
    test(row.name, () => {
      const f = fixture();
      const other = {
        staged: fakeServer({ generation: 4 }),
        previous: fakeServer({ generation: 5 }),
      };
      f.slot.beginSwitch(f.op);

      f.slot.releaseSwitch(row.release === 'current' ? f.op : other);

      expect(f.slot.switching).toBe(row.switching ? f.op : undefined);
      // The switch's own finish step still has to report its outcome.
      expect(f.slot.status).toBe('switching');
    });
  }
});

interface ClearRow {
  name: string;
  setup: (f: Fixture) => void;
  target: (f: Fixture) => ServerState;
  cleared: boolean;
  server: ServerRef;
}

const CLEAR_CASES: readonly ClearRow[] = [
  {
    name: 'clears the current server and reports true',
    setup: (f) => f.slot.markReady(f.previous),
    target: (f) => f.previous,
    cleared: true,
    server: 'none',
  },
  {
    name: 'leaves a promoted replacement in place and reports false',
    setup: (f) => {
      f.slot.markReady(f.previous);
      f.slot.markReady(f.replacement);
    },
    target: (f) => f.previous,
    cleared: false,
    server: 'replacement',
  },
  {
    name: 'reports false when no server is current',
    setup: () => {},
    target: (f) => f.staged,
    cleared: false,
    server: 'none',
  },
];

describe('ServerSlot clearIfCurrent', () => {
  for (const row of CLEAR_CASES) {
    test(row.name, () => {
      const f = fixture();
      row.setup(f);

      expect(f.slot.clearIfCurrent(row.target(f))).toBe(row.cleared);
      expect(f.slot.server).toBe(
        row.server === 'none' ? undefined : f[row.server],
      );
    });
  }
});

describe('ServerSlot assertNoOtherLiveServer', () => {
  test('throws when the current server is live and not the one being activated', () => {
    const slot = new ServerSlot();
    const live = fakeServer({ generation: 3 });
    const activating = fakeServer({ generation: 4 });
    slot.track(live);
    slot.markReady(live);

    expect(() => slot.assertNoOtherLiveServer(activating)).toThrow();
  });

  test('throws when the staged server is live', () => {
    const slot = new ServerSlot();
    const staged = fakeServer({ generation: 5 });
    slot.track(staged);
    slot.beginSwitch({ staged, previous: undefined });

    expect(() => slot.assertNoOtherLiveServer()).toThrow();
  });

  test('throws when the previous server is live', () => {
    const slot = new ServerSlot();
    const previous = fakeServer({ generation: 6 });
    slot.track(previous);
    slot.beginSwitch({ staged: undefined, previous });

    expect(() => slot.assertNoOtherLiveServer()).toThrow();
  });

  test('throws when a retained closed process has no confirmed exit', () => {
    const slot = new ServerSlot();
    const closed = fakeServer({ generation: 7, closed: true });
    slot.trackSpawning(closed);
    slot.clearIfCurrent(closed);
    slot.beginSwitch({
      staged: undefined,
      previous: fakeServer({ generation: 6, exited: true }),
    });

    expect(() => slot.assertNoOtherLiveServer()).toThrow(/generation 7/);
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
    slot.track(live);
    slot.markReady(live);

    expect(() =>
      slot.assertNoOtherLiveServer(fakeServer({ generation: 12 })),
    ).toThrow(/generation 11, pid 4242/);
  });

  test('the error reports an unknown pid as unknown', () => {
    const slot = new ServerSlot();
    const live = fakeServer({ generation: 13, pid: undefined });
    slot.track(live);
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
    slot.track(previous);
    slot.track(staged);
    slot.markReady(previous);
    slot.beginSwitch({ staged, previous });

    expect(() => slot.assertNoOtherLiveServer(staged)).toThrow(/generation 22/);
  });
});
