import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { parseLine } from '../testing/server-process.ts';
import {
  DEFAULT_MAX_SESSIONS,
  DEFAULT_SESSION_HANDOFF_TIMEOUT_MS,
  SessionRouter,
} from './session-router.ts';
import type { SessionHolder } from './session-router.ts';

interface Harness {
  router: SessionRouter;
  logs: () => Record<string, unknown>[];
  /** A plain holder: chain already settled, never closes on its own. */
  holder: (connectionId: string) => SessionHolder;
}

/**
 * A router plus a factory for plain holders, so the routing table can be
 * exercised without a connection. Cases that need to control a chain tail build
 * their own holder and pass it to `bind` directly.
 */
function harness(
  options: {
    maxSessions?: number;
    handoffTimeoutMs?: number;
    logger?: Logger;
  } = {},
): Harness {
  const lines: string[] = [];
  const logger: Logger =
    options.logger ??
    createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
  const holders = new Map<string, SessionHolder>();
  const router = new SessionRouter({
    maxSessions: options.maxSessions ?? DEFAULT_MAX_SESSIONS,
    handoffTimeoutMs:
      options.handoffTimeoutMs ?? DEFAULT_SESSION_HANDOFF_TIMEOUT_MS,
    logger,
  });
  const holder = (connectionId: string): SessionHolder => {
    const existing = holders.get(connectionId);
    if (existing !== undefined) {
      return existing;
    }
    const created: SessionHolder = {
      connectionId,
      chain: () => Promise.resolve(),
      closed: () => new Promise(() => {}),
    };
    holders.set(connectionId, created);
    return created;
  };
  return {
    router,
    logs: () => lines.map(parseLine),
    holder,
  };
}

describe('SessionRouter', () => {
  test('a first binding returns no gate', () => {
    const { router, holder, logs } = harness();

    expect(router.bind(holder('c1'), 's1')).toBeUndefined();
    expect(router.sessionOf('c1')).toBe('s1');
    expect(logs().map((entry) => entry.msg)).toEqual(['session bound']);
  });

  test('rebinding the same session on the same holder is idempotent', () => {
    const { router, holder, logs } = harness();
    const c1 = holder('c1');

    expect(router.bind(c1, 's1')).toBeUndefined();
    expect(router.bind(c1, 's1')).toBeUndefined();
    expect(router.bind(c1, 's1')).toBeUndefined();

    // A long-lived connection binds once; repeating it must not log per bind
    // or turn into a self-handoff.
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(1);
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
  });

  test('a second holder on the same session gets a gate naming the previous one', () => {
    const { router, holder, logs } = harness();

    router.bind(holder('c1'), 's1');
    const gate = router.bind(holder('c2'), 's1');

    expect(gate).toBeDefined();
    expect(gate?.sessionId).toBe('s1');
    expect(gate?.previousConnectionId).toBe('c1');
    const rebound = logs().filter(
      (entry) => entry.msg === 'session rebound to a new connection',
    );
    expect(rebound).toHaveLength(1);
    expect(rebound[0]).toMatchObject({
      level: 'warn',
      sessionId: 's1',
      previousConnectionId: 'c1',
      connectionId: 'c2',
    });
    // The new holder owns the route; the old one no longer answers for it.
    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.sessionOf('c1')).toBeUndefined();
  });

  test('a released route rebinds as a first binding, with no gate', () => {
    const { router, holder } = harness();
    router.bind(holder('c1'), 's1');

    router.release(holder('c1'));
    expect(router.sessionOf('c1')).toBeUndefined();

    // SPEC 6.1: the route went with the connection, so nothing is inherited.
    expect(router.bind(holder('c2'), 's1')).toBeUndefined();
  });

  test('release is safe for a connection that holds nothing', () => {
    const { router, holder } = harness();

    router.release(holder('never-seen'));
    router.release(holder('never-seen'));

    expect(router.sessionOf('never-seen')).toBeUndefined();
  });

  test('release keeps a route another connection has taken over', () => {
    const { router, holder } = harness();
    router.bind(holder('c1'), 's1');
    router.bind(holder('c2'), 's1');

    // c1 was already displaced; releasing it must not drop c2's route.
    router.release(holder('c1'));

    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.bind(holder('c3'), 's1')?.previousConnectionId).toBe('c2');
  });

  test('refuses a new session once the table is full', () => {
    const { router, holder, logs } = harness({ maxSessions: 1 });
    router.bind(holder('c1'), 's1');

    expect(router.bind(holder('c2'), 's2')).toBeUndefined();

    const refused = logs().filter(
      (entry) => entry.msg === 'session binding refused: session table is full',
    );
    expect(refused).toHaveLength(1);
    expect(refused[0]).toMatchObject({
      level: 'warn',
      sessionId: 's2',
      connectionId: 'c2',
      limit: 1,
    });
    // The refused id left no route, and the refused holder gained nothing.
    expect(router.sessionOf('c2')).toBeUndefined();
  });

  test('a refused binding leaves the holder on its previous session', () => {
    const { router, holder } = harness({ maxSessions: 1 });
    const c1 = holder('c1');
    router.bind(c1, 's1');

    // Same holder, a new id: refused, so the old route must still stand.
    expect(router.bind(c1, 's2')).toBeUndefined();

    expect(router.sessionOf('c1')).toBe('s1');
  });

  test('rebinding an existing session is allowed at the cap', () => {
    const { router, holder, logs } = harness({ maxSessions: 1 });
    router.bind(holder('c1'), 's1');

    // The cap is on distinct Sessions, not on bindings: a reconnect must not be
    // refused just because the table is at its limit.
    const gate = router.bind(holder('c2'), 's1');

    expect(gate?.previousConnectionId).toBe('c1');
    expect(
      logs().filter(
        (entry) =>
          entry.msg === 'session binding refused: session table is full',
      ),
    ).toHaveLength(0);
  });

  test('clear drops every route', () => {
    const { router, holder } = harness();
    router.bind(holder('c1'), 's1');
    router.bind(holder('c2'), 's2');

    router.clear();

    expect(router.sessionOf('c1')).toBeUndefined();
    expect(router.sessionOf('c2')).toBeUndefined();
    // Nothing is inherited, so each id binds as a first binding.
    expect(router.bind(holder('c3'), 's1')).toBeUndefined();
    expect(router.bind(holder('c4'), 's2')).toBeUndefined();
  });

  test('binding a new session on one holder drops the old route', () => {
    const { router, holder } = harness();
    const c1 = holder('c1');
    router.bind(c1, 's1');

    router.bind(c1, 's2');

    expect(router.sessionOf('c1')).toBe('s2');
    // s1 is free, so another connection binds it without a handoff.
    expect(router.bind(holder('c2'), 's1')).toBeUndefined();
  });

  describe('waitForHandoff', () => {
    test('waits for the previous holder chain as it stands at wait time', async () => {
      const { router } = harness();
      const release = Promise.withResolvers<void>();
      let tail: Promise<unknown> = Promise.resolve();
      const previous: SessionHolder = {
        connectionId: 'c1',
        chain: () => tail,
        closed: () => Promise.resolve(),
      };
      const next: SessionHolder = {
        connectionId: 'c2',
        chain: () => Promise.resolve(),
        // Never closes: only the previous holder's chain can end the wait.
        closed: () => new Promise(() => {}),
      };

      router.bind(previous, 's1');
      const gate = router.bind(next, 's1');
      // The work the old connection accepted after the rebind is the work that
      // must have a terminal state first, so the live tail must be the one read.
      tail = release.promise;

      let done = false;
      const waiting = router.waitForHandoff(gate!, next).then(() => {
        done = true;
        return undefined;
      });
      await Promise.resolve();
      expect(done).toBe(false);

      release.resolve();
      await waiting;
      expect(done).toBe(true);
    });

    test('releases the wait when the new holder closes first', async () => {
      const { router } = harness();
      const closeNext = Promise.withResolvers<void>();
      const previous: SessionHolder = {
        connectionId: 'c1',
        // Never settles: only the new holder's closure can end the wait.
        chain: () => new Promise(() => {}),
        closed: () => Promise.resolve(),
      };
      const next: SessionHolder = {
        connectionId: 'c2',
        chain: () => Promise.resolve(),
        closed: () => closeNext.promise,
      };
      router.bind(previous, 's1');
      const gate = router.bind(next, 's1');

      const waiting = router.waitForHandoff(gate!, next);
      closeNext.resolve();
      // Resolves rather than hanging until the handoff bound: nobody can read
      // the result of a request on a closed connection.
      await waiting;
    });

    test('logs a warn and releases the wait on timeout', async () => {
      const { router, logs } = harness({ handoffTimeoutMs: 5 });
      const previous: SessionHolder = {
        connectionId: 'c1',
        // Never settles: the timeout is the only thing that can end the wait.
        chain: () => new Promise(() => {}),
        closed: () => Promise.resolve(),
      };
      const next: SessionHolder = {
        connectionId: 'c2',
        chain: () => Promise.resolve(),
        closed: () => new Promise(() => {}),
      };
      router.bind(previous, 's1');
      const gate = router.bind(next, 's1');

      // A handler nobody can preempt must not stall the session forever.
      await router.waitForHandoff(gate!, next);

      const timedOut = logs().filter(
        (entry) =>
          entry.msg === 'session handoff timed out; releasing the next request',
      );
      expect(timedOut).toHaveLength(1);
      expect(timedOut[0]).toMatchObject({
        level: 'warn',
        sessionId: 's1',
        previousConnectionId: 'c1',
        timeoutMs: 5,
      });
    });
  });
});
