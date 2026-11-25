import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { parseLine } from '../testing/server-process.ts';
import { SessionRouter } from './session-router.ts';

function harness(): {
  router: SessionRouter;
  bind: (connectionId: string, sessionId: string) => void;
  logs: () => Record<string, unknown>[];
} {
  const lines: string[] = [];
  const router = new SessionRouter({
    logger: createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    }),
  });
  return {
    router,
    bind: (connectionId, sessionId) => {
      router.bind(connectionId, sessionId, Promise.resolve());
    },
    logs: () => lines.map(parseLine),
  };
}

describe('SessionRouter ownership', () => {
  test('the first binding establishes ownership', () => {
    const { router, bind, logs } = harness();
    bind('c1', 's1');

    expect(router.sessionOf('c1')).toBe('s1');
    expect(logs().map((entry) => entry.msg)).toEqual(['session bound']);
  });

  test('rebinding the same session on the same holder is idempotent', () => {
    const { router, bind, logs } = harness();
    bind('c1', 's1');
    bind('c1', 's1');
    bind('c1', 's1');

    expect(router.sessionOf('c1')).toBe('s1');
    expect(logs().map((entry) => entry.msg)).toEqual(['session bound']);
  });

  test('a takeover revokes the previous holder', () => {
    const { router, bind, logs } = harness();
    bind('c1', 's1');
    bind('c2', 's1');

    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.sessionOf('c1')).toBeUndefined();
    expect(
      logs().find(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toMatchObject({
      level: 'warn',
      sessionId: 's1',
      previousConnectionId: 'c1',
      connectionId: 'c2',
    });
  });

  test('release removes only the connection route', () => {
    const { router, bind } = harness();
    bind('c1', 's1');
    router.release('c1');

    expect(router.sessionOf('c1')).toBeUndefined();
    bind('c2', 's1');
    expect(router.sessionOf('c2')).toBe('s1');
  });

  test('release is safe for a connection that holds nothing', () => {
    const { router } = harness();
    router.release('never-seen');
    router.release('never-seen');

    expect(router.sessionOf('never-seen')).toBeUndefined();
  });

  test('releasing a displaced connection keeps the successor route', () => {
    const { router, bind } = harness();
    bind('c1', 's1');
    bind('c2', 's1');
    router.release('c1');

    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.sessionOf('c1')).toBeUndefined();
  });

  test('clear releases every route', () => {
    const { router, bind } = harness();
    bind('c1', 's1');
    bind('c2', 's2');
    router.clear();

    expect(router.sessionOf('c1')).toBeUndefined();
    expect(router.sessionOf('c2')).toBeUndefined();
  });

  test('binding a new session releases the old route', () => {
    const { router, bind } = harness();
    bind('c1', 's1');
    bind('c1', 's2');

    expect(router.sessionOf('c1')).toBe('s2');
    bind('c2', 's1');
    expect(router.sessionOf('c2')).toBe('s1');
  });

  test('unbind removes the route and logs the session it ended', () => {
    const { router, bind, logs } = harness();
    bind('c1', 's1');

    router.unbind('c1');

    expect(router.sessionOf('c1')).toBeUndefined();
    const unbound = logs().filter((entry) => entry.msg === 'session unbound');
    expect(unbound).toHaveLength(1);
    expect(unbound[0]).toMatchObject({
      level: 'info',
      sessionId: 's1',
      connectionId: 'c1',
    });
  });

  test('unbind reports a takeover instead of releasing the successor route', () => {
    const { router, bind } = harness();
    bind('c1', 's1');

    // The old connection no longer holds anything, but the Session is routed:
    // releasing it here would detach the successor (SPEC 3.2).
    expect(router.unbind('c1', 's1')).toBe('unbound');
    expect(router.sessionOf('c1')).toBeUndefined();
    bind('c2', 's1');
    expect(router.unbind('c1', 's1')).toBe('held-by-other');
    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.unbind('c1', 'unrouted')).toBe('not-held');
    expect(router.unbind('c1')).toBe('not-held');
  });

  test('unbind is a no-op for a connection that holds nothing', () => {
    const { router, logs } = harness();

    // A repeated logout is not a failure.
    expect(() => router.unbind('never-seen')).not.toThrow();
    router.unbind('never-seen');

    expect(
      logs().filter((entry) => entry.msg === 'session unbound'),
    ).toHaveLength(0);
  });

  test('unbind releases the route but keeps the unfinished session barrier', async () => {
    const { router, bind } = harness();
    bind('c1', 's1');
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let oldWorkDone = false;
    const oldRun = router.run('c1', new AbortController().signal, async () => {
      entered.resolve();
      await release.promise;
      oldWorkDone = true;
    });
    await entered.promise;

    router.unbind('c1');
    expect(router.sessionOf('c1')).toBeUndefined();
    // A different connection takes the Session while the first request is
    // still running: the terminal barrier must outlive the route (INV-004).
    bind('c2', 's1');
    let newStarted = false;
    const newRun = router.run('c2', new AbortController().signal, async () => {
      newStarted = true;
    });
    await Promise.resolve();
    expect(newStarted).toBe(false);

    release.resolve();
    await Promise.all([oldRun, newRun]);
    expect(oldWorkDone).toBe(true);
    expect(newStarted).toBe(true);
  });
});
