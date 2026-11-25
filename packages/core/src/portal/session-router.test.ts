import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { parseLine } from '../testing/server-process.ts';
import { DEFAULT_MAX_SESSIONS, SessionRouter } from './session-router.ts';

function harness(maxSessions = DEFAULT_MAX_SESSIONS): {
  router: SessionRouter;
  bind: (connectionId: string, sessionId: string) => void;
  logs: () => Record<string, unknown>[];
} {
  const lines: string[] = [];
  const router = new SessionRouter({
    maxSessions,
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

  test('refuses a new session once the table is full', () => {
    const { router, bind, logs } = harness(1);
    bind('c1', 's1');
    bind('c2', 's2');

    expect(router.sessionOf('c2')).toBeUndefined();
    expect(
      logs().find(
        (entry) =>
          entry.msg === 'session binding refused: session table is full',
      ),
    ).toMatchObject({
      level: 'warn',
      sessionId: 's2',
      connectionId: 'c2',
      limit: 1,
    });
  });

  test('a refused binding leaves the holder on its previous session', () => {
    const { router, bind } = harness(1);
    bind('c1', 's1');
    bind('c1', 's2');

    expect(router.sessionOf('c1')).toBe('s1');
  });

  test('rebinding an existing session is allowed at the cap', () => {
    const { router, bind, logs } = harness(1);
    bind('c1', 's1');
    bind('c2', 's1');

    expect(router.sessionOf('c2')).toBe('s1');
    expect(
      logs().filter(
        (entry) =>
          entry.msg === 'session binding refused: session table is full',
      ),
    ).toHaveLength(0);
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
});
