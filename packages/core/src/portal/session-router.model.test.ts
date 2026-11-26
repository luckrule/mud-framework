import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { SessionRouter } from './session-router.ts';
import type { UnbindResult } from './session-router.ts';

/**
 * `RoutingModel` mirrors the implementation's two-map design (session →
 * connection and connection → session). It is a regression oracle for that
 * implementation, not an independent spec oracle: a wrong design repeated in
 * both would still match. The barrier checks here and the hand-written
 * scenarios in `session-router.test.ts` are the independent assertions.
 */

const CONNECTIONS = ['c1', 'c2', 'c3', 'c4'] as const;
const SESSIONS = ['s1', 's2', 's3'] as const;

type Event =
  | { kind: 'bind'; connectionId: string; sessionId: string }
  | { kind: 'unbind'; connectionId: string; sessionId?: string }
  | { kind: 'release'; connectionId: string };

/** Deterministic PRNG; no dependency and no shared state between sequences. */
function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(values: readonly T[], random: () => number): T {
  return values[Math.floor(random() * values.length)];
}

/** The routing table the implementation must match, written from the spec. */
class RoutingModel {
  private readonly routes = new Map<string, string>();
  private readonly heldBy = new Map<string, string>();

  bind(connectionId: string, sessionId: string): void {
    const previousConnectionId = this.routes.get(sessionId);
    if (previousConnectionId === connectionId) {
      return;
    }
    this.release(connectionId);
    if (previousConnectionId !== undefined) {
      this.heldBy.delete(previousConnectionId);
    }
    this.routes.set(sessionId, connectionId);
    this.heldBy.set(connectionId, sessionId);
  }

  release(connectionId: string): void {
    const sessionId = this.heldBy.get(connectionId);
    if (sessionId === undefined) {
      return;
    }
    this.heldBy.delete(connectionId);
    if (this.routes.get(sessionId) === connectionId) {
      this.routes.delete(sessionId);
    }
  }

  unbind(connectionId: string, sessionId?: string): UnbindResult {
    if (this.heldBy.has(connectionId)) {
      this.release(connectionId);
      return 'unbound';
    }
    if (sessionId !== undefined && this.routes.has(sessionId)) {
      return 'held-by-other';
    }
    return 'not-held';
  }

  sessionOf(connectionId: string): string | undefined {
    return this.heldBy.get(connectionId);
  }

  connectionOf(sessionId: string): string | undefined {
    return this.routes.get(sessionId);
  }
}

function createRouter(): SessionRouter {
  return new SessionRouter({
    logger: createJsonLogger({ sink: () => undefined }),
  });
}

function randomEvent(random: () => number): Event {
  const connectionId = pick(CONNECTIONS, random);
  const roll = random();
  if (roll < 0.45) {
    return { kind: 'bind', connectionId, sessionId: pick(SESSIONS, random) };
  }
  if (roll < 0.75) {
    return random() < 0.6
      ? { kind: 'unbind', connectionId, sessionId: pick(SESSIONS, random) }
      : { kind: 'unbind', connectionId };
  }
  return { kind: 'release', connectionId };
}

function describeEvent(event: Event): string {
  if (event.kind === 'bind') {
    return `bind ${event.connectionId} ${event.sessionId}`;
  }
  if (event.kind === 'unbind') {
    return `unbind ${event.connectionId} ${event.sessionId ?? '-'}`;
  }
  return `release ${event.connectionId}`;
}

function applyEvent(
  router: SessionRouter,
  model: RoutingModel,
  event: Event,
): void {
  switch (event.kind) {
    case 'bind':
      router.bind(event.connectionId, event.sessionId, Promise.resolve());
      model.bind(event.connectionId, event.sessionId);
      return;
    case 'unbind': {
      const expected = model.unbind(event.connectionId, event.sessionId);
      const actual = router.unbind(event.connectionId, event.sessionId);
      if (actual !== expected) {
        throw new Error(
          `unbind ${event.connectionId} ${event.sessionId ?? '-'} returned ${actual}, model expected ${expected}`,
        );
      }
      return;
    }
    case 'release':
      router.release(event.connectionId);
      model.release(event.connectionId);
  }
}

function assertMatches(router: SessionRouter, model: RoutingModel): void {
  for (const connectionId of CONNECTIONS) {
    const actual = router.sessionOf(connectionId);
    const expected = model.sessionOf(connectionId);
    if (actual !== expected) {
      throw new Error(
        `connection -> session mismatch for ${connectionId}: actual ${actual}, model ${expected}`,
      );
    }
  }
  for (const sessionId of SESSIONS) {
    const actual = router.connectionOf(sessionId);
    const expected = model.connectionOf(sessionId);
    if (actual !== expected) {
      throw new Error(
        `session -> connection mismatch for ${sessionId}: actual ${actual}, model ${expected}`,
      );
    }
  }
}

function assertInvariants(router: SessionRouter, model: RoutingModel): void {
  // 1. The two maps are inverse maps of each other.
  for (const connectionId of CONNECTIONS) {
    const sessionId = model.sessionOf(connectionId);
    if (
      sessionId !== undefined &&
      model.connectionOf(sessionId) !== connectionId
    ) {
      throw new Error(`model is not inverse around ${connectionId}`);
    }
  }
  for (const sessionId of SESSIONS) {
    const connectionId = model.connectionOf(sessionId);
    if (
      connectionId !== undefined &&
      model.sessionOf(connectionId) !== sessionId
    ) {
      throw new Error(`model is not inverse around ${sessionId}`);
    }
  }

  // 2. One holder per connection and one connection per Session.
  const heldSessions = CONNECTIONS.map((id) => router.sessionOf(id)).filter(
    (id): id is string => id !== undefined,
  );
  expect(new Set(heldSessions).size).toBe(heldSessions.length);
  const routedConnections = SESSIONS.map((id) =>
    router.connectionOf(id),
  ).filter((id): id is string => id !== undefined);
  expect(new Set(routedConnections).size).toBe(routedConnections.length);

  // 3. The barrier count is never negative.
  for (const sessionId of SESSIONS) {
    expect(router.pendingCount(sessionId)).toBeGreaterThanOrEqual(0);
  }
}

function failWithSequence(
  error: unknown,
  seed: number,
  events: readonly string[],
): never {
  const detail = error instanceof Error ? error.message : String(error);
  throw new Error(
    `SessionRouter model mismatch (seed=${seed})\nevents:\n${events.join('\n')}\n${detail}`,
    { cause: error },
  );
}

describe('SessionRouter model', () => {
  test('800 random bind/unbind/release sequences match the inverse maps', () => {
    for (let sequence = 0; sequence < 800; sequence += 1) {
      const seed = Math.imul(sequence + 1, 0x9e3779b1) >>> 0;
      const random = mulberry32(seed);
      const router = createRouter();
      const model = new RoutingModel();
      const events: string[] = [];
      for (let step = 0; step < 200; step += 1) {
        const event = randomEvent(random);
        events.push(describeEvent(event));
        try {
          applyEvent(router, model, event);
          assertMatches(router, model);
          assertInvariants(router, model);
        } catch (error) {
          failWithSequence(error, seed, events);
        }
      }
    }
  });

  test('200 random route changes never drop an unfinished barrier', async () => {
    for (let sequence = 0; sequence < 200; sequence += 1) {
      const seed = Math.imul(sequence + 1, 0x85ebca6b) >>> 0;
      const random = mulberry32(seed);
      const router = createRouter();
      const model = new RoutingModel();
      const target = pick(CONNECTIONS, random);
      const sessionId = pick(SESSIONS, random);
      const events: string[] = [`start run ${target} ${sessionId}`];
      router.bind(target, sessionId, Promise.resolve());
      model.bind(target, sessionId);
      await Promise.resolve();
      await Promise.resolve();

      const gate = Promise.withResolvers<void>();
      let startedSession: string | undefined;
      const running = router.run(
        target,
        new AbortController().signal,
        async (session) => {
          startedSession = session;
          await gate.promise;
        },
      );

      try {
        expect(startedSession).toBe(sessionId);
        for (let step = 0; step < 80; step += 1) {
          const event = randomEvent(random);
          events.push(describeEvent(event));
          applyEvent(router, model, event);
          await Promise.resolve();
          await Promise.resolve();
          assertMatches(router, model);
          assertInvariants(router, model);
          // 4. Takeover, unbind and disconnect keep the unfinished barrier.
          if (router.pendingCount(sessionId) < 1) {
            throw new Error(
              `barrier for ${sessionId} was dropped while a request was unfinished`,
            );
          }
        }
        gate.resolve();
        await running;
        await Promise.resolve();
        await Promise.resolve();
        // 5. Finishing a request does not change the route.
        assertMatches(router, model);
        expect(router.pendingCount(sessionId)).toBe(0);
      } catch (error) {
        gate.resolve();
        await running;
        failWithSequence(error, seed, events);
      }
    }
  });

  test('route changes never alter the barrier and finishing never moves the route', async () => {
    const router = createRouter();
    router.bind('c1', 's1', Promise.resolve());
    await Promise.resolve();
    await Promise.resolve();

    const gate = Promise.withResolvers<void>();
    const running = router.run('c1', new AbortController().signal, async () => {
      await gate.promise;
    });
    expect(router.pendingCount('s1')).toBe(1);

    router.unbind('c1', 's1');
    expect(router.sessionOf('c1')).toBeUndefined();
    expect(router.pendingCount('s1')).toBe(1);

    router.bind('c2', 's1', Promise.resolve());
    await Promise.resolve();
    await Promise.resolve();
    expect(router.sessionOf('c2')).toBe('s1');
    expect(router.pendingCount('s1')).toBe(1);

    router.release('c1');
    router.release('c2');
    expect(router.pendingCount('s1')).toBe(1);

    // Re-establish a route, finish the request, and prove the route survives.
    router.bind('c3', 's1', Promise.resolve());
    await Promise.resolve();
    await Promise.resolve();
    gate.resolve();
    await running;
    await Promise.resolve();
    await Promise.resolve();

    expect(router.pendingCount('s1')).toBe(0);
    expect(router.sessionOf('c3')).toBe('s1');
    expect(router.connectionOf('s1')).toBe('c3');
  });
});
