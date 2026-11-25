import { describe, expect, test } from 'bun:test';

import { sleep } from '../testing/common.ts';
import { lastFrame, sessionHarness } from '../testing/connection-layer.ts';

describe('ConnectionLayer session routing (SPEC 3.2, INV-004)', () => {
  test('a bound session makes a later connection rebind it as a handoff', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    transport.connect('c2');
    await transport.receive('c2', '{"id":"2","method":"bind"}');

    // The route exists because the second bind was recognised as a reconnect
    // rather than a first binding.
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
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(2);
    await layer.stop();
  });

  test('rebinding the same session on one connection is idempotent', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    await transport.receive('c1', '{"id":"3","method":"bind"}');

    // A long-lived connection binds once; repeating it must not log per
    // request or turn into a self-handoff.
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(1);
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('binding a new session on one connection drops the old route', async () => {
    let bound = 's1';
    const { transport, layer, logs } = sessionHarness({
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    // c1 no longer holds s1, so this is a first binding, not a handoff.
    transport.connect('c2');
    bound = 's1';
    await transport.receive('c2', '{"id":"3","method":"bind"}');

    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('a reconnecting connection waits for the previous connection to settle', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);
    expect(controlled.started).toEqual(['bind', 'hang']);

    // The binding request itself is not delayed: it already executed, and its
    // result belongs to the client that sent it.
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    expect(lastFrame(transport, 'c2')).toEqual({
      id: '3',
      ok: true,
      result: 'bind',
    });

    const next = transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(10);
    // SPEC 3.2: the new connection's next request must not start while the
    // old connection still has work without a terminal state.
    expect(controlled.started).toEqual(['bind', 'hang', 'bind']);

    controlled.release();
    await Promise.all([hanging, next]);
    expect(controlled.started).toEqual(['bind', 'hang', 'bind', 'next']);
    // The handoff ordering is what matters: the old connection's work has a
    // terminal state before the new connection's next request starts.
    expect(controlled.finished.indexOf('hang')).toBeLessThan(
      controlled.started.lastIndexOf('next'),
    );
    await layer.stop();
  });

  test('a connection that lost its session and takes it back is gated again', async () => {
    const { transport, layer, controlled, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    // c2 takes the Session over, so c1 is no longer the holder.
    transport.connect('c2');
    await transport.receive('c2', '{"id":"2","method":"bind"}');
    const hanging = transport.receive('c2', '{"id":"3","method":"hang"}');
    await sleep(0);

    // c1 binding s1 again is a reconnect from c1's side, not an idempotent
    // rebind of its own route: it must be gated on c2, which still has work.
    await transport.receive('c1', '{"id":"4","method":"bind"}');
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(2);

    const next = transport.receive('c1', '{"id":"5","method":"next"}');
    await sleep(10);
    expect(controlled.started).not.toContain('next');

    controlled.release();
    await Promise.all([hanging, next]);
    expect(controlled.started).toContain('next');
    await layer.stop();
  });

  test('releases a reconnect wait that outlives the handoff timeout', async () => {
    const { transport, layer, controlled, logs } = sessionHarness({
      sessionHandoffTimeoutMs: 20,
    });
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    const next = transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(60);

    // A handler nobody can preempt must not stall the session forever.
    expect(controlled.started).toContain('next');
    const timedOut = logs().filter(
      (entry) =>
        entry.msg === 'session handoff timed out; releasing the next request',
    );
    expect(timedOut).toHaveLength(1);
    expect(timedOut[0]).toMatchObject({
      level: 'warn',
      sessionId: 's1',
      previousConnectionId: 'c1',
      timeoutMs: 20,
    });

    controlled.release();
    await Promise.all([hanging, next]);
    await layer.stop();
  });

  test('a disconnect removes the route so the next binding is not a handoff', async () => {
    const { transport, layer, controlled, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    // Left in flight on purpose: its chain never settles, so any gate would
    // be observable.
    const abandoned = transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);
    await transport.disconnect('c1', 'client left');

    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    const next = transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(10);

    // SPEC 6.1: the route went with the connection, so nothing is inherited.
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
    expect(controlled.started).toContain('next');

    controlled.release();
    await Promise.all([abandoned, next]);
    await layer.stop();
  });

  test('refuses a binding once the session table is full but still answers the request', async () => {
    let bound = 's1';
    const { transport, layer, logs } = sessionHarness({
      maxSessions: 1,
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');

    // The request succeeded: a routing limit is not a business failure.
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '2',
      ok: true,
      result: 'bind',
    });
    const refused = logs().filter(
      (entry) => entry.msg === 'session binding refused: session table is full',
    );
    expect(refused).toHaveLength(1);
    expect(refused[0]).toMatchObject({
      level: 'warn',
      sessionId: 's2',
      connectionId: 'c1',
      limit: 1,
    });
    // The refused id left no route behind.
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(1);
    await layer.stop();
  });

  test('passes the bound session to the executor on the next request', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    // The binding request itself runs unbound: the binding it declares is
    // applied only after it succeeds.
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    await transport.receive('c1', '{"id":"2","method":"next"}');

    expect(controlled.requests).toHaveLength(2);
    expect(controlled.requests[0]?.sessionId).toBeUndefined();
    // SPEC 7.1: the Session is the context's only source, so it must reach
    // the executor rather than stay in this layer's routing table.
    expect(controlled.requests[1]?.sessionId).toBe('s1');
    await layer.stop();
  });

  test('passes no session on the first request of an unbound connection', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"next"}');

    expect(controlled.requests).toHaveLength(1);
    expect(controlled.requests[0]?.sessionId).toBeUndefined();
    await layer.stop();
  });

  test('carries the session on the lines this layer does emit', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');

    // SPEC 7.1: request lines name the Session. Successful requests are not
    // logged here, so the observable line is the binding itself — it belongs
    // to the request that declared it and carries the id it just bound.
    const bound = logs().filter((line) => line.msg === 'session bound');
    expect(bound).toHaveLength(1);
    expect(bound[0]?.sessionId).toBe('s1');
    await layer.stop();
  });

  test('passes the newest session after a rebind', async () => {
    let bound = 's1';
    const { transport, layer, controlled } = sessionHarness({
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    await transport.receive('c1', '{"id":"3","method":"next"}');

    expect(controlled.requests.map((request) => request.sessionId)).toEqual([
      undefined,
      's1',
      's2',
    ]);
    await layer.stop();
  });

  test('passes the reconnected session to the new connection', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    transport.connect('c2');
    // A new connection starts unbound; the bind it sends is a handoff.
    await transport.receive('c2', '{"id":"2","method":"next"}');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    await transport.receive('c2', '{"id":"4","method":"next"}');

    expect(controlled.requests.map((request) => request.sessionId)).toEqual([
      undefined,
      undefined,
      // The rebinding request on the new connection runs before its own
      // binding is applied, exactly like a first bind.
      undefined,
      's1',
    ]);
    await layer.stop();
  });

  test('clears the session table on stop', async () => {
    const { transport, layer } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    // Bracket access: both the layer and the router keep the table private,
    // and shutdown leaves nothing a client could observe it through (the
    // connection is gone too).
    const sessions: Map<string, unknown> = layer['router']['routes'];
    expect(sessions.size).toBe(1);

    await layer.stop();

    expect(sessions.size).toBe(0);
  });
});
