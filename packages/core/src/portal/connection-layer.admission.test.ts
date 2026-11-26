import { describe, expect, test } from 'bun:test';

import { CONNECTION_LIMIT_CLOSE_CODE } from '../shared/close-codes.ts';
import { sessionHarness } from '../testing/connection-layer.ts';

describe('ConnectionLayer connection admission (SPEC 6.1)', () => {
  test('pins the close code to the value SPEC 6.1 mandates', () => {
    // SPEC 6.1 says the admission refusal MUST pass `1013` (Try Again Later) to
    // the transport. The behavioral test below compares against the constant so
    // it reads in domain terms; on its own it would still pass if the constant
    // were changed to any other number. This literal is the contract itself:
    // changing it requires changing SPEC 6.1 first.
    expect(CONNECTION_LIMIT_CLOSE_CODE).toBe(1013);
  });

  test('refuses a new connection once maxConnections is reached', async () => {
    const { transport, layer, controlled, logs } = sessionHarness({
      maxConnections: 1,
    });
    await layer.start();

    transport.connect('c1');
    transport.connect('c2');

    // SPEC 6.1 mandates 1013 here; the constant names it.
    expect(transport.closedWith('c2')).toEqual({
      code: CONNECTION_LIMIT_CLOSE_CODE,
      reason: 'connection limit reached',
    });
    // The refused connection never reaches the executor or receives a frame.
    expect(controlled.requests).toEqual([]);
    expect(transport.sentTo('c2')).toEqual([]);
    expect(() => transport.receive('c2', '{"id":"2","method":"next"}')).toThrow(
      /closed/,
    );
    const refused = logs().filter(
      (entry) => entry.msg === 'connection refused: connection limit reached',
    );
    expect(refused).toHaveLength(1);
    expect(refused[0]).toMatchObject({
      level: 'warn',
      connectionId: 'c2',
      limit: 1,
    });

    // The admitted connection keeps working.
    await transport.receive('c1', '{"id":"1","method":"next"}');
    expect(controlled.requests.map((request) => request.connectionId)).toEqual([
      'c1',
    ]);
    await layer.stop();
  });

  test('frees a connection slot when a connection disconnects', async () => {
    const { transport, layer, controlled } = sessionHarness({
      maxConnections: 1,
    });
    await layer.start();

    transport.connect('c1');
    await transport.disconnect('c1');
    transport.connect('c2');

    expect(transport.closedWith('c2')).toBeUndefined();
    await transport.receive('c2', '{"id":"1","method":"next"}');
    expect(controlled.requests.map((request) => request.connectionId)).toEqual([
      'c2',
    ]);
    await layer.stop();
  });
});
