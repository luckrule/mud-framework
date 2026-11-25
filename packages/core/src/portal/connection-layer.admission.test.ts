import { describe, expect, test } from 'bun:test';

import { sessionHarness } from '../testing/connection-layer.ts';

describe('ConnectionLayer connection admission (SPEC 6.1)', () => {
  test('refuses a new connection once maxConnections is reached', async () => {
    const { transport, layer, controlled, logs } = sessionHarness({
      maxConnections: 1,
    });
    await layer.start();

    transport.connect('c1');
    transport.connect('c2');

    expect(transport.closedWith('c2')).toEqual({
      code: 1013,
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
