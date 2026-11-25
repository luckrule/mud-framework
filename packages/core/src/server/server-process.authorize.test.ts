import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { CountingBackend } from '../testing/dispatcher.ts';
import {
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

describe('runServerProcess access policy', () => {
  test('forwards authorize to the dispatcher and refuses the request', async () => {
    const backend = new CountingBackend();
    let calls = 0;
    const harness = startServer({
      backend,
      authorize: () => false,
      extensions: [
        {
          name: 'game',
          register: (dispatcher) => {
            dispatcher.register(
              'look',
              async () => {
                calls += 1;
                return 'ok';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'look' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: {
          code: 'FORBIDDEN',
          message: 'access to "look" was denied',
        },
      },
    ]);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    await harness.stop();
  });
});
