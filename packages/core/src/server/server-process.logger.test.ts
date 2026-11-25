import { describe, expect, test } from 'bun:test';

import { throwingLogger, tick } from '../testing/common.ts';
import {
  echoRegistration,
  received,
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

describe('ServerProcess logger safety', () => {
  test('reaches ready, answers a request and shuts down when the logger throws', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
      logger: throwingLogger(),
    });
    await tick();

    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);

    harness.portal.send(requestMessage());
    await tick();
    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'hi' },
    ]);

    harness.portal.send({ type: 'shutdown', timeoutMs: 100 });
    await harness.done;
  }, 2000);
});
