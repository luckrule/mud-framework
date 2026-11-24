import { describe, expect, test } from 'bun:test';

import {
  failureOf,
  flushTicks,
  harness,
  readyMessage,
  request,
  requestMessages,
  serverAt,
  started,
  successMessage,
} from '../test-support/ipc-executor.ts';

const MISMATCH_LOG = 'server code version differs from the portal version';

describe('IpcExecutor code version', () => {
  test('stamps the configured code version on the first delivery', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const server = await started(h);

    const outcome = h.executor.execute(request({ requestId: 'req-1' }));

    const [delivered] = requestMessages(server);
    expect(delivered).toMatchObject({
      requestId: 'req-1',
      replayCount: 0,
      codeVersion: 'build-1',
    });

    server.send(successMessage('req-1', 'ok'));
    expect(await outcome).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('sends no code version when none is configured', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const outcome = h.executor.execute(request({ requestId: 'req-1' }));

    const [delivered] = requestMessages(server);
    expect(delivered?.codeVersion).toBeUndefined();

    server.send(successMessage('req-1', 'ok'));
    expect(await outcome).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('replays the version stamped at acceptance, not the replacement version', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const first = await started(h);

    const outcome = h.executor.execute(request({ requestId: 'req-1' }));
    expect(requestMessages(first)[0]?.codeVersion).toBe('build-1');

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    // The replacement runs a different build: the request still names the code
    // it was accepted under, which is what makes the mismatch observable.
    second.send(readyMessage(5000, 'build-2'));
    await flushTicks();

    const [replay] = requestMessages(second);
    expect(replay).toMatchObject({
      requestId: 'req-1',
      replayCount: 1,
      codeVersion: 'build-1',
    });

    second.send(successMessage('req-1', 'after restart'));
    expect(await outcome).toEqual({ ok: true, result: 'after restart' });
    await h.executor.close();
  });

  test('refuses an empty configured code version', () => {
    expect(() => harness({ timeoutMs: 5000, codeVersion: '' })).toThrow(
      'codeVersion must be a non-empty string',
    );
  });

  test('refuses a non-string configured code version at runtime', () => {
    expect(() =>
      harness({ timeoutMs: 5000, codeVersion: 7 as unknown as string }),
    ).toThrow('codeVersion must be a non-empty string');
  });

  test('fails a caller-supplied empty code version without delivering it', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const server = await started(h);

    const failure = failureOf(
      await h.executor.execute(
        request({ requestId: 'req-1', codeVersion: '' }),
      ),
    );

    expect(failure.code).toBe('INTERNAL_ERROR');
    expect(failure.message).toContain('codeVersion');
    expect(requestMessages(server)).toEqual([]);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request rejected' && line.code === 'INTERNAL_ERROR',
        ),
    ).toBe(true);
    await h.executor.close();
  });

  test('fails a non-string caller-supplied code version at runtime', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const failure = failureOf(
      await h.executor.execute(
        request({ requestId: 'req-1', codeVersion: 7 as unknown as string }),
      ),
    );

    expect(failure.code).toBe('INTERNAL_ERROR');
    expect(failure.message).toContain('codeVersion');
    expect(requestMessages(server)).toEqual([]);
    await h.executor.close();
  });

  test('sends a caller-supplied version instead of the configured one on first delivery and replay', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'configured' });
    const first = await started(h);

    const outcome = h.executor.execute(
      request({ requestId: 'req-1', codeVersion: 'caller' }),
    );
    expect(requestMessages(first)[0]?.codeVersion).toBe('caller');

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    second.send(readyMessage(5000));
    await flushTicks();

    expect(requestMessages(second)[0]).toMatchObject({
      requestId: 'req-1',
      replayCount: 1,
      codeVersion: 'caller',
    });

    second.send(successMessage('req-1', 'after restart'));
    expect(await outcome).toEqual({ ok: true, result: 'after restart' });
    await h.executor.close();
  });

  test('warns when ready reports a different code version', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    server.send(readyMessage(5000, 'build-2'));
    await startPromise;

    const warning = h.logs().find((line) => line.msg === MISMATCH_LOG);
    expect(warning).toMatchObject({
      level: 'warn',
      pid: 4242,
      expectedCodeVersion: 'build-1',
      actualCodeVersion: 'build-2',
    });
    await h.executor.close();
  });

  test('does not warn when ready matches the configured version', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    server.send(readyMessage(5000, 'build-1'));
    await startPromise;

    expect(h.logs().some((line) => line.msg === MISMATCH_LOG)).toBe(false);
    await h.executor.close();
  });

  test('does not warn when ready omits the code version', async () => {
    const h = harness({ timeoutMs: 5000, codeVersion: 'build-1' });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    // A deployment that declared no version says nothing; silence is not the
    // same as a different version.
    server.send(readyMessage());
    await startPromise;

    expect(h.logs().some((line) => line.msg === MISMATCH_LOG)).toBe(false);
    await h.executor.close();
  });
});
