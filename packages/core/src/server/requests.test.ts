import { describe, expect, test } from 'bun:test';

import type { IpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { sleep, tick } from '../testing/common.ts';
import { parseLine, requestMessage } from '../testing/server-process.ts';
import { Dispatcher } from './dispatcher.ts';
import type { Handler, HandlerOptions } from './dispatcher.ts';
import { LocalExecutor } from './local-executor.ts';
import { RequestRunner } from './requests.ts';

interface RunnerHarness {
  readonly runner: RequestRunner;
  /** Messages the runner handed to `send`, in order. */
  readonly sent: IpcMessage[];
  /** Parsed log lines, in emission order; debug lines included. */
  readonly logs: () => Record<string, unknown>[];
  readonly register: (
    method: string,
    handler: Handler,
    options?: HandlerOptions,
  ) => void;
}

function startRunner(
  timeoutMs = 1000,
  accepting: () => boolean = () => true,
): RunnerHarness {
  const backend = new MemoryStorageBackend();
  const lines: string[] = [];
  const logger = createJsonLogger({
    // `request completed` and `cancel ignored` are debug lines; the runner's
    // bookkeeping is only observable if the harness keeps them.
    level: 'debug',
    sink: (line) => {
      lines.push(line);
    },
  });
  const dispatcher = new Dispatcher({ backend, timeoutMs, logger });
  const sent: IpcMessage[] = [];
  const runner = new RequestRunner({
    runtime: {
      dispatcher,
      executor: new LocalExecutor({ dispatcher }),
      capabilities: backend.capabilities(),
    },
    logger,
    send: (message) => {
      sent.push(message);
    },
    accepting,
  });
  return {
    runner,
    sent,
    logs: () => lines.map(parseLine),
    register: (method, handler, options) => {
      dispatcher.register(method, handler, options);
    },
  };
}

describe('RequestRunner', () => {
  test('accept runs the handler and answers with its result', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ payload: 'ping' }));
    await tick();

    expect(harness.sent).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'ping' },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({ requestId: 'req-1' });
  });

  test('echoes the entry session id on a success result', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'hi',
      },
    ]);
  });

  test('echoes the entry session id on a failure result', async () => {
    const harness = startRunner();
    harness.register('fail', () => Promise.reject(new Error('boom')), {
      access: 'public',
    });

    harness.runner.accept(
      requestMessage({ method: 'fail', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toHaveLength(1);
    expect(harness.sent[0]).toMatchObject({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: false,
      error: { code: 'INTERNAL_ERROR' },
    });
  });

  test('logs and relays an unbind change without inventing a session id', async () => {
    const harness = startRunner();
    harness.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return 'bye';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(
      requestMessage({ method: 'logout', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'bye',
        sessionChange: { type: 'unbind' },
      },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
      sessionChange: { type: 'unbind' },
    });
  });

  test('keeps a newly bound session distinct from the entry session', async () => {
    const harness = startRunner();
    harness.register(
      'login',
      async (ctx) => {
        ctx.bindSession('bound-1');
        return 'welcome';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(
      requestMessage({ method: 'login', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'welcome',
        sessionChange: { type: 'bind', sessionId: 'bound-1' },
      },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    // `sessionId` stays the entry Session; the new binding is the change.
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
  });

  test('logs no sessionId for a bind when the request entered without one', async () => {
    const harness = startRunner();
    harness.register(
      'login',
      async (ctx) => {
        ctx.bindSession('bound-1');
        return 'welcome';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(requestMessage({ method: 'login' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).not.toHaveProperty('sessionId');
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
  });

  test('logs the entry session without a sessionChange when nothing changed', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
    });
    expect(completed[0]).not.toHaveProperty('sessionChange');
  });

  test('omits requestSessionId when the request entered without a session', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });
    harness.register('fail', () => Promise.reject(new Error('boom')), {
      access: 'public',
    });

    harness.runner.accept(requestMessage());
    harness.runner.accept(
      requestMessage({ requestId: 'req-2', method: 'fail' }),
    );
    await tick();

    expect(harness.sent).toHaveLength(2);
    for (const message of harness.sent) {
      expect(message).not.toHaveProperty('requestSessionId');
    }
  });

  test('echoes the entry session id when refusing during shutdown', () => {
    const harness = startRunner(1000, () => false);

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: false,
        error: {
          code: 'FRAMEWORK_SHUTDOWN',
          message: 'server is shutting down',
        },
      },
    ]);
  });

  test('omits requestSessionId when refusing a request without a session', () => {
    const harness = startRunner(1000, () => false);

    harness.runner.accept(requestMessage());

    expect(harness.sent).toHaveLength(1);
    expect(harness.sent[0]).toMatchObject({
      ok: false,
      error: { code: 'FRAMEWORK_SHUTDOWN' },
    });
    expect(harness.sent[0]).not.toHaveProperty('requestSessionId');
  });

  test('request completed names the Extension that registered the handler', async () => {
    const harness = startRunner();
    harness.register('deposit', (_ctx, payload) => Promise.resolve(payload), {
      extension: 'banking',
      access: 'public',
    });

    harness.runner.accept(requestMessage({ method: 'deposit', payload: 'ok' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('request failed names the Extension that registered the handler', async () => {
    const harness = startRunner();
    harness.register(
      'withdraw',
      () => Promise.reject(new Error('insufficient funds')),
      { extension: 'banking', access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'withdraw' }));
    await tick();

    const failed = harness
      .logs()
      .filter((line) => line.msg === 'request failed');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      code: 'INTERNAL_ERROR',
      extension: 'banking',
    });
  });

  test('a handler no Extension registered logs no extension key', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ payload: 'ping' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    // An absent field, not `extension: undefined` (SPEC 7.1).
    expect(completed[0]).not.toHaveProperty('extension');
  });

  test('runs a duplicated request id once and logs the duplicate once', async () => {
    const harness = startRunner();
    let calls = 0;
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        calls += 1;
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow', replayCount: 0 }));
    harness.runner.accept(requestMessage({ method: 'slow', replayCount: 1 }));
    gate.resolve();
    await tick();

    expect(calls).toBe(1);
    expect(harness.sent).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'done' },
    ]);
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    expect(duplicates[0]).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      replayCount: 1,
    });
  });

  test('cancel of a running request invalidates its transaction', async () => {
    const harness = startRunner();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    let lateWriteError = '';
    harness.register(
      'slow',
      async (ctx) => {
        await ctx.storage.execute('INSERT 1');
        entered.resolve();
        await gate.promise;
        try {
          await ctx.storage.execute('INSERT 2');
          lateWriteError = 'write was accepted';
        } catch (error) {
          lateWriteError =
            error instanceof Error ? error.message : String(error);
        }
        return 'done';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await entered.promise;
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    // The write after the cancel is rejected, which only happens when
    // `dispatcher.cancel` invalidated the transaction; the handler still
    // returns, and its outcome is neither answered nor committed.
    expect(lateWriteError).toMatch(/expired/);
    expect(harness.sent).toEqual([]);
    const applied = harness
      .logs()
      .filter((line) => line.msg === 'cancel applied to a running handler');
    expect(applied).toHaveLength(1);
    expect(applied[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_TIMEOUT',
      requestId: 'req-1',
    });
  });

  test('cancel applied to a running handler names the Extension', async () => {
    const harness = startRunner();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        entered.resolve();
        await gate.promise;
        return 'done';
      },
      { extension: 'banking', access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await entered.promise;
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    const applied = harness
      .logs()
      .filter((line) => line.msg === 'cancel applied to a running handler');
    expect(applied).toHaveLength(1);
    expect(applied[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('a cancel before start names the Extension in both of its lines', async () => {
    const harness = startRunner();
    harness.register('slow', () => Promise.resolve('done'), {
      extension: 'banking',
      access: 'public',
    });

    harness.runner.accept(requestMessage({ method: 'slow' }));
    // No await in between: the cancel lands inside the pre-start grace period.
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    await tick();

    const logs = harness.logs();
    const beforeStart = logs.filter(
      (line) => line.msg === 'request cancelled before start',
    );
    const skipped = logs.filter(
      (line) => line.msg === 'cancelled request skipped',
    );
    expect(beforeStart).toHaveLength(1);
    expect(beforeStart[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
    expect(skipped).toHaveLength(1);
    expect(skipped[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('cancel of an unknown request id is a no-op', () => {
    const harness = startRunner();

    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-missing',
      code: 'FRAMEWORK_TIMEOUT',
    });

    expect(harness.sent).toEqual([]);
    const ignored = harness
      .logs()
      .filter((line) => line.msg === 'cancel ignored');
    expect(ignored).toHaveLength(1);
    expect(ignored[0]).toMatchObject({
      reason: 'no request in flight',
      requestId: 'req-missing',
    });
    // No id is remembered: the cancel neither leaked a set entry nor claimed
    // the id for a later delivery.
    expect(harness.runner.pending).toBe(0);
  });

  test('drain waits for a running handler', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await tick();
    let drained = false;
    const drain = harness.runner.drain(500).then(() => {
      drained = true;
      return undefined;
    });

    await sleep(10);
    expect(drained).toBe(false);
    gate.resolve();
    await drain;
    expect(drained).toBe(true);
    expect(harness.sent).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'done' },
    ]);
  });

  test('drain returns when the budget runs out and names what is left', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'stuck',
      async () => {
        await gate.promise;
        return 'late';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'stuck' }));
    await tick();
    await harness.runner.drain(10);

    const timeouts = harness
      .logs()
      .filter((line) => line.code === 'SHUTDOWN_DRAIN_TIMEOUT');
    expect(timeouts).toHaveLength(1);
    expect(timeouts[0]).toMatchObject({
      level: 'error',
      timeoutMs: 10,
      pendingRequests: 1,
      requestIds: ['req-1'],
    });
    gate.resolve();
    await tick();
  });

  test('counts pending and running requests over a request lifecycle', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );

    expect(harness.runner.pending).toBe(0);
    expect(harness.runner.running).toBe(0);

    harness.runner.accept(requestMessage({ method: 'slow' }));
    // Counted synchronously: a `shutdown` in the same batch must see the
    // request even before its handler started.
    expect(harness.runner.pending).toBe(1);
    expect(harness.runner.running).toBe(1);
    expect(harness.runner.pendingRequestIds).toEqual(['req-1']);

    await tick();
    expect(harness.runner.pending).toBe(1);
    expect(harness.runner.running).toBe(1);

    gate.resolve();
    await tick();
    expect(harness.runner.pending).toBe(0);
    expect(harness.runner.running).toBe(0);
    expect(harness.runner.pendingRequestIds).toEqual([]);
  });
});
