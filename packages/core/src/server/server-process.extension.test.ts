import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { tick } from '../testing/common.ts';
import {
  echoRegistration,
  messageTypes,
  received,
  requestMessage,
  results,
  startServer,
  startupFailure,
} from '../testing/server-process.ts';

describe('runServerProcess extension capabilities', () => {
  test('registers an extension whose requirements the backend meets', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'full', concurrency: 'pooled' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register(
              'deposit',
              (_ctx, payload) => Promise.resolve({ deposited: payload }),
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'deposit' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: { deposited: 'hi' },
      },
    ]);
    const registered = harness
      .logs()
      .filter((line) => line.msg === 'extension registered');
    expect(registered).toHaveLength(1);
    expect(registered[0]).toMatchObject({
      level: 'info',
      extension: 'banking',
    });
    await harness.stop();
  });

  test('skips an extension whose requirements are unmet, and none of its handlers exist', async () => {
    // The defect this covers: a business requiring `transactions: full` ran
    // silently on a `none` backend (INV-010).
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'), {
              access: 'public',
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'deposit' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'UNKNOWN_METHOD' },
    });
    const skipped = harness
      .logs()
      .filter(
        (line) => line.msg === 'extension skipped: storage capabilities unmet',
      );
    expect(skipped).toHaveLength(1);
    expect(skipped[0]).toMatchObject({
      level: 'warn',
      extension: 'banking',
      unmet: ['transactions: needs full, backend declares none'],
    });
    // The process still started: skipping one extension is not a startup failure.
    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    await harness.stop();
  });

  test('a skipped extension does not stop later ones from registering', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'limited' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'), {
              access: 'public',
            });
          },
        },
        {
          name: 'queries',
          register: echoRegistration,
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ payload: 'later' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'later' },
    ]);
    await harness.stop();
  });

  test('a required extension with unmet requirements refuses startup', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'limited' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          required: true,
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'), {
              access: 'public',
            });
          },
        },
      ],
    });

    const error = await startupFailure(harness.done);

    // SPEC 6.2 step 2: the process cannot run without it, so no `ready` and a
    // ServerStartupError for the caller to map to a non-zero exit.
    expect(error.reason).toContain('banking');
    expect(messageTypes(harness.messages)).toEqual(['prepared']);
    const failures = harness
      .logs()
      .filter((line) => line.msg === 'required extension cannot start');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      level: 'error',
      extension: 'banking',
      unmet: ['transactions: needs full, backend declares limited'],
    });
  });

  test('starts with no extensions at all and warns about it', async () => {
    const harness = startServer({ extensions: [] });
    await tick();

    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    // No Extension was declared, so nothing was skipped; the all-skipped line
    // is about declared-but-refused extensions, not an empty list.
    expect(
      harness
        .logs()
        .filter(
          (line) =>
            line.msg === 'no extension registered: every extension was skipped',
        ),
    ).toHaveLength(0);
    await harness.stop();
  });

  test('warns when every declared extension was skipped', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none', concurrency: 'single' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: () => undefined,
        },
        {
          name: 'reporting',
          requires: { concurrency: 'pooled' },
          register: () => undefined,
        },
      ],
    });
    await tick();

    // A Server with zero handlers is useless but must not crash: Portal is
    // waiting on it, and UNKNOWN_METHOD is a diagnosable answer.
    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    const warnings = harness
      .logs()
      .filter(
        (line) =>
          line.msg === 'no extension registered: every extension was skipped',
      );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({ level: 'warn', total: 2, skipped: 2 });
    await harness.stop();
  });
});
