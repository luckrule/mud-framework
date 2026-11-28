import { tick } from 'mud-framework-test-support';

import type { IpcMessage } from '../ipc/protocol.ts';
import type {
  ExecutorOutcome,
  ExecutorRequest,
  SessionChange,
} from '../shared/executor.ts';

/**
 * The in-memory channel delivers synchronously, but crash detection, the exit
 * wait, the restart timer and the ready handshake each cross a task boundary.
 * Five macrotask hops are enough for the whole chain, with no wall-clock wait.
 */
export async function flushTicks(): Promise<void> {
  await tick();
  await tick();
  await tick();
  await tick();
  await tick();
}

/** Enough microtask hops for a timer callback's continuation to finish. */
export async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

/**
 * Flushes microtasks until `condition` holds; throws after `maxRounds` to fail
 * loudly instead of hanging. A fixed flush count breaks whenever a wrapper adds
 * one async hop, so wait on the effect the test needs instead.
 */
export async function flushUntil(
  condition: () => boolean,
  maxRounds = 50,
): Promise<void> {
  const poll = async (round: number): Promise<void> => {
    if (condition()) {
      return;
    }
    if (round >= maxRounds) {
      throw new Error(
        `condition was not met after ${String(maxRounds)} microtask flushes: ${String(condition)}`,
      );
    }
    await flushMicrotasks();
    return poll(round + 1);
  };
  return poll(0);
}

/**
 * Waits for a condition that only a real timer can satisfy. The restart backoff
 * is a wall-clock delay now that `ready` no longer resets it, so a fixed number
 * of ticks is not enough for the second and later restarts.
 */
export async function waitFor(
  condition: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const poll = async (): Promise<void> => {
    if (condition()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error('condition was not met before the timeout');
    }
    await tick();
    return poll();
  };
  return poll();
}

export function preparedMessage(pid = 4242): IpcMessage {
  return { type: 'prepared', pid };
}

export function readyMessage(
  maxTransactionDurationMs = 5000,
  pid = 4242,
): IpcMessage {
  return {
    type: 'ready',
    pid,
    capabilities: {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: true,
      maxTransactionDurationMs,
    },
  };
}

export function successMessage(
  requestId: string,
  result: unknown,
  sessionChange?: SessionChange,
): IpcMessage {
  return sessionChange === undefined
    ? { type: 'result', requestId, ok: true, result }
    : { type: 'result', requestId, ok: true, result, sessionChange };
}

export function request(
  overrides: Partial<ExecutorRequest> = {},
): ExecutorRequest {
  return {
    requestId: 'req-1',
    connectionId: 'c1',
    clientRequestId: 'client-1',
    method: 'echo',
    payload: null,
    ...overrides,
  };
}

export function failureOf(outcome: ExecutorOutcome): {
  code: string;
  message: string;
} {
  if (outcome.ok) {
    throw new Error('expected a failure outcome');
  }
  return outcome.error;
}
