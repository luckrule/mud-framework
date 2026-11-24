import type { RequestMessage } from '../ipc/protocol.ts';
import type { ExecutorOutcome, ExecutorRequest } from './executor.ts';

/**
 * Builds the wire message for a request delivery or replay.
 *
 * `remainingMs` is passed in rather than read from the Portal's bookkeeping:
 * the Portal's deadline is authoritative, so the Server is given the time
 * actually left, not the original budget (INV-007). The caller computes it,
 * which keeps this free of `PendingRequest`'s shape.
 */
export function requestMessage(
  request: ExecutorRequest,
  remainingMs: number,
  replayCount: number,
): RequestMessage {
  return {
    // The identity fields are the request's own and mean the same on the wire,
    // so they are carried over rather than copied one by one.
    ...request,
    type: 'request',
    // A replay that arrives late must be able to roll back on its own instead
    // of writing after the Portal has already settled the request. The floor is
    // 1 because the protocol requires a positive `timeoutMs`.
    timeoutMs: Math.max(1, remainingMs),
    replayCount,
  };
}

export function failure(code: string, message: string): ExecutorOutcome {
  return { ok: false, error: { code, message } };
}

let portalGenerationCounter = 0;

/**
 * A process-wide increasing id, so a Server can tell a new Portal instance from
 * the one it handshook with even within the same pid.
 */
export function nextPortalGeneration(): number {
  portalGenerationCounter += 1;
  return portalGenerationCounter;
}
