import type { ExecutorHealth } from '../../shared/executor.ts';

/** The executor statuses health classifies; `ServerSlot` remains their writer. */
export type IpcHealthStatus =
  | 'idle'
  | 'starting'
  | 'ready'
  | 'switching'
  | 'closing'
  | 'closed'
  | 'failed';

export interface IpcHealthFacts {
  readonly status: IpcHealthStatus;
  /** True once a Server completed the handshake at least once. */
  readonly handshakeCompleted: boolean;
  /** The current generation can serve now. */
  readonly serverReady: boolean;
  /** Recovery is stopped by an unconfirmed writer exit (INV-002). */
  readonly recoveryBlocked: boolean;
}

export function ipcExecutorHealth(facts: IpcHealthFacts): ExecutorHealth {
  if (facts.recoveryBlocked) {
    // The old writer's exit is unconfirmed, so no replacement may start
    // (INV-002). No timeout turns this into readiness.
    return { state: 'blocked', ready: false };
  }
  switch (facts.status) {
    case 'idle':
      return { state: 'idle', ready: false };
    case 'starting':
      // The same status covers the first `start()` and a restart after a loss;
      // only a completed handshake proves this is the latter.
      return {
        state: facts.handshakeCompleted ? 'recovering' : 'starting',
        ready: false,
      };
    case 'switching':
      // Requests are buffered until the replacement is promoted; nothing
      // reaches a Server until then.
      return { state: 'recovering', ready: false };
    case 'ready':
      return facts.serverReady
        ? { state: 'ready', ready: true }
        : { state: 'recovering', ready: false };
    case 'closing':
    case 'closed':
    case 'failed':
      // `failed` is terminal like `closed`: the start timeout stopped the
      // restart loop, so no execution can follow and the state set has no
      // separate failure member.
      return { state: 'closed', ready: false };
  }
  return { state: 'unknown', ready: false };
}
