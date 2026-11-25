import type {
  ExecutorHealth,
  ExecutorHealthState,
  RequestExecutor,
} from '../shared/executor.ts';
import { isRecord } from '../shared/guards.ts';

export type ConnectionHealthState =
  | 'idle'
  | 'starting'
  | 'ready'
  | 'degraded'
  | 'stopping'
  | 'stopped';

/**
 * A point-in-time view of the connection layer and the executor behind it
 * (SPEC 6.6). It goes stale immediately and is meant for deployment probes,
 * not for admitting individual requests.
 */
export interface ConnectionHealth {
  readonly state: ConnectionHealthState;
  readonly ready: boolean;
  readonly listening: boolean;
  readonly connections: number;
  readonly executor: ExecutorHealth;
}

export type ConnectionLayerStatus =
  | 'idle'
  | 'starting'
  | 'started'
  | 'stopping'
  | 'stopped';

const EXECUTOR_STATES: ReadonlySet<string> = new Set<ExecutorHealthState>([
  'idle',
  'starting',
  'ready',
  'recovering',
  'blocked',
  'closed',
  'unknown',
]);

const UNKNOWN: ExecutorHealth = { state: 'unknown', ready: false };

/**
 * Reads the executor's self-report without letting outside code break a probe:
 * a throw or a malformed value becomes `unknown`. Nothing is logged, because a
 * health endpoint is polled and a broken executor would flood the log.
 */
export function readExecutorHealth(executor: RequestExecutor): ExecutorHealth {
  try {
    const value: unknown = executor.health();
    return isExecutorHealth(value) ? value : UNKNOWN;
  } catch {
    return UNKNOWN;
  }
}

/** Derives the layer's snapshot from its status, admission count and executor. */
export function connectionHealthSnapshot(
  status: ConnectionLayerStatus,
  connections: number,
  executor: ExecutorHealth,
): ConnectionHealth {
  const listening = status === 'started';
  const ready = listening && executor.ready;
  const state: ConnectionHealthState =
    status === 'started' ? (ready ? 'ready' : 'degraded') : status;
  return { state, ready, listening, connections, executor };
}

function isExecutorHealth(value: unknown): value is ExecutorHealth {
  return (
    isRecord(value) &&
    typeof value.ready === 'boolean' &&
    typeof value.state === 'string' &&
    EXECUTOR_STATES.has(value.state)
  );
}
