import type { RequestExecutor } from '../shared/executor.ts';
import type { ConnectionState } from './connection-state.ts';
import type { QueuedRequest } from './connection-writer.ts';
import type { EventDeliveryHost } from './event-frame.ts';

/**
 * The layer's full host, declared once. `EventDeliveryHost` is the base: the
 * delivery primitives name their own port so they do not depend on this
 * assembly, and this interface adds the fields the writer and request side
 * need. The narrow `Pick`s keep a module from reaching for state it has no
 * business touching: reply never sees the connection table, request never sees
 * delivery's queues.
 */
export interface LayerHost extends EventDeliveryHost {
  /** The live connection table. Admission owns writes; delivery only reads. */
  readonly connections: Map<string, ConnectionState>;
  readonly maxConnections: number;
  readonly maxQueuedPerConnection: number;
  /** Mints the framework request id at the moment a request enters (SPEC 7.3). */
  readonly createRequestId: () => string;
  readonly executor: RequestExecutor;
  /**
   * Queues request work on the connection's writer. Passed in rather than
   * reached for through the writer, so the writer's request queue has one owner.
   */
  readonly enqueue: (
    state: ConnectionState,
    work: () => Promise<void>,
    queued: QueuedRequest,
  ) => Promise<void>;
}
