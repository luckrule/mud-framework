import type { Logger } from '../logging/types.ts';
import type { Codec } from '../shared/codec.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import type { ConnectionState } from './connection-admission.ts';
import type {
  BacklogOverflow,
  EventWrite,
  QueueEventResult,
  SessionEventQueue,
} from './connection-events.ts';
import type { QueuedRequest } from './connection-writer.ts';
import type { EventBacklog } from './event-backlog.ts';
import type { EventDropLog } from './event-drop-log.ts';
import type { SessionRouter } from './session-router.ts';

/**
 * Everything the layer hands its portal modules, declared once. Each module
 * takes a `Pick` of it instead of redeclaring the fields it needs, so a field's
 * type has one home and the layer's assembly writes each field once. The
 * narrow `Pick`s keep a module from reaching for state it has no business
 * touching: reply never sees the connection table, delivery never sees the
 * executor.
 */
export interface LayerHost {
  /** The wire format, and the sink for failures that must not reach the transport. */
  readonly codec: Codec;
  readonly logger: Logger;
  /** The live connection table. Admission owns writes; delivery only reads. */
  readonly connections: Map<string, ConnectionState>;
  readonly maxConnections: number;
  readonly maxQueuedPerConnection: number;
  /** Frame bound of each event buffer (SPEC 3.8). */
  readonly maxQueuedEvents: number;
  /** Byte bound of each event buffer (SPEC 3.8). */
  readonly maxQueuedEventBytes: number;
  /** Layer-wide event totals every buffer reports to (SPEC 6.6). */
  readonly backlog: EventBacklog;
  /**
   * Aggregates repeated `EVENT_DROPPED` lines per reason and target (SPEC 7.1).
   * A missing target or a closed connection can recur at emission rate — a room
   * broadcasting to Sessions nobody holds — so one line per drop would make the
   * diagnostic channel a second unbounded stream.
   */
  readonly dropLog: EventDropLog;
  /** Mints the framework request id at the moment a request enters (SPEC 7.3). */
  readonly createRequestId: () => string;
  readonly router: SessionRouter;
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
  /**
   * Queues one encoded frame on the target connection's event queue. The queue
   * belongs to the connection's single writer, so the frame is written after
   * any request work already waiting (SPEC 3.8).
   */
  readonly queueEvent: (
    state: ConnectionState,
    write: EventWrite,
  ) => QueueEventResult;
  /**
   * Cancels a frame that has not started writing. False once it did: the
   * write's own bound decides when the serial slot is released.
   */
  readonly cancelEvent: (state: ConnectionState, write: EventWrite) => boolean;
  /**
   * Closes the connection owning an overflowing event buffer with `1013`
   * (SPEC 3.8). Passed in rather than implemented here because the close has to
   * mark the state before the transport reports the disconnect, so `onDisconnect`
   * can tell a backlog close from a client leaving. Returns false when this
   * connection is already being closed for backlog.
   */
  readonly closeForBacklog: (
    state: ConnectionState,
    overflow: BacklogOverflow,
  ) => boolean;
  /**
   * False once the layer started stopping. Events are best-effort and the
   * buffers are discarded at that point, so a late event is not accepted
   * (SPEC 3.8).
   */
  readonly acceptingEvents: () => boolean;
  /**
   * Pending queues of the Sessions the layer addresses, owned by the layer so
   * ordering and bound state is per layer (and per test) rather than
   * process-global. An entry exists while that Session has unfinished delivery
   * work, which is also when its serial slot is held.
   */
  readonly sessionQueues: Map<string, SessionEventQueue>;
  /**
   * Upper bound on one downlink write: a Server event frame and a response
   * frame alike (SPEC 3.8). A transport bounds its own sends, but the layer
   * cannot assume every implementation does: without this bound a send that
   * never settles would pin the Session's serial slot — or the connection's
   * single writer, and with it the Session's terminal barrier — forever.
   */
  readonly sendTimeoutMs: number;
}
