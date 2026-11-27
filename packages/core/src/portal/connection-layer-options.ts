import type { Logger } from '../logging/types.ts';
import type { Codec } from '../shared/codec.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import { describeValue } from '../shared/guards.ts';
import type { Transport } from '../transport/types.ts';

/**
 * SPEC 6.3 bounds the wait for in-flight work. 10s covers a healthy commit or
 * rollback with room to spare, and is short enough that a deploy hitting a
 * stuck handler looks slow rather than hung.
 */
export const DEFAULT_STOP_TIMEOUT_MS = 10_000;

/**
 * Slack added to the executor's own close budget. The executor's bound is
 * exact, so waiting precisely that long would race its last step (the bounded
 * exit wait) and abandon it on the tick it was about to finish; a second of
 * margin makes "the executor gave up" the observable outcome instead of "the
 * caller gave up first".
 */
export const EXECUTOR_CLOSE_SLACK_MS = 1000;

/**
 * Slack added to a transport's declared send bound to form this layer's write
 * bound (SPEC 3.8): one downlink write, an event frame and a response frame
 * alike.
 *
 * The transport's bound is the one that can explain a stalled write: its own
 * timeout logs the more specific failure and terminates the connection. Two
 * equal bounds race instead, and when this layer's timer wins the log says only
 * "the transport did not settle the write" — the operator cannot tell a stalled
 * socket from a broken transport timeout. Making this bound strictly wider
 * leaves the transport's verdict observable. Same order as
 * `SHUTDOWN_ACK_SLACK_MS`: enough for timer granularity and event-loop lag
 * between the two registrations, negligible against a send bound.
 */
export const EVENT_SEND_SLACK_MS = 250;

/**
 * Largest delay `setTimeout` accepts. Bun clamps anything above it to 1ms, so a
 * derived bound that overflowed this would release the writer almost at once —
 * the opposite of what the bound is for (SPEC 3.8). Exported so the option
 * tests can name the same ceiling rather than hard-code it.
 */
export const MAX_TIMER_DELAY_MS = 2_147_483_647;

/**
 * The bound on one downlink write when neither the caller nor the transport
 * supplies one (SPEC 3.8): an event frame and a response frame alike.
 * `Transport.sendTimeoutMs` is optional, so this case is reachable: without a
 * bound, a send that never settles would pin the Session's serial slot, and with
 * it the connection's single writer and the Session's terminal barrier, forever.
 * 30s matches the WS transport's own default, so a stack that declares nothing
 * behaves as it did before the declaration existed.
 *
 * Named for the case it covers rather than as a general default: delivery
 * receives the resolved number and must not carry a second fallback that could
 * disagree with this one. No slack is added here: nothing else bounds the
 * write, so there is no competing timer to lose to and the default is the only
 * verdict available.
 */
export const UNDECLARED_EVENT_SEND_TIMEOUT_MS = 30_000;

export interface ConnectionLayerOptions {
  transport: Transport;
  executor: RequestExecutor;
  /** Defaults to `JsonCodec`; swapping it swaps the wire format. */
  codec?: Codec;
  /**
   * Defaults to the JSON logger. Failures that must not reach the transport (a
   * write to a closed socket, a result that cannot be encoded) are recorded
   * here rather than dropped.
   */
  logger?: Logger;
  /**
   * Defaults to `randomUUID`. The id is minted here, not in the executor,
   * because a request enters the framework at this boundary and every log line
   * about it — including a failed execution — must carry the same id (SPEC 7.3).
   */
  createRequestId?: () => string;
  /**
   * Requests allowed to wait on one connection, excluding the one executing
   * (SPEC 3.5). Defaults to `DEFAULT_MAX_QUEUED_PER_CONNECTION`; the request
   * that would exceed it is refused with `FRAMEWORK_QUEUE_FULL`. Must be a
   * positive integer, so at least one waiting request is allowed; `0` is
   * invalid.
   */
  maxQueuedPerConnection?: number;
  /**
   * Server events allowed to wait in one event buffer (SPEC 3.8). The bound
   * applies to each connection's event queue and to each Session's pending
   * queue separately, not to a per-connection total. Defaults to
   * `DEFAULT_MAX_QUEUED_EVENTS`. Must be a positive integer.
   */
  maxQueuedEvents?: number;
  /**
   * The same bound in encoded frame bytes (SPEC 3.8), with the same per-queue
   * scope. Defaults to `DEFAULT_MAX_QUEUED_EVENT_BYTES`. Must be a positive
   * integer.
   */
  maxQueuedEventBytes?: number;
  /**
   * Aggregate event bytes above which the layer warns once (SPEC 3.8). The
   * per-queue bounds cap one slow reader each; this one observes what all of
   * them together hold, which is what the deployment has to fit in memory.
   * Defaults to `DEFAULT_EVENT_BACKLOG_WARN_BYTES`. Must be a positive integer.
   *
   * It is an observation, not an admission rule: crossing it logs
   * `EVENT_BACKLOG_HIGH` and nothing else. A global cap here would let one loud
   * emitter drop events for every connection, which is worse than the backlog.
   */
  eventBacklogWarnBytes?: number;
  /**
   * Ceiling on each shutdown wait: draining in-flight requests, closing the
   * executor and stopping the transport (SPEC 6.3). Defaults to
   * `DEFAULT_STOP_TIMEOUT_MS`. Must be a finite number of at least 1, no greater
   * than the largest usable timer delay; a longer value would be clamped to 1ms
   * and drop every queued request instead of waiting for it.
   *
   * This is a floor, not the executor's ceiling: when the executor can bound
   * its own `close()` (`RequestExecutor.closeBudgetMs`, which `IpcExecutor`
   * does), the executor step is given at least that long. The two used to be
   * independent, and a short value here made `stop()` return while the Server
   * process was still draining and writing. That derived budget is capped at
   * the largest usable timer delay, because it grows with this value and would
   * otherwise cross the ceiling this option itself is checked against.
   */
  shutdownTimeoutMs?: number;
  /**
   * Connections this layer admits at once (SPEC 6.1). Defaults to
   * `DEFAULT_MAX_CONNECTIONS`. This is an admission cap, not a business
   * Session lifetime limit: a connection beyond it is closed (code `1013`)
   * before any request on it is read. Must be a positive integer.
   */
  maxConnections?: number;
  /**
   * Upper bound on one downlink write: a Server event frame and a response
   * frame alike (SPEC 3.8). Defaults to the transport's declared
   * `Transport.sendTimeoutMs` plus `EVENT_SEND_SLACK_MS` when that declaration
   * is a finite number of at least 1, and to `UNDECLARED_EVENT_SEND_TIMEOUT_MS`
   * otherwise. Deriving the bound from the transport keeps the Session's serial
   * slot held until the transport itself would fail the write, so the slot is
   * never released while a frame is still being written (SPEC 3.8). The slack
   * makes this bound strictly wider than the transport's, so a stalled write is
   * reported by the transport's own timeout rather than by this layer's
   * fallback.
   *
   * An explicit value here is the layer's own bound and wins over the
   * declaration; no slack is added to it, because it replaces the derivation
   * rather than competing with a transport timer it was meant to outlast. Must
   * be a positive finite number no greater than the largest usable timer delay;
   * a longer value would be clamped to 1ms, turning the widest bound into the
   * narrowest.
   *
   * It bounds the write, not the disconnect: once a frame is being written an
   * abort does not release the Session's serial slot, because the bytes may
   * already be on the wire (SPEC 3.8).
   */
  eventSendTimeoutMs?: number;
}

/**
 * Resolves the bound on one downlink write — an event frame or a response
 * frame —: the layer's explicit option first, then the transport's declaration
 * plus `EVENT_SEND_SLACK_MS`, then this layer's default.
 *
 * A transport's value is only taken when it is usable (finite and at least 1).
 * `Infinity` is legal for a transport — `WsTransport` accepts it to disable its
 * own send timeout — but it must not become this bound: an unbounded wait would
 * pin the Session's serial slot, or the connection's single writer, forever, the
 * very failure the bound prevents. Such a declaration counts as "no
 * declaration" and falls back to the default. The fallback is a complete bound
 * of its own, so it takes no slack: nothing else bounds the write, and there is
 * no transport timer whose verdict the slack would preserve.
 *
 * An explicit option is used verbatim. It replaces the derivation rather than
 * standing beside it, so it carries no slack; a caller that wants the
 * transport's bound kept observable leaves the option unset.
 *
 * The derived sum is capped at the largest usable timer delay. A transport may
 * declare a value close to that ceiling, and the addition would push it past
 * the point where `setTimeout` clamps the delay to 1ms — turning the widest
 * possible bound into the narrowest one.
 */
export function resolveSendTimeoutMs(options: ConnectionLayerOptions): number {
  if (options.eventSendTimeoutMs !== undefined) {
    assertTimerDelay(options.eventSendTimeoutMs, 'eventSendTimeoutMs');
    return options.eventSendTimeoutMs;
  }
  const declared = options.transport.sendTimeoutMs;
  if (declared !== undefined && Number.isFinite(declared) && declared >= 1) {
    return Math.min(declared + EVENT_SEND_SLACK_MS, MAX_TIMER_DELAY_MS);
  }
  return UNDECLARED_EVENT_SEND_TIMEOUT_MS;
}

export function assertPositiveInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive integer, got ${describeValue(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(
      `${name} must be a positive integer, got ${String(value)}`,
    );
  }
}

function assertFiniteAtLeastOne(
  value: unknown,
  name: string,
): asserts value is number {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a finite number of at least 1, got ${describeValue(value)}`,
    );
  }
  if (!Number.isFinite(value) || value < 1) {
    throw new RangeError(
      `${name} must be a finite number of at least 1, got ${String(value)}`,
    );
  }
}

/**
 * A configured delay must also fit `setTimeout`. Above `MAX_TIMER_DELAY_MS` Bun
 * clamps the delay to 1ms, so the longest wait an operator can ask for becomes
 * the shortest: shutdown would drop every queued request, and a send bound would
 * release the writer while the frame may still be on the wire. Rejected rather
 * than capped because the value is the operator's stated intent; silently
 * shortening it is the same inversion, and a delay this far out of range is a
 * configuration error worth reporting rather than quietly repairing.
 *
 * Every caller of this is a timer delay, so the ceiling applies to all of them.
 */
export function assertTimerDelay(
  value: unknown,
  name: string,
): asserts value is number {
  assertFiniteAtLeastOne(value, name);
  if (value > MAX_TIMER_DELAY_MS) {
    throw new RangeError(
      `${name} must not exceed ${MAX_TIMER_DELAY_MS}ms (the largest setTimeout delay), got ${String(value)}`,
    );
  }
}
