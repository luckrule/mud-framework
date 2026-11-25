import type { ResultMessage } from '../../ipc/protocol.ts';
import { parseIpcMessage } from '../../ipc/protocol.ts';
import type { Logger } from '../../logging/types.ts';
import { messageOf } from '../../shared/guards.ts';
import type { ServerSlot, ServerState } from './server-slot.ts';
import type { SwitchOperation } from './version-switch.ts';

/**
 * The owner-side operations one inbound message may need. `ServerSupervisor`
 * implements them directly: each member is a real operation or live state, not
 * a getter over another object.
 */
export interface ServerMessageContext {
  readonly logger: Logger;
  /** Sole owner of `status`, `server` and the in-flight switch. */
  readonly slot: ServerSlot<SwitchOperation>;
  /**
   * Marks `server` live: the slot transition, the stability clock and the
   * backend's declared ceiling. Returns the ceiling now in force, which is what
   * the `server ready` line reports.
   */
  adoptReady(server: ServerState, declaredMaxDurationMs: number): number;
  /** Re-delivers the buffered requests to the current generation. */
  pump(): void;
  /** Releases a `start()` waiting for the readiness barrier. */
  resolveStart(): void;
  /** A `result` message: it settles through the buffer and the ledger. */
  onResult(message: ResultMessage): void;
  /** A send failed: the owner decides whether that is a crash or a retirement. */
  onServerLost(
    server: ServerState,
    code: number | null,
    signal: string | null,
    reason: string,
  ): void;
  /** Wakes the `closed` wait a `close()` may be parked on, for that Server only. */
  resolveClosed(server: ServerState): void;
}

/**
 * The Server→Portal half of the split: one raw message parsed, then dispatched
 * to the handshake, result or shutdown branch it belongs to.
 *
 * Pure dispatch: which process a message belongs to arrives as `server`, and
 * every consequence (activation, readiness, settlement, loss) is a context
 * operation.
 */
export function onServerMessage(
  server: ServerState,
  raw: unknown,
  context: ServerMessageContext,
): void {
  const parsed = parseIpcMessage(raw);
  if (!parsed.ok) {
    context.logger.warn(
      { pid: server.pid, reason: parsed.reason },
      'ipc message rejected',
    );
    return;
  }
  const message = parsed.message;
  switch (message.type) {
    case 'prepared':
      onPrepared(context, server, message.pid);
      return;
    case 'ready':
      onReady(
        context,
        server,
        message.pid,
        message.capabilities.maxTransactionDurationMs,
      );
      return;
    case 'result':
      context.onResult(message);
      return;
    case 'closed':
      onClosed(context, server, message.reason);
      return;
    case 'hello':
    case 'request':
    case 'cancel':
    case 'shutdown':
    case 'activate':
      // Valid shape, wrong direction: a Server must not send these.
      context.logger.warn(
        { pid: server.pid, reason: `unexpected "${message.type}" message` },
        'ipc message rejected',
      );
  }
}

function onClosed(
  context: ServerMessageContext,
  server: ServerState,
  reason: string,
): void {
  server.closedSignal.resolve();
  if (server.retiring) {
    // The previous process finished its graceful close; the switch waits on
    // its own signal for this before it may activate the replacement
    // (INV-002). A `close()` that took over may also be waiting, so wake it.
    server.shutdownAcknowledged = true;
    context.resolveClosed(server);
    return;
  }
  if (context.slot.status === 'closing') {
    server.shutdownAcknowledged = true;
    // Another generation's `closed` must not end a wait for the current Server.
    context.resolveClosed(server);
    return;
  }
  if (context.slot.switching?.staged === server) {
    // A prepared process has no storage to close, so this is not a shutdown
    // ack; the exit that follows is what the switch reacts to.
    context.logger.warn(
      { pid: server.pid, reason },
      'replacement server closed before activation',
    );
    return;
  }
  // Unsolicited: the process exit that follows is what actually matters,
  // and it arrives through `exited`/`onClose`. Not a shutdown ack, so
  // close() must still be able to kill a process that stays alive.
  context.logger.warn(
    { pid: server.pid, reason },
    'server closed without being asked',
  );
}

/**
 * The Server accepted the handshake and is waiting for activation (ADR-0007):
 * it holds no storage yet, so the only thing that can happen now is opening
 * it. Sending `activate` at once is the ordinary-start path; a `switchTo` owns
 * its staged process instead and activates it only after the previous one has
 * exited.
 */
function onPrepared(
  context: ServerMessageContext,
  server: ServerState,
  pid: number,
): void {
  const op = context.slot.switching;
  if (op?.staged === server) {
    server.pid = pid;
    op.prepared.seen = true;
    op.prepared.resolve();
    return;
  }
  if (context.slot.server !== server || server.closed) {
    context.logger.debug(
      { pid },
      'prepared from a server generation already given up',
    );
    return;
  }
  if (server.ready) {
    // Out of order: this generation already opened its storage.
    context.logger.warn({ pid }, 'prepared received after ready; ignored');
    return;
  }
  if (context.slot.isTerminal) {
    // Nothing may open storage while the Portal is closing: there would be
    // nowhere to drain it. The shutdown path kills this process.
    context.logger.debug({ pid }, 'prepared ignored while closing');
    return;
  }
  server.pid = pid;
  try {
    context.slot.assertNoOtherLiveServer(server);
    server.channel.send({ type: 'activate' });
  } catch (error) {
    context.onServerLost(
      server,
      null,
      null,
      `activate failed: ${messageOf(error)}`,
    );
    return;
  }
  context.logger.debug({ pid }, 'server prepared; activating');
}

function onReady(
  context: ServerMessageContext,
  server: ServerState,
  pid: number,
  declaredMaxDurationMs: number,
): void {
  const op = context.slot.switching;
  if (op?.staged === server) {
    server.ready = true;
    server.pid = pid;
    server.declaredMaxDurationMs = declaredMaxDurationMs;
    op.ready.seen = true;
    op.ready.resolve();
    return;
  }
  if (context.slot.server !== server || server.closed) {
    context.logger.debug(
      { pid },
      'ready from a server generation already given up',
    );
    return;
  }
  if (server.ready) {
    context.logger.debug({ pid }, 'duplicate ready ignored');
    return;
  }
  server.ready = true;
  server.pid = pid;
  server.declaredMaxDurationMs = declaredMaxDurationMs;
  if (context.slot.isTerminal) {
    return;
  }
  const effectiveTimeoutMs = context.adoptReady(server, declaredMaxDurationMs);
  context.logger.info({ pid, effectiveTimeoutMs }, 'server ready');
  context.resolveStart();
  context.pump();
}
