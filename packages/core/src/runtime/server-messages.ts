import { messageOf } from '../internal/guards.ts';
import type { ResultMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { Logger } from '../logging/types.ts';
import type { ServerSlot, ServerState } from './server-slot.ts';
import type { SwitchOperation } from './version-switch.ts';

/**
 * What the Server→Portal handlers need from their owner. Every member reads or
 * calls back into live executor state, so a decision taken here is the owner's
 * state at that moment, not a snapshot.
 *
 * Deliberately narrow: the process state machine is only visible through
 * `slot`, and everything that is not a message-level decision — delivery,
 * settlement, crash recovery, restart backoff — stays behind a callback.
 */
export interface MessageHost {
  readonly logger: Logger;
  /** Sole owner of `status`, `server` and the in-flight switch. */
  readonly slot: ServerSlot<SwitchOperation>;
  /** The version new requests carry now; the expected one in a mismatch check. */
  currentCodeVersion(): string | undefined;
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
  /** Wakes the `closed` wait a `close()` may be parked on. */
  resolveClosed(): void;
}

/**
 * The Server→Portal half of the split: one raw message parsed, then dispatched
 * to the handshake, result or shutdown branch it belongs to.
 *
 * Kept out of the executor because none of it decides what happens next beyond
 * the message itself: which process a message belongs to arrives as `server`,
 * and every consequence (activation, readiness, settlement, loss) is a host
 * call.
 */
export function onServerMessage(
  host: MessageHost,
  server: ServerState,
  raw: unknown,
): void {
  const parsed = parseIpcMessage(raw);
  if (!parsed.ok) {
    host.logger.warn(
      { pid: server.pid, reason: parsed.reason },
      'ipc message rejected',
    );
    return;
  }
  const message = parsed.message;
  switch (message.type) {
    case 'prepared':
      onPrepared(host, server, message.pid, message.codeVersion);
      return;
    case 'ready':
      onReady(
        host,
        server,
        message.pid,
        message.capabilities.maxTransactionDurationMs,
        message.codeVersion,
      );
      return;
    case 'result':
      host.onResult(message);
      return;
    case 'closed':
      onClosed(host, server, message.reason);
      return;
    case 'hello':
    case 'request':
    case 'cancel':
    case 'shutdown':
    case 'activate':
      // Valid shape, wrong direction: a Server must not send these.
      host.logger.warn(
        { pid: server.pid, reason: `unexpected "${message.type}" message` },
        'ipc message rejected',
      );
  }
}

function onClosed(
  host: MessageHost,
  server: ServerState,
  reason: string,
): void {
  server.closedSignal.resolve();
  if (server.retiring) {
    // The previous process finished its graceful close; the switch waits on
    // its own signal for this before it may activate the replacement
    // (INV-002). A `close()` that took over may also be waiting, so wake it.
    server.shutdownAcknowledged = true;
    host.resolveClosed();
    return;
  }
  if (host.slot.status === 'closing') {
    server.shutdownAcknowledged = true;
    host.resolveClosed();
    return;
  }
  if (host.slot.switching?.staged === server) {
    // A prepared process has no storage to close, so this is not a shutdown
    // ack; the exit that follows is what the switch reacts to.
    host.logger.warn(
      { pid: server.pid, reason },
      'replacement server closed before activation',
    );
    return;
  }
  // Unsolicited: the process exit that follows is what actually matters,
  // and it arrives through `exited`/`onClose`. Not a shutdown ack, so
  // close() must still be able to kill a process that stays alive.
  host.logger.warn(
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
  host: MessageHost,
  server: ServerState,
  pid: number,
  codeVersion: string | undefined,
): void {
  const op = host.slot.switching;
  if (op?.staged === server) {
    server.pid = pid;
    server.reportedCodeVersion = codeVersion;
    warnOnVersionMismatch(
      host,
      pid,
      op.targetCodeVersion ?? host.currentCodeVersion(),
      codeVersion,
      op,
    );
    op.prepared.seen = true;
    op.prepared.resolve();
    return;
  }
  if (host.slot.server !== server || server.closed) {
    host.logger.debug(
      { pid },
      'prepared from a server generation already given up',
    );
    return;
  }
  if (server.ready) {
    // Out of order: this generation already opened its storage.
    host.logger.warn({ pid }, 'prepared received after ready; ignored');
    return;
  }
  if (host.slot.isTerminal) {
    // Nothing may open storage while the Portal is closing: there would be
    // nowhere to drain it. The shutdown path kills this process.
    host.logger.debug({ pid }, 'prepared ignored while closing');
    return;
  }
  server.pid = pid;
  server.reportedCodeVersion = codeVersion;
  try {
    server.channel.send({ type: 'activate' });
  } catch (error) {
    host.onServerLost(
      server,
      null,
      null,
      `activate failed: ${messageOf(error)}`,
    );
    return;
  }
  host.logger.debug({ pid }, 'server prepared; activating');
}

function onReady(
  host: MessageHost,
  server: ServerState,
  pid: number,
  declaredMaxDurationMs: number,
  actualCodeVersion: string | undefined,
): void {
  const op = host.slot.switching;
  if (op?.staged === server) {
    server.ready = true;
    server.pid = pid;
    server.declaredMaxDurationMs = declaredMaxDurationMs;
    server.reportedCodeVersion = actualCodeVersion;
    warnOnVersionMismatch(
      host,
      pid,
      op.targetCodeVersion ?? host.currentCodeVersion(),
      actualCodeVersion,
      op,
    );
    op.ready.seen = true;
    op.ready.resolve();
    return;
  }
  if (host.slot.server !== server || server.closed) {
    host.logger.debug(
      { pid },
      'ready from a server generation already given up',
    );
    return;
  }
  if (server.ready) {
    host.logger.debug({ pid }, 'duplicate ready ignored');
    return;
  }
  server.ready = true;
  server.pid = pid;
  server.declaredMaxDurationMs = declaredMaxDurationMs;
  server.reportedCodeVersion = actualCodeVersion;
  // Reported, not enforced: a Server built from different code may still be
  // the only process available, and refusing it would turn a deployment
  // mismatch into an outage (SPEC 6.4). A deployment that declared no version
  // says nothing, so silence is not a mismatch.
  warnOnVersionMismatch(
    host,
    pid,
    host.currentCodeVersion(),
    actualCodeVersion,
  );
  if (host.slot.isTerminal) {
    return;
  }
  const effectiveTimeoutMs = host.adoptReady(server, declaredMaxDurationMs);
  host.logger.info({ pid, effectiveTimeoutMs }, 'server ready');
  host.resolveStart();
  host.pump();
}

/**
 * Warns when a Server reports a code version other than the one expected
 * (SPEC 6.4). A missing value on either side says nothing, so silence is not
 * a mismatch. `op` records the warning so a switch does not log the same
 * mismatch for both `prepared` and `ready`.
 */
function warnOnVersionMismatch(
  host: MessageHost,
  pid: number,
  expected: string | undefined,
  actual: string | undefined,
  op?: SwitchOperation,
): void {
  if (expected === undefined || actual === undefined || actual === expected) {
    return;
  }
  if (op !== undefined) {
    if (op.versionWarned) {
      return;
    }
    op.versionWarned = true;
  }
  host.logger.warn(
    { pid, expectedCodeVersion: expected, actualCodeVersion: actual },
    'server code version differs from the portal version',
  );
}
