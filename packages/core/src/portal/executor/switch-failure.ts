/**
 * The stage a failed switch stopped in. These are the switch's own phase
 * boundaries, not a second vocabulary: `preparing` is before the previous
 * process is touched, `retiring` is while it is drained and awaited, and
 * `activating` runs from `activate` through promotion.
 */
export type SwitchFailureStage = 'preparing' | 'retiring' | 'activating';

/**
 * What a failed `switchTo` left behind, in terms a caller can act on.
 *
 * The same rejection otherwise reads the same whether the old Server is still
 * serving (retry freely) or has been replaced by a rollback that is still
 * starting (retrying now would stack a second attempt on top of it).
 */
export interface SwitchFailureDetails {
  /** Where the switch stopped; see `SwitchFailureStage`. */
  readonly stage: SwitchFailureStage;
  /** The previous Server is still the current one and can serve. */
  readonly previousServing: boolean;
  /** The previous version is being restarted through the crash path. */
  readonly rolledBack: boolean;
  /** The replacement was promoted before the failure (a post-promotion loss). */
  readonly promoted: boolean;
  /**
   * `close()` interrupted the switch, so it owns the cleanup. Neither
   * `previousServing` nor `rolledBack` describes what happens next: the old
   * Server is still the current one, but the executor is shutting down and a
   * retry will be refused.
   */
  readonly closeAborted: boolean;
  /**
   * Processes whose exit the switch could not confirm. Non-zero means the
   * writer barrier is up and no restart may begin yet (INV-002).
   */
  readonly pendingExits: number;
  /** Last stderr lines of the failed process, when its spawn captured them. */
  readonly stderrTail?: readonly string[];
}

/**
 * A `switchTo` rejection. The message keeps the switch's own text so callers
 * that match on it keep working; the structured fields are additive, which is
 * what lets a caller decide whether a retry is safe without parsing that text.
 */
export class ServerSwitchError extends Error {
  readonly details: SwitchFailureDetails;

  constructor(message: string, details: SwitchFailureDetails) {
    super(message);
    this.name = 'ServerSwitchError';
    this.details = details;
  }
}

/**
 * `switchTo` failed with a `ServerSwitchError`. A guard rather than a bare
 * `instanceof`: a consumer may hold the error across a bundler boundary where
 * the class identity differs, so the shape is accepted too.
 */
export function isServerSwitchError(
  error: unknown,
): error is ServerSwitchError {
  return (
    error instanceof ServerSwitchError ||
    (error instanceof Error &&
      error.name === 'ServerSwitchError' &&
      'details' in error)
  );
}

/** Caps the stderr text one rejection message carries. */
const STDERR_TAIL_MESSAGE_LIMIT = 4000;

/**
 * Attaches the failed process's captured stderr to a switch rejection.
 *
 * Only the failure path calls this. Without it the real cause of a Server that
 * dies before the handshake — a syntax error, a missing module — exists only on
 * the Portal's console: the message says nothing about it and a caller has no
 * programmatic view. The message text is otherwise unchanged and only grows
 * when a spawn actually captured a tail. It is bounded because that text ends
 * up in a `reason` log field.
 */
export function withStderrTail(
  message: string,
  tail: readonly string[] | undefined,
): string {
  if (tail === undefined || tail.length === 0) {
    return message;
  }
  const kept: string[] = [];
  let size = 0;
  for (const line of tail.toReversed()) {
    if (kept.length > 0 && size + line.length > STDERR_TAIL_MESSAGE_LIMIT) {
      break;
    }
    kept.unshift(line);
    size += line.length + 1;
  }
  return `${message}\nserver stderr (last ${String(kept.length)} lines):\n${kept.join('\n')}`;
}
