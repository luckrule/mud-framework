/**
 * The single validator for `StorageCapabilities`, shared by the two places that
 * check a declaration: the Server's self-check of its own backend and Portal's
 * parse of the `ready` message.
 *
 * The inputs differ in one respect only: a backend states `Infinity` when it has
 * no ceiling, while on the wire "unbounded" is `null` because JSON (the
 * cross-Node fallback in ADR-0001) turns `Infinity` into `null`. Field list,
 * enum values and error wording live here once, so the Server cannot accept a
 * declaration Portal then rejects: that mismatch would leave the Server ready
 * and the Portal waiting for a barrier that never arrives.
 */
import type { StorageCapabilities } from './types.ts';

/** Transaction modes a backend may declare (SPEC 4.2). */
export const TRANSACTION_MODES = ['full', 'limited', 'none'] as const;

/** Concurrency modes a backend may declare (SPEC 4.2). */
export const CONCURRENCY_MODES = ['single', 'pooled'] as const;

export type CapabilityCheck =
  | { readonly ok: true; readonly capabilities: StorageCapabilities }
  | { readonly ok: false; readonly reason: string };

/** The only place the two entries below differ. */
interface DurationRule {
  /** Expected value as it appears in a rejection reason. */
  readonly expected: string;
  /** Whether `null` (wire "unbounded") is accepted and normalised. */
  readonly acceptsNull: boolean;
}

const BACKEND_DURATION: DurationRule = {
  expected: 'a positive finite number or Infinity',
  acceptsNull: false,
};

const WIRE_DURATION: DurationRule = {
  expected: 'a positive number or null',
  acceptsNull: true,
};

/**
 * Validates a backend's own declaration before it is trusted (SPEC 4.2,
 * INV-009). Every field is required: a missing or malformed one would turn a
 * capability check into an `undefined` comparison that silently passes.
 *
 * `null` is refused even though the wire entry accepts it: a backend states
 * `Infinity` for "no ceiling", and only the JSON wire has to encode it as
 * `null`.
 */
export function checkBackendCapabilities(
  declared: StorageCapabilities,
): CapabilityCheck {
  return validate(declared, BACKEND_DURATION, 'capabilities');
}

/**
 * Validates a `ready.capabilities` value straight off the IPC channel, where
 * "unbounded" arrives as `null` and is normalised back to `Infinity`.
 *
 * `path` names the value in a rejection reason, e.g. `ready.capabilities`.
 */
export function checkWireCapabilities(
  raw: unknown,
  path: string,
): CapabilityCheck {
  return validate(raw, WIRE_DURATION, path);
}

/**
 * Internal control flow; `validate` is the only place it is observed. Keeping
 * it an Error lets the readers stay flat.
 */
class CapabilityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CapabilityError';
  }
}

function validate(
  raw: unknown,
  rule: DurationRule,
  path: string,
): CapabilityCheck {
  try {
    if (!isRecord(raw)) {
      throw new CapabilityError(capabilityReason(path, 'an object', raw));
    }
    return {
      ok: true,
      capabilities: {
        transactions: readMode(
          raw.transactions,
          TRANSACTION_MODES,
          `${path}.transactions`,
        ),
        concurrency: readMode(
          raw.concurrency,
          CONCURRENCY_MODES,
          `${path}.concurrency`,
        ),
        snapshotRead: readBoolean(raw.snapshotRead, `${path}.snapshotRead`),
        uniqueConstraint: readBoolean(
          raw.uniqueConstraint,
          `${path}.uniqueConstraint`,
        ),
        maxTransactionDurationMs: readDuration(
          raw.maxTransactionDurationMs,
          rule,
          `${path}.maxTransactionDurationMs`,
        ),
      },
    };
  } catch (error) {
    if (error instanceof CapabilityError) {
      return { ok: false, reason: error.message };
    }
    throw error;
  }
}

function readMode<T extends string>(
  value: unknown,
  allowed: readonly T[],
  path: string,
): T {
  if (!isOneOf(value, allowed)) {
    throw new CapabilityError(
      capabilityReason(path, `one of ${allowed.join(', ')}`, value),
    );
  }
  return value;
}

function readBoolean(value: unknown, path: string): boolean {
  if (typeof value !== 'boolean') {
    throw new CapabilityError(capabilityReason(path, 'a boolean', value));
  }
  return value;
}

/**
 * Zero, negatives and NaN are rejected because they would clamp the effective
 * timeout to a millisecond or make every later comparison false (SPEC 3.4).
 */
function readDuration(
  value: unknown,
  rule: DurationRule,
  path: string,
): number {
  if (value === null && rule.acceptsNull) {
    return Number.POSITIVE_INFINITY;
  }
  if (
    typeof value === 'number' &&
    (value === Number.POSITIVE_INFINITY ||
      (Number.isFinite(value) && value > 0))
  ) {
    return value;
  }
  throw new CapabilityError(capabilityReason(path, rule.expected, value));
}

function isOneOf<T extends string>(
  value: unknown,
  allowed: readonly T[],
): value is T {
  return (
    typeof value === 'string' &&
    allowed.some((candidate) => candidate === value)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function capabilityReason(
  path: string,
  expected: string,
  value: unknown,
): string {
  return `capability "${path}" must be ${expected}, received ${describeValue(value)}`;
}

function describeValue(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return 'array';
  }
  if (typeof value === 'string') {
    return `string ${JSON.stringify(value)}`;
  }
  if (typeof value === 'number') {
    return `number ${value.toString()}`;
  }
  if (typeof value === 'boolean') {
    return `boolean ${value ? 'true' : 'false'}`;
  }
  return typeof value;
}
