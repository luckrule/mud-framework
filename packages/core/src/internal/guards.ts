/**
 * Internal helpers shared across `ipc/`, `logging/`, `runtime/` and `storage/`.
 *
 * Deliberately not re-exported from `index.ts`: these are implementation
 * details of the core, and widening the public surface to include them would
 * turn them into contract. `internal/` is the one place no other directory has
 * to depend on a sibling directory to reach.
 */

/**
 * Every validator here receives `unknown`, and almost all of them need to read
 * properties only when the value is a plain object; `typeof` alone accepts
 * arrays and `null`, which would then be probed for fields.
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Rejection reasons must say what was received, but the value may hold client
 * data, so only its type and scalar text are reported, never a serialized copy.
 */
export function describeValue(value: unknown): string {
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

/**
 * Catch blocks bind `unknown`, yet every error path here needs a string for a
 * log line or an error envelope; a non-`Error` throw still has to produce one.
 */
export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
