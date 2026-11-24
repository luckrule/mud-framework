import type { Logger } from '../logging/types.ts';

/**
 * Helpers shared by the runtime and connector tests.
 *
 * `isRecord` is a deliberate copy rather than an import from
 * `../internal/guards.ts`: a test that checks what the framework puts on the
 * wire must not share the predicate the framework uses to validate its own
 * input, or loosening the guard would weaken the test in the same step.
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** A wall-clock delay; the timeout tests need real time to pass. */
export const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** Yields one macrotask so callbacks queued behind a timer can run. */
export const tick = (): Promise<void> => sleep(0);

/** Parses an outbound frame or log line, failing loudly on a non-object. */
export function parseFrame(raw: string | Uint8Array): Record<string, unknown> {
  const text = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`frame is not a JSON object: ${text}`);
  }
  return parsed;
}

/** Returns the rejection as an `Error`, and fails the test if it resolves. */
export async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
    throw new Error(`non-Error rejection: ${String(error)}`, { cause: error });
  }
  throw new Error('expected the promise to reject');
}

/**
 * A logger whose every method throws, including `child`. Injecting it at a
 * framework boundary proves a logger fault cannot alter control flow.
 */
export function throwingLogger(): Logger {
  return {
    debug: failLogging,
    info: failLogging,
    warn: failLogging,
    error: failLogging,
    child: failLogging,
  };
}

function failLogging(): never {
  throw new Error('logger exploded');
}
