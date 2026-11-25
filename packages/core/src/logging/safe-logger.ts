import type { Logger } from './types.ts';

const SAFE_LOGGERS = new WeakSet<Logger>();

/**
 * Wraps a logger injected by the caller so a throwing implementation cannot
 * change framework control flow. Every call is best effort; a failed log is
 * swallowed rather than written to stderr, which would pollute the caller's
 * output.
 *
 * Idempotent: a logger already wrapped here is returned as is, so handing one
 * from a framework entry point to another does not stack wrappers.
 *
 * `setLevel` is the one call that is not swallowed: it is configuration, not a
 * log write, so an invalid level must reach the caller instead of leaving the
 * logger silently at its previous level.
 */
export function safeLogger(logger: Logger): Logger {
  if (SAFE_LOGGERS.has(logger)) {
    return logger;
  }
  const safe: Logger = {
    debug(fields, message) {
      callQuietly(() => logger.debug(fields, message));
    },
    info(fields, message) {
      callQuietly(() => logger.info(fields, message));
    },
    warn(fields, message) {
      callQuietly(() => logger.warn(fields, message));
    },
    error(fields, message) {
      callQuietly(() => logger.error(fields, message));
    },
    setLevel(level) {
      // Deliberately not callQuietly: configuration errors must not be silent.
      logger.setLevel(level);
    },
    child(bindings) {
      try {
        return safeLogger(logger.child(bindings));
      } catch {
        // A throwing child() cannot carry the bindings; the parent wrapper is
        // still safe to log through.
        return safe;
      }
    },
  };
  SAFE_LOGGERS.add(safe);
  return safe;
}

function callQuietly(call: () => void): void {
  try {
    call();
  } catch {
    // Logging is best effort: a failed log must not affect control flow.
  }
}
