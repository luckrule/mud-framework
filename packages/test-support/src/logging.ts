/**
 * The structural slice of the framework's `Logger` that a test needs here.
 * Keeping it structural avoids importing core, and every core `Logger` is
 * assignable to it.
 */
export interface ThrowingLogger {
  debug(fields: Record<string, unknown>, message: string): never;
  info(fields: Record<string, unknown>, message: string): never;
  warn(fields: Record<string, unknown>, message: string): never;
  error(fields: Record<string, unknown>, message: string): never;
  child(bindings: Record<string, unknown>): ThrowingLogger;
  setLevel(level: string): never;
}

/**
 * A logger whose every method throws, including `child` and `setLevel`.
 * Injecting it at a framework boundary proves a logger fault cannot alter
 * control flow.
 */
export function throwingLogger(): ThrowingLogger {
  return {
    debug: failLogging,
    info: failLogging,
    warn: failLogging,
    error: failLogging,
    child: failLogging,
    setLevel: failLogging,
  };
}

function failLogging(): never {
  throw new Error('logger exploded');
}
