export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * Log fields are `unknown`, not `any`: a logger never interprets a value, so
 * the caller keeps its own type and `any` would leak unchecked access into
 * every call site (and trip the `no-unsafe-*` rules this repo enables).
 */
export interface LogFields {
  [key: string]: unknown;
}

export interface Logger {
  debug(fields: LogFields, message: string): void;
  info(fields: LogFields, message: string): void;
  warn(fields: LogFields, message: string): void;
  error(fields: LogFields, message: string): void;
  /** A new logger with `bindings` merged over the parent's; the parent is untouched. */
  child(bindings: LogFields): Logger;
  /**
   * Changes the minimum level emitted, including for loggers already derived
   * from this one. Throws `TypeError` for a value outside `LogLevel`.
   */
  setLevel(level: LogLevel): void;
}

export interface LoggerOptions {
  /** Initial minimum level; defaults to `info`. Use `Logger.setLevel` to change it at runtime. */
  level?: LogLevel;
  /** Receives one complete line per event; defaults to stdout. */
  sink?: (line: string) => void;
  /** Fields merged into every event this logger emits. */
  bindings?: LogFields;
}
