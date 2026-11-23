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
}

export interface LoggerOptions {
  /** Minimum level emitted; defaults to `info`. */
  level?: LogLevel;
  /** Receives one complete line per event; defaults to stdout. */
  sink?: (line: string) => void;
  /** Fields merged into every event this logger emits. */
  bindings?: LogFields;
}

export interface LoggerFactory {
  create(options?: LoggerOptions): Logger;
}
