import { messageOf } from '../shared/validation.ts';
import type { LogFields, LogLevel, Logger, LoggerOptions } from './types.ts';

const LEVEL_ORDER: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

interface LevelState {
  threshold: number;
}

function createLevelState(level: LogLevel = 'info'): LevelState {
  return { threshold: LEVEL_ORDER[level] };
}

/**
 * One JSON object per line on stdout. Hand-written on purpose: a structured
 * logger is a dozen lines of `JSON.stringify`, and ADR-0001 keeps the runtime
 * dependency-free.
 *
 * Levels are filtered before formatting, so a filtered event costs one integer
 * comparison and nothing else. A logger tree shares one mutable level: setting
 * it on any node changes filtering for the root and every child at once, which
 * is what makes a runtime level change possible without rebuilding the tree.
 */
export class JsonLogger implements Logger {
  private readonly state: LevelState;
  private readonly sink: (line: string) => void;
  private readonly bindings: LogFields;

  /**
   * @param sharedState - Set only by `child()`: every node in a tree must
   * filter through the same mutable level.
   */
  constructor(options: LoggerOptions = {}, sharedState?: LevelState) {
    this.state = sharedState ?? createLevelState(options.level);
    this.sink = options.sink ?? writeToStdout;
    this.bindings = options.bindings ?? {};
  }

  debug(fields: LogFields, message: string): void {
    this.emit('debug', fields, message);
  }

  info(fields: LogFields, message: string): void {
    this.emit('info', fields, message);
  }

  warn(fields: LogFields, message: string): void {
    this.emit('warn', fields, message);
  }

  error(fields: LogFields, message: string): void {
    this.emit('error', fields, message);
  }

  child(bindings: LogFields): Logger {
    return new JsonLogger(
      {
        sink: this.sink,
        // Child bindings win over the parent's, so a narrower scope can pin a
        // field (a request id over a connection id) without a second argument.
        bindings: { ...this.bindings, ...bindings },
      },
      this.state,
    );
  }

  setLevel(level: LogLevel): void {
    if (!Object.hasOwn(LEVEL_ORDER, level)) {
      throw new TypeError(`unknown log level: ${level}`);
    }
    this.state.threshold = LEVEL_ORDER[level];
  }

  private emit(level: LogLevel, fields: LogFields, message: string): void {
    if (LEVEL_ORDER[level] < this.state.threshold) {
      return;
    }

    const time = new Date().toISOString();
    let line: string;
    try {
      line = JSON.stringify({
        level,
        time,
        msg: message,
        ...this.bindings,
        ...fields,
      });
    } catch (error) {
      // A cycle or a BigInt must not lose the event or throw into the caller:
      // fall back to the fields that are always serializable.
      line = fallbackLine(level, time, message, error);
    }
    this.sink(line);
  }
}

export function createJsonLogger(options?: LoggerOptions): Logger {
  return new JsonLogger(options);
}

function writeToStdout(line: string): void {
  // `Bun.stdout.write` returns a promise; a closed pipe must not surface as an
  // unhandled rejection.
  void Bun.stdout.write(`${line}\n`).catch(() => undefined);
}

function fallbackLine(
  level: LogLevel,
  time: string,
  message: string,
  error: unknown,
): string {
  // Primitives only, so this cannot throw a second time.
  return JSON.stringify({
    level,
    time,
    msg: message,
    logError: `log fields could not be serialized: ${messageOf(error)}`,
  });
}
