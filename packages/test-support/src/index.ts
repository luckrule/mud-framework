import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ReadableStreamDefaultReader } from 'node:stream/web';

/**
 * Helpers shared by tests in every workspace package.
 *
 * This package deliberately has no dependency on `mud-framework-core`: the
 * helpers below are structural, so importing them cannot create a workspace
 * dependency cycle.
 *
 * `isRecord` is a test-local predicate, not the framework's own guard. A test
 * that checks what the framework puts on the wire must not share the predicate
 * the framework uses to validate its own input, or loosening the guard would
 * weaken the test in the same step.
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** A wall-clock delay; timeout tests need real time to pass. */
export const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** Yields one macrotask so callbacks queued behind a timer can run. */
export const tick = (): Promise<void> => sleep(0);

export interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
}

export function deferred<T>(): Deferred<T> {
  return Promise.withResolvers<T>();
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

/**
 * Parses one JSON line, failing loudly on invalid JSON or a non-object.
 * `description` is the caller's name for the line, used in the error message.
 */
export function parseJsonLine(
  line: string,
  description = 'line',
): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch (error) {
    throw new Error(`${description} is not JSON: ${line}`, { cause: error });
  }
  if (!isRecord(parsed)) {
    throw new Error(`${description} is not a JSON object: ${line}`);
  }
  return parsed;
}

/** Parses an outbound frame or log line, failing loudly on a non-object. */
export function parseFrame(raw: string | Uint8Array): Record<string, unknown> {
  const text = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`frame is not a JSON object: ${text}`);
  }
  return parsed;
}

const TIMED_OUT = Symbol('timed out');

/** Rejects with `${description} within ${timeoutMs}ms` if `promise` is slower. */
export function withDeadline<T>(
  promise: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<typeof TIMED_OUT>((resolve) => {
    timer = setTimeout(() => {
      resolve(TIMED_OUT);
    }, timeoutMs);
  });
  return Promise.race([promise, timeout])
    .then((result) => {
      if (result === TIMED_OUT) {
        throw new Error(`${description} within ${timeoutMs}ms`);
      }
      return result;
    })
    .finally(() => {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    });
}

export interface TempDatabase {
  readonly directory: string;
  readonly path: string;
  remove(): void;
}

/**
 * A fresh SQLite path in its own temp directory. `remove` deletes the whole
 * directory, so a test must not share a directory between two databases.
 */
export function tempDatabase(
  options: {
    readonly prefix?: string;
    readonly filename?: string;
  } = {},
): TempDatabase {
  const directory = mkdtempSync(join(tmpdir(), options.prefix ?? 'mud-test-'));
  return {
    directory,
    path: join(directory, options.filename ?? 'server.sqlite'),
    remove(): void {
      rmSync(directory, { recursive: true, force: true });
    },
  };
}

/**
 * `process.kill(pid, 0)` is the only portable liveness probe: it throws ESRCH
 * for a reaped process. Anything else (EPERM included) means it still exists.
 */
export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = isRecord(error) ? error.code : undefined;
    return code !== 'ESRCH';
  }
}

/**
 * Resolves with the next text or binary message, rejecting on a socket error.
 *
 * It listens for `message` and `error` only: a `close` without either leaves it
 * pending forever, and it has no timeout. Pair it with `withDeadline` whenever a
 * missing reply must fail the test.
 */
export function nextMessage(socket: WebSocket): Promise<string | Uint8Array> {
  return new Promise((resolve, reject) => {
    socket.addEventListener(
      'message',
      (event: MessageEvent) => {
        const data: unknown = event.data;
        if (typeof data === 'string' || data instanceof Uint8Array) {
          resolve(data);
          return;
        }
        reject(new Error(`unexpected message type: ${typeof data}`));
      },
      { once: true },
    );
    socket.addEventListener(
      'error',
      () => {
        reject(new Error('socket errored before a reply arrived'));
      },
      { once: true },
    );
  });
}

export interface OpenClientOptions {
  /**
   * When set, the connection also fails if it closes before opening or does
   * not open within this budget. Without it the helper only listens for
   * `open` and `error`, like the original per-test copies.
   */
  readonly timeoutMs?: number;
}

export function openClient(
  url: string,
  options: OpenClientOptions = {},
): Promise<WebSocket> {
  if (options.timeoutMs === undefined) {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url);
      socket.addEventListener(
        'open',
        () => {
          resolve(socket);
        },
        { once: true },
      );
      socket.addEventListener(
        'error',
        () => {
          reject(new Error(`failed to connect to ${url}`));
        },
        { once: true },
      );
    });
  }

  const timeoutMs = options.timeoutMs;
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    let timer: ReturnType<typeof setTimeout> | undefined;

    function cleanup(): void {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      socket.removeEventListener('open', onOpen);
      socket.removeEventListener('error', onError);
      socket.removeEventListener('close', onClose);
    }

    function fail(error: Error): void {
      cleanup();
      socket.close();
      reject(error);
    }

    function onOpen(): void {
      cleanup();
      resolve(socket);
    }

    function onError(): void {
      fail(new Error(`failed to connect to ${url}`));
    }

    function onClose(): void {
      fail(new Error(`connection to ${url} closed before open`));
    }

    socket.addEventListener('open', onOpen);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
    timer = setTimeout(() => {
      fail(new Error(`connection to ${url} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
}

/**
 * Splits a byte stream into lines, keeping a trailing line without a newline.
 * The Portal and Server share stdout, so callers keep reading until the marker
 * they need rather than assuming the first line is theirs.
 */
export class LineReader {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>;
  private readonly decoder = new TextDecoder();
  private buffer = '';

  constructor(stream: ReadableStream<Uint8Array<ArrayBuffer>>) {
    this.reader = stream.getReader();
  }

  async nextLine(): Promise<string | undefined> {
    let index = this.buffer.indexOf('\n');
    while (index < 0) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- a stream yields chunks over time; reading the next one is the operation itself
      const chunk = await this.reader.read();
      if (chunk.done) {
        const rest = this.buffer;
        this.buffer = '';
        return rest.length > 0 ? rest : undefined;
      }
      this.buffer += this.decoder.decode(chunk.value, { stream: true });
      index = this.buffer.indexOf('\n');
    }
    const line = this.buffer.slice(0, index);
    this.buffer = this.buffer.slice(index + 1);
    return line;
  }

  cancel(): Promise<void> {
    return this.reader.cancel();
  }
}
