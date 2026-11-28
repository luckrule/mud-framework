/** Caps one retained line so a child cannot make the tail unbounded. */
const STDERR_LINE_LIMIT = 2000;
/**
 * Bounds how long exit reporting waits for the stderr pipe to drain. Bun
 * reports the exit independently of the pipe, so without this the tail a
 * consumer reads can be empty; a grandchild can keep the pipe open forever, so
 * the wait has to be bounded — the tail is diagnostic, not evidence.
 */
export const STDERR_DRAIN_GRACE_MS = 250;

type ServerStdio =
  | ['ignore', 'inherit', 'inherit']
  | ['ignore', 'inherit', 'pipe'];

interface StderrCapture {
  readonly stdio: ServerStdio;
  /** The live tail, or undefined when capture is off. */
  readonly lines: string[] | undefined;
  readonly limit: number;
}

/**
 * Resolves the capture option to the stdio tuple and the live tail it fills.
 * A bad count throws instead of silently disabling the diagnostic: a caller who
 * asked for a tail must not get a run that looks configured but reports none.
 */
export function stderrCapture(limit: number | undefined): StderrCapture {
  if (limit === undefined) {
    return {
      stdio: ['ignore', 'inherit', 'inherit'],
      lines: undefined,
      limit: 0,
    };
  }
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new TypeError(
      `stderrTailLines must be a positive integer, got ${String(limit)}`,
    );
  }
  return { stdio: ['ignore', 'inherit', 'pipe'], lines: [], limit };
}

/**
 * Keeps the last `limit` lines while mirroring every chunk to the Portal's own
 * stderr, and resolves once the pipe is drained.
 *
 * Mirroring rather than logging keeps the operator's view identical to the
 * default `inherit`: the same fd 2, no structured-log flood from a chatty child,
 * and a custom logger sink is not silently bypassed. The mirror is best-effort;
 * a closed stderr must not break exit observation.
 *
 * The tail array is shared with every `ServerExitInfo` this spawn reports. Bun
 * reports the exit independently of the pipe, so a consumer can observe the
 * exit before the last lines have been read; the returned promise lets
 * `recordExit` wait for them within a bound instead of reporting an empty tail.
 */
export function pumpStderr(
  stream: ReadableStream<Uint8Array>,
  capture: StderrCapture,
): Promise<void> {
  const lines = capture.lines;
  if (lines === undefined) {
    return Promise.resolve();
  }
  const decoder = new TextDecoder();
  let pending = '';
  const trim = (): void => {
    const overflow = lines.length - capture.limit;
    if (overflow > 0) {
      lines.splice(0, overflow);
    }
  };
  const record = (text: string): void => {
    const parts = text.split('\n');
    pending = parts.pop() ?? '';
    for (const part of parts) {
      lines.push(part.slice(0, STDERR_LINE_LIMIT));
    }
    // A child that never writes a newline must not grow the tail either.
    if (pending.length > STDERR_LINE_LIMIT) {
      lines.push(pending.slice(0, STDERR_LINE_LIMIT));
      pending = '';
    }
    trim();
  };
  return (async () => {
    try {
      for await (const chunk of stream) {
        const text = decoder.decode(chunk, { stream: true });
        mirrorToPortalStderr(text);
        record(text);
      }
      mirrorToPortalStderr(decoder.decode());
      if (pending.length > 0) {
        lines.push(pending);
        pending = '';
        trim();
      }
    } catch {
      // A read failure only costs diagnostics; exit observation must not
      // depend on the pipe.
    }
  })();
}

function mirrorToPortalStderr(text: string): void {
  if (text.length === 0) {
    return;
  }
  try {
    process.stderr.write(text);
  } catch {
    // The Portal's stderr is gone; the child's output has nowhere to go.
  }
}
