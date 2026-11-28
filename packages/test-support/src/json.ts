import type { ReadableStreamDefaultReader } from 'node:stream/web';

/**
 * `isRecord` is a test-local predicate, not the framework's own guard. A test
 * that checks what the framework puts on the wire must not share the predicate
 * the framework uses to validate its own input, or loosening the guard would
 * weaken the test in the same step.
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
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
