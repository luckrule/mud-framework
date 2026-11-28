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
   * When set, the connection also fails if it does not open within this
   * budget. Without it the helper still rejects on `error` or `close` before
   * `open`, but never times out.
   */
  readonly timeoutMs?: number;
}

export function openClient(
  url: string,
  options: OpenClientOptions = {},
): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    const timeoutMs = options.timeoutMs;
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
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => {
        fail(new Error(`connection to ${url} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    }
  });
}
