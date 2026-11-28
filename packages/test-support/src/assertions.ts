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
