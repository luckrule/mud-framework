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
