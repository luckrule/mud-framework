import type { Logger } from '../logging/types.ts';

export interface SessionRouterOptions {
  readonly logger: Logger;
}

/** Routing ownership and unfinished Session work have independent lifetimes. */
export class SessionRouter {
  private readonly logger: Logger;
  private readonly routes = new Map<string, string>();
  private readonly heldBy = new Map<string, string>();
  private readonly unfinished = new Map<string, Set<Promise<void>>>();

  constructor(options: SessionRouterOptions) {
    this.logger = options.logger;
  }

  /** Binding is applied after success; it cannot serialize the binding handler. */
  bind(
    connectionId: string,
    sessionId: string,
    terminal: Promise<void>,
    logger: Logger = this.logger,
  ): void {
    const previousConnectionId = this.routes.get(sessionId);
    if (previousConnectionId === connectionId) {
      this.track(sessionId, terminal);
      return;
    }
    this.release(connectionId);
    if (previousConnectionId !== undefined) {
      logger.warn(
        { sessionId, previousConnectionId, connectionId },
        'session rebound to a new connection',
      );
      this.heldBy.delete(previousConnectionId);
    }
    this.routes.set(sessionId, connectionId);
    this.heldBy.set(connectionId, sessionId);
    // Include the binding request's reply without replacing any older barrier.
    this.track(sessionId, terminal);
    logger.info({ sessionId, connectionId }, 'session bound');
  }

  sessionOf(connectionId: string): string | undefined {
    return this.heldBy.get(connectionId);
  }

  release(connectionId: string): void {
    const sessionId = this.heldBy.get(connectionId);
    if (sessionId === undefined) {
      return;
    }
    this.heldBy.delete(connectionId);
    if (this.routes.get(sessionId) === connectionId) {
      this.routes.delete(sessionId);
    }
  }

  /**
   * Applies an unbind declared by a successful request. Idempotent: a
   * connection that holds nothing is already unbound, so a repeated unbind is
   * a no-op rather than a failure. The Session's unfinished work is untouched —
   * the terminal barrier does not follow the route (INV-004, SPEC 3.2).
   */
  unbind(connectionId: string, logger: Logger = this.logger): void {
    const sessionId = this.heldBy.get(connectionId);
    if (sessionId === undefined) {
      return;
    }
    this.release(connectionId);
    logger.info({ sessionId, connectionId }, 'session unbound');
  }

  /**
   * Recheck ownership after every wait: a displaced waiter must not execute as
   * its former Session. Only framework completion, including the reply, removes
   * work from the barrier; disconnecting a route cannot make it terminal.
   */
  async run(
    connectionId: string,
    closed: AbortSignal,
    work: (
      sessionId: string | undefined,
      terminal: Promise<void>,
    ) => Promise<void>,
  ): Promise<boolean> {
    while (true) {
      const sessionId = this.sessionOf(connectionId);
      const pending =
        sessionId === undefined ? undefined : this.unfinished.get(sessionId);
      if (pending !== undefined && pending.size > 0) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- ownership must be rechecked after each preceding Session request settles
        const ready = await waitForTerminal(pending, closed);
        // Completion can win the race just before shutdown aborts the waiter.
        if (!ready || closed.aborted) {
          return false;
        }
        continue;
      }
      const terminal = Promise.withResolvers<void>();
      if (sessionId !== undefined) {
        this.track(sessionId, terminal.promise);
      }
      try {
        // oxlint-disable-next-line eslint/no-await-in-loop -- the terminal barrier must include completion of the request and its reply
        await work(sessionId, terminal.promise);
      } finally {
        terminal.resolve();
      }
      return true;
    }
  }

  private track(sessionId: string, terminal: Promise<void>): void {
    let pending = this.unfinished.get(sessionId);
    if (pending === undefined) {
      pending = new Set();
      this.unfinished.set(sessionId, pending);
    }
    if (pending.has(terminal)) {
      return;
    }
    pending.add(terminal);
    const tracked = pending;
    void terminal.then(() => {
      tracked.delete(terminal);
      if (tracked.size === 0 && this.unfinished.get(sessionId) === tracked) {
        this.unfinished.delete(sessionId);
      }
      return undefined;
    });
  }

  clear(): void {
    this.routes.clear();
    this.heldBy.clear();
    this.unfinished.clear();
  }
}

async function waitForTerminal(
  pending: ReadonlySet<Promise<void>>,
  closed: AbortSignal,
): Promise<boolean> {
  if (closed.aborted) {
    return false;
  }
  const closing = Promise.withResolvers<boolean>();
  const onClose = (): void => closing.resolve(false);
  closed.addEventListener('abort', onClose, { once: true });
  try {
    return await Promise.race([
      Promise.all(pending).then(() => true),
      closing.promise,
    ]);
  } finally {
    closed.removeEventListener('abort', onClose);
  }
}
