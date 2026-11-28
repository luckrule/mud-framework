import type { Logger } from '../logging/types.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import { messageOf } from '../shared/guards.ts';
import { checkBackendCapabilities } from '../storage/capabilities.ts';
import { degradations } from '../storage/degradations.ts';
import { unmetRequirements } from '../storage/requirements.ts';
import type { StorageBackend, StorageCapabilities } from '../storage/types.ts';
import { DispatcherExecutor } from './dispatcher-executor.ts';
import { Dispatcher } from './dispatcher.ts';
import type { Extension, ExtensionRegistrar } from './extension.ts';
import type { ServerProcessOptions } from './server-process-options.ts';

/**
 * The process could not reach the readiness barrier: backend creation, the
 * capability self-check, an Extension's unmet requirements or its `register`
 * failed. `ready` was not sent, so Portal must not have started delivering
 * requests.
 */
export class ServerStartupError extends Error {
  readonly code = 'SERVER_STARTUP_FAILED';
  readonly reason: string;

  constructor(reason: string) {
    super(`server process failed to start: ${reason}`);
    this.name = 'ServerStartupError';
    this.reason = reason;
  }
}

/** The running pieces a started Server hands to the process that owns it. */
export interface Runtime {
  readonly dispatcher: Dispatcher;
  readonly executor: RequestExecutor;
  readonly capabilities: StorageCapabilities;
}

/** What a completed startup hands back to the process that owns the lifecycle. */
export interface StartupResult {
  readonly runtime: Runtime;
  /** Extensions that were skipped for unmet capabilities (SPEC 4.3). */
  readonly skippedExtensions: number;
}

export interface StartupDeps {
  readonly options: ServerProcessOptions;
  readonly logger: Logger;
}

/**
 * Brings a Server up to the readiness barrier: backend, capability self-check,
 * degradation warnings, dispatcher and extensions (SPEC 6.2).
 *
 * It deliberately does not send `ready` and does not attach inbound handlers.
 * Both belong to the caller, in one order: install the full handler, set the
 * process active, and only then announce `ready`. A `ready` sent from here would
 * cross the barrier while requests still have nowhere to go, and a Portal that
 * delivers immediately after `ready` (the contract SPEC 6.2 gives it) would lose
 * them.
 *
 * Throws `ServerStartupError` when any step fails, after attempting to close
 * whatever was opened; a close failure is logged as `STORAGE_CLOSE_FAILED` and
 * is not a promise that the backend was released.
 */
export async function startServer(deps: StartupDeps): Promise<StartupResult> {
  const { options, logger } = deps;
  const backend = await createBackend(options, logger);
  let dispatcher: Dispatcher | undefined;
  try {
    // SPEC 6.2 step 1: the backend self-checks its declaration before any
    // handler can see it. A declaration the framework cannot reason about is
    // refused here, not degraded into undefined later (INV-009, INV-010).
    const checked = checkBackendCapabilities(backend.capabilities());
    if (!checked.ok) {
      throw new ServerStartupError(
        `storage capability self-check failed: ${checked.reason}`,
      );
    }
    // SPEC 4.3: a backend weaker than the strongest level is a degradation,
    // reported once per dimension here so it stays at startup volume. The
    // `ready` info line below carries the full declaration; these lines carry
    // what each shortfall costs the business. Both are needed: one for the
    // whole picture, one per consequence.
    for (const degradation of degradations(checked.capabilities)) {
      logger.warn(
        {
          capability: degradation.capability,
          declared: degradation.declared,
          consequence: degradation.consequence,
        },
        'storage capability degraded',
      );
    }
    dispatcher = new Dispatcher({
      backend,
      timeoutMs: options.timeoutMs,
      maxEventsPerRequest: options.maxEventsPerRequest,
      authorize: options.authorize,
      logger,
    });
    const runtime: Runtime = {
      dispatcher,
      executor: new DispatcherExecutor({ dispatcher }),
      capabilities: checked.capabilities,
    };
    // SPEC 6.2 step 2: every Extension is checked against the backend and
    // registered before Portal can deliver anything (INV-010).
    const skippedExtensions = await registerExtensions(
      options.extensions,
      dispatcher,
      checked.capabilities,
      logger,
    );
    if (
      skippedExtensions > 0 &&
      skippedExtensions === options.extensions.length
    ) {
      // Still ready: a Server with no handlers answers every request with
      // UNKNOWN_METHOD, which is diagnosable, whereas refusing to start would
      // take down a process the Portal is waiting on. The line is the signal.
      logger.warn(
        { total: options.extensions.length, skipped: skippedExtensions },
        'no extension registered: every extension was skipped',
      );
    }
    // SPEC 6.2 step 3 is the caller's: the readiness barrier is crossed only
    // once a request handler exists and the process is active, which cannot be
    // known here. The caller logs and sends `ready` together, so this line would
    // otherwise claim readiness before the wire did.
    return { runtime, skippedExtensions };
  } catch (error) {
    const failure =
      error instanceof ServerStartupError
        ? error
        : new ServerStartupError(messageOf(error));
    logger.error(
      { code: failure.code, reason: failure.reason },
      'server process failed to start',
    );
    await closeQuietly(dispatcher, backend, logger);
    throw failure;
  }
}

/**
 * Checks each Extension against the backend declaration and registers the
 * ones it satisfies (SPEC 4.3, INV-010). Returns how many were skipped.
 *
 * A skipped Extension never has its `register` called, so none of its
 * handlers exist; one Extension being skipped does not affect the others. A
 * `required` Extension that fails is different: the process cannot run
 * without it, so startup is refused before `ready` (SPEC 6.2 step 2).
 */
async function registerExtensions(
  extensions: readonly Extension[],
  dispatcher: Dispatcher,
  capabilities: StorageCapabilities,
  logger: Logger,
): Promise<number> {
  let skipped = 0;
  for (const extension of extensions) {
    const unmet = unmetRequirements(capabilities, extension.requires ?? {});
    if (unmet.length === 0) {
      // The Extension never sees the dispatcher, so it cannot forget (or
      // override) the name its handlers are logged under (SPEC 7.1).
      const registrar: ExtensionRegistrar = {
        register: (method, handler, options) => {
          dispatcher.register(method, handler, {
            ...options,
            extension: extension.name,
          });
        },
      };
      // oxlint-disable-next-line eslint/no-await-in-loop -- registration order is the contract: a later Extension may rely on an earlier one's handlers
      await extension.register(registrar);
      logger.info({ extension: extension.name }, 'extension registered');
      continue;
    }
    if (extension.required === true) {
      logger.error(
        { extension: extension.name, unmet },
        'required extension cannot start',
      );
      throw new ServerStartupError(
        `required extension "${extension.name}" cannot start: ${unmet.join('; ')}`,
      );
    }
    skipped += 1;
    logger.warn(
      { extension: extension.name, unmet },
      'extension skipped: storage capabilities unmet',
    );
  }
  return skipped;
}

async function createBackend(
  options: ServerProcessOptions,
  logger: Logger,
): Promise<StorageBackend> {
  try {
    return await options.createBackend();
  } catch (error) {
    const failure = new ServerStartupError(
      `storage backend could not be created: ${messageOf(error)}`,
    );
    logger.error(
      { code: failure.code, reason: failure.reason },
      'server process failed to start',
    );
    throw failure;
  }
}

async function closeQuietly(
  dispatcher: Dispatcher | undefined,
  backend: StorageBackend,
  logger: Logger,
): Promise<void> {
  try {
    if (dispatcher !== undefined) {
      await dispatcher.close();
    } else {
      await backend.close();
    }
  } catch (error) {
    logger.error(
      { code: 'STORAGE_CLOSE_FAILED', reason: messageOf(error) },
      'storage backend could not be closed',
    );
  }
}
