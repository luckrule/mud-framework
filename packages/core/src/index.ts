/**
 * Framework core public surface. The in-memory backend is deliberately not
 * exported here: the framework package must not ship backend implementations
 * (SPEC 4.1, INV-016); tests import it by path instead.
 */
export type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from './storage/types.ts';
export type {
  DispatcherOptions,
  Handler,
  RequestContext,
} from './runtime/dispatcher.ts';
export { Dispatcher, FrameworkTimeoutError } from './runtime/dispatcher.ts';
