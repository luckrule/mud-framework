/**
 * Framework core public surface. The in-memory backend and the fake transport
 * are deliberately not exported here: the framework package must not ship
 * backend or transport implementations (SPEC 4.1, 5.1, INV-016, INV-018);
 * tests import them by path instead.
 */
export type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from './storage/types.ts';
export type {
  Transport,
  TransportCapabilities,
  TransportConnection,
  TransportHandlers,
} from './transport/types.ts';
export type {
  DispatcherOptions,
  Handler,
  RequestContext,
} from './runtime/dispatcher.ts';
export { Dispatcher, FrameworkTimeoutError } from './runtime/dispatcher.ts';
