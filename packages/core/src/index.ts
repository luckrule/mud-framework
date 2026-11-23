/**
 * Framework core public surface. The in-memory backend and the fake transport
 * are deliberately not exported here: the framework package must not ship
 * backend or transport implementations (SPEC 4.1, 5.1, INV-016, INV-018);
 * tests import them by path instead.
 *
 * The connection layer and codec are exported: they are framework behaviour
 * (ordering, error mapping, wire envelope), not a transport or backend
 * implementation (INV-015).
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
  DispatchMeta,
  DispatcherOptions,
  Handler,
  RequestContext,
} from './runtime/dispatcher.ts';
export {
  Dispatcher,
  FrameworkTimeoutError,
  UnknownHandlerError,
} from './runtime/dispatcher.ts';
export type {
  LogFields,
  LogLevel,
  Logger,
  LoggerFactory,
  LoggerOptions,
} from './logging/types.ts';
export {
  createJsonLogger,
  jsonLoggerFactory,
  JsonLogger,
} from './logging/json-logger.ts';
export type { Codec, Envelope, ResponseEnvelope } from './runtime/codec.ts';
export { EnvelopeError, JsonCodec } from './runtime/codec.ts';
export type { ConnectionLayerOptions } from './runtime/connection-layer.ts';
export { ConnectionLayer } from './runtime/connection-layer.ts';
