/**
 * Framework core public surface. The in-memory backend and the fake transport
 * are deliberately not exported here: the framework package must not ship
 * backend or transport implementations (SPEC 4.1, 5.1, INV-016, INV-018);
 * tests import them by path instead.
 *
 * The connection layer and codec are exported: they are framework behaviour
 * (ordering, error mapping, wire envelope), not a transport or backend
 * implementation (INV-015).
 *
 * The Portal <-> Server IPC contract is exported for the same reason: it is
 * framework behaviour both processes must agree on (ADR-0002), and `exports`
 * allows no deep paths. That covers the channel implementations too:
 * `spawnBunServer` and `createProcessChannel` are the Portal and Server ends of
 * that link, not a client-side Transport implementation, so exporting them does
 * not make a transport swap change the core (INV-018). Its in-memory channel
 * pair stays private for the same reason as the fake transport: it is a test
 * double, not a framework implementation.
 */
export type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from './storage/types.ts';
export type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from './transport/types.ts';
export type {
  DispatchMeta,
  DispatchResult,
  DispatcherOptions,
  Handler,
  RequestContext,
} from './runtime/dispatcher.ts';
export {
  Dispatcher,
  FrameworkTimeoutError,
  RequestCancelledError,
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
export type {
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from './runtime/executor.ts';
export { LocalExecutor } from './runtime/executor.ts';
export {
  BusinessError,
  FRAMEWORK_CAPABILITY_UNSUPPORTED,
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_REPLAY_EXHAUSTED,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_TIMEOUT,
  INTERNAL_ERROR,
  INVALID_ENVELOPE,
  STORAGE_UNAVAILABLE,
  TRANSPORT_CLOSED,
  TRANSPORT_SEND_FAILED,
  UNKNOWN_METHOD,
  WIRE_CODES,
  describeFailure,
  failureLogLevel,
  isFrameworkCode,
} from './runtime/wire-codes.ts';
export type { ConnectionLayerOptions } from './runtime/connection-layer.ts';
export { ConnectionLayer } from './runtime/connection-layer.ts';
export type { IpcExecutorOptions } from './runtime/ipc-executor.ts';
export { IpcExecutor } from './runtime/ipc-executor.ts';
export type { ServerExitInfo, SpawnedServer } from './ipc/spawn-port.ts';
export type { ServerProcessOptions } from './runtime/server-process.ts';
export {
  runServerProcess,
  ServerChannelClosedError,
  ServerStartupError,
} from './runtime/server-process.ts';
export type { IpcChannel } from './ipc/channel.ts';
export { IpcChannelClosedError } from './ipc/channel.ts';
export type { IpcMessage, IpcParseResult } from './ipc/protocol.ts';
export { parseIpcMessage } from './ipc/protocol.ts';
export { spawnBunServer } from './ipc/bun-spawn.ts';
export { createProcessChannel } from './ipc/bun-child.ts';
