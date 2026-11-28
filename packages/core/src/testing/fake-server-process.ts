import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type { IpcMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { preparedMessage, readyMessage } from './ipc-messages.ts';

export interface FakeServerOptions {
  /** Ack `shutdown` with `closed`, like a cooperative Server. */
  autoCloseOnShutdown: boolean;
  /** Record the exit right after `closed`. False models a process that lingers. */
  exitOnShutdown?: boolean;
  /**
   * Answer `hello` with `prepared` (ADR-0007). On by default: the two-phase
   * handshake is the only startup path, so a fake that stayed silent on `hello`
   * would model a Server that can never start.
   */
  autoPrepare?: boolean;
  /**
   * Answer `prepared` to `hello`, `ready` to `activate`, and exit at once: the
   * "ready, then die on the first request" loop.
   */
  crashOnActivate?: boolean;
  /** Default true. False models a process that survives SIGKILL. */
  killExits?: boolean;
  /**
   * Model the real spawn's ordering: `recordExit` closes the channel (which
   * fires the Portal's `onClose` synchronously) before the `exited` promise
   * settles. Without it a test cannot exercise the crash path where the
   * channel close wins the race against the promise continuation.
   */
  closeChannelOnExit?: boolean;
  /** Ignore `hello`: models a spawn that never reaches `prepared`. */
  silentOnHello?: boolean;
  /** Answer `activate` with `ready` (after `prepared`). Default false. */
  readyOnActivate?: boolean;
  /** Reported in `prepared`/`ready`; defaults to 4242. */
  pid?: number;
  /**
   * stderr tail this process's exit reports, modelling a spawn that captured
   * its child's stderr. Absent by default, like the real `inherit` mode.
   */
  stderrTail?: readonly string[];
  /** Shared event log for cross-process ordering assertions. */
  timeline?: string[];
  /** Prefix for this process's timeline entries. */
  label?: string;
  /**
   * Hold delivered requests and answer them with a success only when `shutdown`
   * arrives, before `closed`. Models a Server draining its in-flight work during
   * a switch, so the test can assert where a request settled.
   */
  holdRequests?: boolean;
}

/**
 * A stand-in Server process: the far end of an in-memory channel pair, with a
 * hand-resolvable `exited` promise so a crash is deterministic.
 */
export class FakeServerProcess {
  readonly received: unknown[] = [];
  readonly handle: SpawnedServer;
  private readonly channel: IpcChannel;
  private readonly exit = Promise.withResolvers<ServerExitInfo>();
  private readonly autoCloseOnShutdown: boolean;
  private readonly exitOnShutdown: boolean;
  private readonly autoPrepare: boolean;
  private readonly crashOnActivate: boolean;
  private readonly killExits: boolean;
  private readonly closeChannelOnExit: boolean;
  private readonly silentOnHello: boolean;
  private readonly readyOnActivate: boolean;
  private readonly pid: number;
  private readonly stderrTail: readonly string[] | undefined;
  private readonly timeline: string[] | undefined;
  private readonly label: string;
  private readonly holdRequests: boolean;
  /** Request ids delivered while `holdRequests` is on, in arrival order. */
  private readonly held: string[] = [];
  private exitRecorded = false;
  private exitInfo: ServerExitInfo | undefined;
  private requestCount = 0;
  private closeOnRequestAt: number | undefined;
  private kills = 0;

  constructor(options: FakeServerOptions) {
    const [portal, server] = createInMemoryChannelPair();
    this.autoCloseOnShutdown = options.autoCloseOnShutdown;
    this.exitOnShutdown = options.exitOnShutdown ?? true;
    this.autoPrepare = options.autoPrepare ?? true;
    this.crashOnActivate = options.crashOnActivate ?? false;
    this.killExits = options.killExits ?? true;
    this.closeChannelOnExit = options.closeChannelOnExit ?? false;
    this.silentOnHello = options.silentOnHello ?? false;
    this.readyOnActivate = options.readyOnActivate ?? false;
    this.pid = options.pid ?? 4242;
    this.stderrTail = options.stderrTail;
    this.timeline = options.timeline;
    this.label = options.label ?? `server-${String(this.pid)}`;
    this.holdRequests = options.holdRequests ?? false;
    this.channel = server;
    this.handle = {
      channel: portal,
      exited: this.exit.promise,
      // The real spawn records the exit before it notifies the channel close, so
      // this is populated by the time `onClose` runs.
      lastExit: () => this.exitInfo,
      kill: () => {
        this.kills += 1;
        // A real SIGKILL ends the process. Without this the executor would pay
        // its exit-wait timeout on every kill.
        if (this.killExits) {
          this.recordExit({ code: null, signal: 'SIGKILL', observed: true });
        }
      },
    };
    server.onMessage((raw) => {
      this.received.push(raw);
      const parsed = parseIpcMessage(raw);
      if (!parsed.ok) {
        return;
      }
      this.mark(`recv:${parsed.message.type}`);
      if (parsed.message.type === 'request') {
        this.requestCount += 1;
        if (this.holdRequests) {
          this.held.push(parsed.message.requestId);
        }
        if (this.requestCount === this.closeOnRequestAt) {
          // A crash while handling a request: the link dies mid-flush.
          this.channel.close('test crash on request');
        }
      }
      if (this.autoCloseOnShutdown && parsed.message.type === 'shutdown') {
        if (this.holdRequests) {
          // Drain first: every held request settles before `closed`, like a
          // Server that finishes in-flight work during its graceful close.
          for (const requestId of this.held.splice(0)) {
            this.send({
              type: 'result',
              requestId,
              ok: true,
              result: `drained-${requestId}`,
            });
          }
        }
        this.send({ type: 'closed', reason: 'test shutdown' });
        this.mark('closed');
        // The real Server exits after `closed`; without this the executor would
        // wait out its exit timeout on every close().
        if (this.exitOnShutdown) {
          this.recordExit({ code: 0, signal: null, observed: true });
        }
      }
      if (this.autoPrepare && parsed.message.type === 'hello') {
        if (!this.silentOnHello) {
          this.send(preparedMessage(this.pid));
        }
      }
      if (parsed.message.type === 'activate') {
        this.mark('activate');
        if (this.crashOnActivate) {
          this.send(readyMessage(5000, this.pid));
          this.recordExit({ code: 1, signal: null, observed: true });
        } else if (this.readyOnActivate) {
          this.send(readyMessage(5000, this.pid));
        }
      }
    });
  }

  /** Appends one entry to the shared timeline, when a test supplied one. */
  private mark(event: string): void {
    this.timeline?.push(`${this.label}:${event}`);
  }

  send(message: IpcMessage): void {
    this.channel.send(message);
  }

  /** Bypasses the message type: the real peer can put anything on the wire. */
  sendRaw(raw: unknown): void {
    // Method parameters are bivariant, so this widens the send signature
    // without an assertion: the channel treats the value as opaque.
    const sender: { send(message: unknown): void } = this.channel;
    sender.send(raw);
  }

  crash(code: number | null = 1): void {
    this.recordExit({ code, signal: null, observed: true });
  }

  /**
   * An exit that could not be observed: the process may or may not be gone.
   * Used to check the executor keeps its defensive kill rather than assuming
   * the process exited.
   */
  unobservableExit(): void {
    this.recordExit({ code: null, signal: null, observed: false });
  }

  /** A dead link without an exit event: the other half of the crash race. */
  closeChannel(): void {
    this.channel.close('test channel closed');
  }

  /** Kills the link once this many requests have been received. */
  closeOnRequest(count: number): void {
    this.closeOnRequestAt = count;
  }

  /** Records a real exit after a delay, modelling a slow SIGKILL. */
  exitAfter(
    delayMs: number,
    info: ServerExitInfo = {
      code: null,
      signal: 'SIGKILL',
      observed: true,
    },
  ): void {
    setTimeout(() => {
      this.recordExit(info);
    }, delayMs);
  }

  get killCount(): number {
    return this.kills;
  }

  private recordExit(info: ServerExitInfo): void {
    if (this.exitRecorded) {
      return;
    }
    this.exitRecorded = true;
    this.exitInfo =
      this.stderrTail === undefined
        ? info
        : { ...info, stderrTail: this.stderrTail };
    this.mark('exit');
    if (this.closeChannelOnExit) {
      // Mirrors `spawnBunServer`: `markClosed` runs before `exit.resolve`, so
      // the Portal's `onClose` sees the exit synchronously while the promise's
      // continuation has not run yet.
      this.channel.close('server process exited');
    }
    this.exit.resolve(this.exitInfo);
  }
}
