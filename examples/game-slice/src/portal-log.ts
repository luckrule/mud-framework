/**
 * Parsing for the Portal's own stdout lines. Shared by the Portal (which taps
 * its executor log to learn the Server pid) and the hot-switch drill (which
 * reads the addresses and pids), so the two cannot drift on the message names.
 */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export type PortalLine =
  | { readonly kind: 'listening'; readonly url: string }
  | { readonly kind: 'control'; readonly url: string }
  | { readonly kind: 'server-pid'; readonly pid: number };

/**
 * The pid of the Server a `server ready` / `replacement server ready` line
 * names. Those executor lines are the only place a process id appears.
 */
export function serverPidOf(line: string): number | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed)) {
    return undefined;
  }
  if (
    parsed.msg !== 'server ready' &&
    parsed.msg !== 'replacement server ready'
  ) {
    return undefined;
  }
  const pid = parsed.pid;
  return typeof pid === 'number' && Number.isInteger(pid) && pid > 0
    ? pid
    : undefined;
}

export function parsePortalLine(line: string): PortalLine | undefined {
  const listening = /^portal: listening on (ws:\/\/\S+)$/.exec(line);
  if (listening !== null) {
    return { kind: 'listening', url: listening[1] };
  }
  const control = /^portal: control on (http:\/\/\S+)$/.exec(line);
  if (control !== null) {
    return { kind: 'control', url: control[1] };
  }
  const pid = serverPidOf(line);
  return pid === undefined ? undefined : { kind: 'server-pid', pid };
}
