import { cpus, release } from 'node:os';

export interface EnvironmentLineOptions {
  /** Appends the logical core count; the load benchmark sizes generators by it. */
  readonly cores?: boolean;
}

/** The run header every benchmark prints: runtime, platform and CPU. */
export function environmentLine(options: EnvironmentLineOptions = {}): string {
  const cpu = cpus()[0]?.model ?? 'unknown CPU';
  const cores = options.cores === true ? `  cores ${cpus().length}` : '';
  return `Bun ${process.versions.bun ?? 'unknown'}  ${process.platform} ${process.arch}  kernel ${release()}  ${cpu}${cores}`;
}
