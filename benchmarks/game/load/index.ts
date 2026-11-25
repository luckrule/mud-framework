import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { cpus, release, tmpdir } from 'node:os';
import { join } from 'node:path';

import { integerText, messageOf, write, writeError } from '../support.ts';
import type { PortalHandle } from '../support.ts';
import {
  collectResults,
  planGenerators,
  sendGo,
  spawnGenerator,
  startLoadPortal,
  stopGenerators,
  stopPortal,
  waitForReady,
} from './orchestrator.ts';
import type { GeneratorHandle } from './orchestrator.ts';
import {
  aggregateResults,
  buildSummary,
  printAggregate,
  printPerGenerator,
  printResources,
} from './report.ts';
import { ResourceSampler } from './resources.ts';
import { effectiveThinkMs, mixText, readMainConfig } from './support.ts';
import type { MainConfig } from './support.ts';

/**
 * Player-level load entry point: the coordinator spawns a real Portal + Server
 * and one or more `client.ts` generator processes. Each generator owns its
 * clients' WebSocket connections and its own event loop, so the client noise
 * that the single-process microbenchmarks pay is spread the way it would be
 * across players' machines. It is still one host: both the generators and the
 * framework share the CPU and the wall clock, so this measures a same-host
 * approximation, not a machine-independent capacity.
 *
 * This file only wires the phases together and prints the run header/summary;
 * process and generator lifecycle live in `orchestrator.ts`, aggregation and
 * formatting in `report.ts`, sampling in `resources.ts`.
 */

function environmentLine(): string {
  const cpu = cpus()[0]?.model ?? 'unknown CPU';
  return `Bun ${process.versions.bun ?? 'unknown'}  ${process.platform} ${process.arch}  kernel ${release()}  ${cpu}  cores ${cpus().length}`;
}

interface Directories {
  readonly scratchDir: string;
  readonly dataDir: string | undefined;
  readonly resultsDir: string;
  readonly userDataDir: boolean;
}

/**
 * Default data lives under the system temp directory, which is tmpfs on most
 * Linux hosts; a point of this benchmark is to rerun with LOAD_DATA_DIR on the
 * target filesystem to price fsync. A user-provided data directory is never
 * removed; the scratch directory holding generator results is.
 */
function prepareDirectories(config: MainConfig): Directories {
  if (config.shared.externalUrl !== undefined) {
    // The external Portal owns its own data directory; only generator results
    // need a scratch location.
    const scratchDir = mkdtempSync(join(tmpdir(), 'mud-load-'));
    const resultsDir = join(scratchDir, 'results');
    mkdirSync(resultsDir);
    return { scratchDir, dataDir: undefined, resultsDir, userDataDir: false };
  }
  if (config.dataDirectory !== undefined) {
    mkdirSync(config.dataDirectory, { recursive: true });
    const scratchDir = mkdtempSync(join(tmpdir(), 'mud-load-results-'));
    return {
      scratchDir,
      dataDir: config.dataDirectory,
      resultsDir: scratchDir,
      userDataDir: true,
    };
  }
  const scratchDir = mkdtempSync(join(tmpdir(), 'mud-load-'));
  const dataDir = join(scratchDir, 'data');
  const resultsDir = join(scratchDir, 'results');
  mkdirSync(dataDir);
  mkdirSync(resultsDir);
  return { scratchDir, dataDir, resultsDir, userDataDir: false };
}

function cleanupDirectories(dirs: Directories, keep: boolean): void {
  if (dirs.userDataDir && dirs.dataDir !== undefined) {
    write(
      `cleanup: kept data dir ${dirs.dataDir} (LOAD_DATA_DIR is user-owned)`,
    );
  }
  if (keep) {
    write(`cleanup: kept ${dirs.scratchDir} (LOAD_KEEP=1)`);
    return;
  }
  try {
    rmSync(dirs.scratchDir, { recursive: true, force: true });
    write(`cleanup: removed ${dirs.scratchDir}`);
  } catch (error) {
    writeError(
      `load benchmark cleanup failed: could not remove ${dirs.scratchDir}: ${messageOf(error)}`,
    );
    process.exitCode = 1;
  }
}

async function main(): Promise<void> {
  const config = readMainConfig();
  const thinkMs = effectiveThinkMs(config.shared);

  write('mud-framework player-level load benchmark');
  write(environmentLine());
  write(
    `config: clients=${integerText(config.clients)} generators=${config.generators} room-size=${config.shared.roomSize} mode=${config.shared.mode} duration=${config.shared.durationSec}s warmup=${config.shared.warmupSec}s think=${thinkMs}ms mix=${mixText(config.shared.mix)} request-timeout=${config.shared.requestTimeoutMs}ms`,
  );

  let portal: PortalHandle | undefined;
  let dirs: Directories | undefined;
  let sampler: ResourceSampler | undefined;
  const generators: GeneratorHandle[] = [];
  let summary: string | undefined;
  try {
    dirs = prepareDirectories(config);
    write(
      `data dir: ${dirs.dataDir ?? 'not used (external Portal)'}${dirs.userDataDir ? ' (LOAD_DATA_DIR; never removed)' : ''}`,
    );
    write(`result dir: ${dirs.resultsDir}`);

    let url: string;
    if (config.shared.externalUrl !== undefined) {
      url = config.shared.externalUrl;
      write(
        `portal: reusing ${url}; spawn, Server pid sampling and cleanup are skipped`,
      );
    } else {
      if (dirs.dataDir === undefined) {
        throw new Error('internal: spawn path has no data directory');
      }
      portal = await startLoadPortal(dirs.dataDir, config.shared);
      url = portal.url;
      write(`portal: spawned pid ${portal.pid}, listening on ${url}`);
    }

    sampler = new ResourceSampler(portal);
    sampler.start();

    const plans = planGenerators(config, dirs.resultsDir);
    write(
      `generators: ${plans.map((plan) => `${plan.index}:${plan.clients}`).join(' ')} (clients per process)`,
    );
    const setupStartedMs = Date.now();
    for (const plan of plans) {
      generators.push(spawnGenerator(plan, url, config.shared));
    }
    await waitForReady(generators);
    write(
      `setup: all generators ready after ${((Date.now() - setupStartedMs) / 1000).toFixed(2)} s; releasing measurement barrier`,
    );
    await sendGo(generators);

    const results = await collectResults(generators, config.shared);
    printPerGenerator(results);
    const aggregate = aggregateResults(results);
    printAggregate(aggregate, config);
    summary = buildSummary(aggregate, config);
  } finally {
    // Stop sampling before teardown: the report is about the measured run, not
    // the exit path.
    const resources = sampler?.stop();
    await stopGenerators(generators);
    await stopPortal(portal, config.shared.externalUrl !== undefined);
    if (dirs !== undefined) {
      cleanupDirectories(dirs, config.keep);
    }
    if (resources !== undefined) {
      printResources(resources);
    }
  }

  if (summary !== undefined) {
    write(summary);
  }
}

await main().catch((error: unknown) => {
  writeError(`load benchmark failed: ${messageOf(error)}`);
  process.exitCode = 1;
});

if (process.exitCode !== undefined && process.exitCode !== 0) {
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed run must not hang on handles it cannot close
  process.exit(1);
}
