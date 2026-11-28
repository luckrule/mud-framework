import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { messageOf } from '../../../packages/core/src/shared/validation.ts';
import { environmentLine } from '../../environment.ts';
import { shouldRemoveDataDir } from '../cleanup.ts';
import { integerText, write, writeError } from '../support.ts';
import type { PortalHandle } from '../support.ts';
import {
  collectResults,
  planGenerators,
  sendGo,
  senderSessionIds,
  spawnGenerator,
  startLoadPortal,
  stopGenerators,
  stopPortal,
  waitForReady,
} from './orchestrator.ts';
import type { GeneratorHandle } from './orchestrator.ts';
import type { PortalStopSummary } from './orchestrator.ts';
import {
  aggregateResults,
  buildSummary,
  printAggregate,
  printPerGenerator,
  printResources,
  printServerStats,
} from './report.ts';
import { ResourceSampler } from './resources.ts';
import { readServerStatsFile } from './server-stats.ts';
import type { ServerStatsRead } from './server-stats.ts';
import {
  effectiveThinkMs,
  mixText,
  readMainConfig,
  serverStatsLabel,
} from './support.ts';
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

function cleanupDirectories(
  dirs: Directories,
  stop: PortalStopSummary,
  keep: boolean,
): void {
  if (dirs.userDataDir && dirs.dataDir !== undefined) {
    write(
      `cleanup: kept data dir ${dirs.dataDir} (LOAD_DATA_DIR is user-owned)`,
    );
  }
  // Results and the scratch data directory stay together: removing them while
  // a Portal or Server may still be alive hides the failure scene and can
  // unlink a SQLite file a live writer still has open.
  if (
    !shouldRemoveDataDir({
      stopFailed: stop.stopFailed,
      survivors: stop.survivors,
      keep,
    })
  ) {
    const why = keep
      ? 'LOAD_KEEP=1'
      : stop.stopFailed
        ? 'Portal stop was not graceful or not confirmed'
        : `Server pid(s) still alive: ${stop.survivors.join(', ')}`;
    write(`cleanup: kept ${dirs.scratchDir} (${why})`);
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
  const resourceJsonlRaw = process.env.LOAD_RESOURCE_JSONL?.trim();
  const resourceJsonl =
    resourceJsonlRaw !== undefined && resourceJsonlRaw.length > 0
      ? resolve(resourceJsonlRaw)
      : undefined;

  write('mud-framework player-level load benchmark');
  write(environmentLine({ cores: true }));
  write(
    `config: clients=${integerText(config.clients)} senders=${integerText(config.senders)} listeners=${integerText(config.clients - config.senders)} generators=${config.generators} room-size=${config.shared.roomSize} mode=${config.shared.mode} duration=${config.shared.durationSec}s warmup=${config.shared.warmupSec}s think=${thinkMs}ms mix=${mixText(config.shared.mix)} request-timeout=${config.shared.requestTimeoutMs}ms server-stats=${serverStatsLabel(config.shared.externalUrl, config.serverStats)}`,
  );

  let portal: PortalHandle | undefined;
  let dirs: Directories | undefined;
  let sampler: ResourceSampler | undefined;
  let serverStatsFile: string | undefined;
  let serverStats: ServerStatsRead | undefined;
  const generators: GeneratorHandle[] = [];
  let summary: string | undefined;
  try {
    dirs = prepareDirectories(config);
    const plans = planGenerators(config, dirs.resultsDir);
    const senderSessions = senderSessionIds(plans);
    write(
      `data dir: ${dirs.dataDir ?? 'not used (external Portal)'}${dirs.userDataDir ? ' (LOAD_DATA_DIR; never removed)' : ''}`,
    );
    write(`result dir: ${dirs.resultsDir}`);

    let url: string;
    if (config.shared.externalUrl !== undefined) {
      url = config.shared.externalUrl;
      write(
        `portal: reusing ${url}; spawn, Server pid sampling, Server stats and cleanup are skipped`,
      );
    } else {
      if (dirs.dataDir === undefined) {
        throw new Error('internal: spawn path has no data directory');
      }
      if (config.serverStats) {
        serverStatsFile = join(dirs.resultsDir, 'server-stats.json');
        portal = await startLoadPortal(dirs.dataDir, config.shared, {
          serverStats: true,
          serverStatsFile,
          senderSessions,
        });
      } else {
        portal = await startLoadPortal(dirs.dataDir, config.shared, {
          serverStats: false,
        });
      }
      url = portal.url;
      write(`portal: spawned pid ${portal.pid}, listening on ${url}`);
    }

    sampler = new ResourceSampler(portal, resourceJsonl);
    sampler.start();
    if (resourceJsonl !== undefined) {
      write(`resources jsonl: ${resourceJsonl}`);
    }

    write(
      `generators: ${plans.map((plan) => `${plan.index}:${plan.clients}(${plan.senders} senders)`).join(' ')}`,
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
    const portalStop = await stopPortal(
      portal,
      config.shared.externalUrl !== undefined,
    );
    // The Server writes its counters as it shuts down, so read them after the
    // Portal stop and before cleanup removes the result directory.
    if (serverStatsFile !== undefined) {
      serverStats = readServerStatsFile(serverStatsFile);
    }
    if (dirs !== undefined) {
      cleanupDirectories(dirs, portalStop, config.keep);
    }
    if (resources !== undefined) {
      printResources(resources);
    }
    if (config.shared.externalUrl !== undefined) {
      write(
        'server ipc/handler stats: skipped (LOAD_URL reuses an external Portal that does not run the benchmark Server entry)',
      );
    } else if (!config.serverStats) {
      write(
        'server ipc/handler stats: server-stats disabled (the production Server entry ran without the benchmark counting channel)',
      );
    } else if (serverStats !== undefined) {
      printServerStats(serverStats, config.senders);
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
