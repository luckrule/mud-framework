import { resolve } from 'node:path';

/**
 * Configuration, fixed-bucket histograms and result types shared by the
 * player-level load coordinator and its generator processes.
 *
 * Latency samples are never collected as raw arrays: a saturated run produces
 * far too many values to serialize. Each accumulator is a logarithmic
 * histogram (5% relative bucket width, 1 us to 60 s plus an overflow bucket);
 * percentiles are reported as the upper edge of the bucket that holds the
 * rank, so they are accurate to within 5%. Exact min, max, count and mean are
 * kept alongside the buckets.
 */

export type LoadMode = 'realistic' | 'saturate';

export type CommandName = 'look' | 'who' | 'move' | 'say';

export const COMMAND_NAMES: readonly CommandName[] = [
  'look',
  'who',
  'move',
  'say',
];

/** Rooms used for the setup grouping; kept in the coordinator and generator. */
export const ROOM_IDS = ['square', 'market', 'smithy', 'tavern'] as const;

const DEFAULT_MIX = 'look:60,who:10,move:20,say:10';
const DEFAULT_ROOM_SIZE = 10;
const DEFAULT_DURATION_SEC = 30;
const DEFAULT_WARMUP_SEC = 5;
const DEFAULT_THINK_MS = 1000;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_CLIENTS = 50;
const DEFAULT_GENERATORS = 4;

export interface CommandWeight {
  readonly command: CommandName;
  readonly weight: number;
}

export interface SharedLoadConfig {
  readonly roomSize: number;
  readonly durationSec: number;
  readonly warmupSec: number;
  readonly thinkMs: number;
  readonly mode: LoadMode;
  readonly mix: readonly CommandWeight[];
  readonly requestTimeoutMs: number;
  /** Distinguishes tokens of concurrent runs on a reused Portal. */
  readonly runTag: string;
  readonly externalUrl: string | undefined;
}

export interface MainConfig {
  readonly shared: SharedLoadConfig;
  readonly clients: number;
  readonly generators: number;
  readonly dataDirectory: string | undefined;
  readonly keep: boolean;
}

function parseInteger(name: string, raw: string, minimum: number): number {
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}, got ${raw}`);
  }
  return value;
}

/** Unlike `support.envInt`, this accepts zero (warmup, think time, offsets). */
export function envInteger(
  name: string,
  fallback: number,
  minimum: number,
): number {
  const raw = process.env[name]?.trim();
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  return parseInteger(name, raw, minimum);
}

export function envIntegerRequired(name: string, minimum: number): number {
  const raw = process.env[name]?.trim();
  if (raw === undefined || raw.length === 0) {
    throw new Error(`${name} is required`);
  }
  return parseInteger(name, raw, minimum);
}

export function parseMix(raw: string): readonly CommandWeight[] {
  const weights = new Map<CommandName, number>();
  for (const part of raw.split(',')) {
    const pieces = part.split(':');
    if (pieces.length !== 2) {
      throw new Error(
        `LOAD_MIX entry must be <command>:<weight>, got ${JSON.stringify(part)}`,
      );
    }
    const [name, weightText] = pieces;
    if (name === undefined || weightText === undefined) {
      throw new Error(`LOAD_MIX entry is incomplete: ${JSON.stringify(part)}`);
    }
    const command = COMMAND_NAMES.find((entry) => entry === name);
    if (command === undefined) {
      throw new Error(
        `LOAD_MIX names an unknown command ${JSON.stringify(name)}; expected one of ${COMMAND_NAMES.join(', ')}`,
      );
    }
    if (weights.has(command)) {
      throw new Error(`LOAD_MIX repeats command ${command}`);
    }
    const weight = Number(weightText);
    if (!Number.isInteger(weight) || weight < 0) {
      throw new Error(
        `LOAD_MIX weight for ${command} must be a non-negative integer, got ${JSON.stringify(weightText)}`,
      );
    }
    weights.set(command, weight);
  }
  const mix = COMMAND_NAMES.map((command) => ({
    command,
    weight: weights.get(command) ?? 0,
  }));
  const total = mix.reduce((sum, entry) => sum + entry.weight, 0);
  if (total <= 0) {
    throw new Error(
      'LOAD_MIX must give at least one command a positive weight',
    );
  }
  return mix;
}

export function mixText(mix: readonly CommandWeight[]): string {
  return mix.map((entry) => `${entry.command}:${entry.weight}`).join(',');
}

export function pickCommand(
  mix: readonly CommandWeight[],
  random: () => number = Math.random,
): CommandName {
  const total = mix.reduce((sum, entry) => sum + entry.weight, 0);
  let threshold = random() * total;
  for (const entry of mix) {
    if (entry.weight === 0) {
      continue;
    }
    threshold -= entry.weight;
    if (threshold < 0) {
      return entry.command;
    }
  }
  // Floating-point drift can leave the threshold just above the last edge.
  const last = mix.findLast((entry) => entry.weight > 0);
  return last?.command ?? 'look';
}

/** Round-robin over ROOM_IDS so consecutive groups of `roomSize` spread out. */
export function roomForIndex(globalIndex: number, roomSize: number): string {
  return ROOM_IDS[Math.floor(globalIndex / roomSize) % ROOM_IDS.length];
}

export function effectiveThinkMs(config: SharedLoadConfig): number {
  return config.mode === 'saturate' ? 0 : config.thinkMs;
}

function defaultRunTag(): string {
  const random = Math.floor(Math.random() * 36 ** 4)
    .toString(36)
    .padStart(4, '0');
  return `${process.pid.toString(36)}${Date.now().toString(36).slice(-6)}${random}`;
}

export function readSharedLoadConfig(): SharedLoadConfig {
  const modeText = (process.env.LOAD_MODE?.trim() ?? '').toLowerCase();
  const mode = modeText.length === 0 ? 'realistic' : modeText;
  if (mode !== 'realistic' && mode !== 'saturate') {
    throw new Error(
      `LOAD_MODE must be realistic or saturate, got ${JSON.stringify(modeText)}`,
    );
  }
  const runTagRaw = process.env.LOAD_RUN_TAG?.trim();
  const urlRaw = process.env.LOAD_URL?.trim();
  const mixRaw = process.env.LOAD_MIX?.trim();
  const runTag =
    runTagRaw !== undefined && runTagRaw.length > 0
      ? runTagRaw
      : defaultRunTag();
  if (runTag.includes('|')) {
    throw new Error('LOAD_RUN_TAG must not contain the token separator "|"');
  }
  return {
    roomSize: envInteger('LOAD_ROOM_SIZE', DEFAULT_ROOM_SIZE, 1),
    durationSec: envInteger('LOAD_DURATION_SEC', DEFAULT_DURATION_SEC, 1),
    warmupSec: envInteger('LOAD_WARMUP_SEC', DEFAULT_WARMUP_SEC, 0),
    thinkMs: envInteger('LOAD_THINK_MS', DEFAULT_THINK_MS, 0),
    mode,
    mix: parseMix(
      mixRaw !== undefined && mixRaw.length > 0 ? mixRaw : DEFAULT_MIX,
    ),
    requestTimeoutMs: envInteger(
      'REQUEST_TIMEOUT_MS',
      DEFAULT_REQUEST_TIMEOUT_MS,
      1,
    ),
    runTag,
    externalUrl: urlRaw !== undefined && urlRaw.length > 0 ? urlRaw : undefined,
  };
}

export function readMainConfig(): MainConfig {
  const shared = readSharedLoadConfig();
  const clients = envInteger('LOAD_CLIENTS', DEFAULT_CLIENTS, 1);
  const generatorsRaw = process.env.LOAD_GENERATORS?.trim();
  const generators =
    generatorsRaw === undefined || generatorsRaw.length === 0
      ? Math.min(DEFAULT_GENERATORS, clients)
      : envInteger('LOAD_GENERATORS', DEFAULT_GENERATORS, 1);
  if (generators > clients) {
    throw new Error(
      `LOAD_GENERATORS (${generators}) must not exceed LOAD_CLIENTS (${clients})`,
    );
  }
  const dataRaw = process.env.LOAD_DATA_DIR?.trim();
  return {
    shared,
    clients,
    generators,
    dataDirectory:
      dataRaw !== undefined && dataRaw.length > 0
        ? resolve(dataRaw)
        : undefined,
    keep: process.env.LOAD_KEEP === '1',
  };
}

const HISTOGRAM_RATIO = 1.05;
const FIRST_EDGE_NS = 1_000;
const LAST_EDGE_NS = 60_000_000_000;

function buildEdges(): readonly number[] {
  const edges: number[] = [];
  for (
    let edge = FIRST_EDGE_NS;
    edge < LAST_EDGE_NS;
    edge = Math.ceil(edge * HISTOGRAM_RATIO)
  ) {
    edges.push(edge);
  }
  edges.push(LAST_EDGE_NS);
  return edges;
}

export const HISTOGRAM_EDGES: readonly number[] = buildEdges();
/** One bucket per edge plus a final bucket for samples above the last edge. */
export const HISTOGRAM_BUCKET_COUNT = HISTOGRAM_EDGES.length + 1;

function bucketIndex(valueNs: number): number {
  let low = 0;
  let high = HISTOGRAM_EDGES.length;
  while (low < high) {
    const middle = low + ((high - low) >>> 1);
    const edge = HISTOGRAM_EDGES[middle] ?? LAST_EDGE_NS;
    if (edge < valueNs) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  return low;
}

export interface HistogramJson {
  readonly counts: readonly number[];
  readonly count: number;
  readonly sumNs: number;
  readonly minNs: number;
  readonly maxNs: number;
}

export interface LatencySummary {
  readonly count: number;
  readonly minNs: number;
  readonly p50Ns: number;
  readonly p95Ns: number;
  readonly p99Ns: number;
  readonly maxNs: number;
  readonly meanNs: number;
  /** Samples above the last bucket edge; included in `count`. */
  readonly overflow: number;
}

export class Histogram {
  private readonly counts = Array.from(
    { length: HISTOGRAM_BUCKET_COUNT },
    () => 0,
  );
  private totalCount = 0;
  private totalSumNs = 0;
  private observedMinNs = 0;
  private observedMaxNs = 0;

  get count(): number {
    return this.totalCount;
  }

  record(valueNs: number): void {
    if (!Number.isFinite(valueNs) || valueNs < 0) {
      throw new RangeError(
        `latency sample must be a non-negative finite number, got ${valueNs}`,
      );
    }
    const index = bucketIndex(valueNs);
    this.counts[index] = (this.counts[index] ?? 0) + 1;
    this.totalCount += 1;
    this.totalSumNs += valueNs;
    if (this.totalCount === 1) {
      this.observedMinNs = valueNs;
      this.observedMaxNs = valueNs;
    } else {
      this.observedMinNs = Math.min(this.observedMinNs, valueNs);
      this.observedMaxNs = Math.max(this.observedMaxNs, valueNs);
    }
  }

  mergeJson(part: HistogramJson): void {
    if (part.counts.length !== HISTOGRAM_BUCKET_COUNT) {
      throw new Error(
        `histogram has ${part.counts.length} buckets, expected ${HISTOGRAM_BUCKET_COUNT}`,
      );
    }
    const hadSamples = this.totalCount > 0;
    for (const [index, value] of part.counts.entries()) {
      this.counts[index] = (this.counts[index] ?? 0) + value;
    }
    this.totalCount += part.count;
    this.totalSumNs += part.sumNs;
    if (part.count > 0) {
      if (!hadSamples) {
        this.observedMinNs = part.minNs;
        this.observedMaxNs = part.maxNs;
      } else {
        this.observedMinNs = Math.min(this.observedMinNs, part.minNs);
        this.observedMaxNs = Math.max(this.observedMaxNs, part.maxNs);
      }
    }
  }

  summary(): LatencySummary {
    return {
      count: this.totalCount,
      minNs: this.observedMinNs,
      p50Ns: this.percentileNs(0.5),
      p95Ns: this.percentileNs(0.95),
      p99Ns: this.percentileNs(0.99),
      maxNs: this.observedMaxNs,
      meanNs: this.totalCount === 0 ? 0 : this.totalSumNs / this.totalCount,
      overflow: this.counts[HISTOGRAM_BUCKET_COUNT - 1] ?? 0,
    };
  }

  toJson(): HistogramJson {
    return {
      counts: [...this.counts],
      count: this.totalCount,
      sumNs: this.totalSumNs,
      minNs: this.observedMinNs,
      maxNs: this.observedMaxNs,
    };
  }

  private percentileNs(quantile: number): number {
    if (this.totalCount === 0) {
      return 0;
    }
    const rank = Math.max(1, Math.ceil(quantile * this.totalCount));
    let cumulative = 0;
    for (const [index, value] of this.counts.entries()) {
      cumulative += value;
      if (cumulative >= rank) {
        return HISTOGRAM_EDGES[index] ?? HISTOGRAM_EDGES.at(-1) ?? LAST_EDGE_NS;
      }
    }
    return LAST_EDGE_NS;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function readIntegerField(
  record: Record<string, unknown>,
  field: string,
  label: string,
  minimum = 0,
): number {
  const value = record[field];
  if (
    typeof value !== 'number' ||
    !Number.isInteger(value) ||
    value < minimum
  ) {
    throw new Error(`${label}.${field} must be an integer >= ${minimum}`);
  }
  return value;
}

function readNumberField(
  record: Record<string, unknown>,
  field: string,
  label: string,
): number {
  const value = record[field];
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(`${label}.${field} must be a non-negative finite number`);
  }
  return value;
}

export function parseHistogramJson(
  value: unknown,
  label: string,
): HistogramJson {
  if (!isRecord(value)) {
    throw new Error(`${label} is not an object`);
  }
  const rawCounts = value.counts;
  if (!Array.isArray(rawCounts)) {
    throw new Error(`${label}.counts is not an array`);
  }
  if (rawCounts.length !== HISTOGRAM_BUCKET_COUNT) {
    throw new Error(
      `${label}.counts has ${rawCounts.length} buckets, expected ${HISTOGRAM_BUCKET_COUNT}`,
    );
  }
  let sum = 0;
  const counts = rawCounts.map((entry, index) => {
    if (typeof entry !== 'number' || !Number.isInteger(entry) || entry < 0) {
      throw new Error(
        `${label}.counts[${index}] is not a non-negative integer`,
      );
    }
    sum += entry;
    return entry;
  });
  const count = readIntegerField(value, 'count', label);
  if (count !== sum) {
    throw new Error(
      `${label}.count (${count}) does not match its buckets (${sum})`,
    );
  }
  const minNs = readNumberField(value, 'minNs', label);
  const maxNs = readNumberField(value, 'maxNs', label);
  if (count > 0 && minNs > maxNs) {
    throw new Error(`${label}.minNs is greater than its maxNs`);
  }
  return {
    counts,
    count,
    sumNs: readNumberField(value, 'sumNs', label),
    minNs,
    maxNs,
  };
}

export interface CommandResultJson {
  readonly count: number;
  readonly errors: number;
  readonly histogram: HistogramJson;
}

/**
 * Every `say` carries a token. `epoch` increments when the sender changes
 * rooms and `seq` is the sender's next successful-say counter. Failed say
 * attempts do not consume a seq, so a receiver can count missing sequence
 * numbers inside one (sender, epoch) pair as lost events without mistaking a
 * rejected attempt or a room change for a gap. A timed-out attempt that was
 * nevertheless broadcast can repeat its seq on the retry; that shows up as a
 * duplicate, which is ambiguous with a real double delivery, not as a gap.
 */
export interface EventResultJson {
  /** Measured `say` attempts, successful or not. */
  readonly saysAttempted: number;
  /** Attempts the Server accepted; also the `deliveries/say` denominator. */
  readonly saysSent: number;
  /** Measured attempts that ended in an error. */
  readonly saysFailed: number;
  readonly received: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly staleRoom: number;
  readonly lateEpoch: number;
  readonly unparsed: number;
  readonly histogram: HistogramJson;
}

export interface LoadResultJson {
  readonly version: 1;
  readonly generator: number;
  readonly clients: number;
  readonly setupMs: number;
  readonly startedAtMs: number;
  readonly measuredFromMs: number;
  readonly measuredUntilMs: number;
  readonly commands: Readonly<Record<CommandName, CommandResultJson>>;
  readonly events: EventResultJson;
  readonly errors: Readonly<Record<string, number>>;
  /** Final room per client, for a cheap sanity check that setup worked. */
  readonly rooms: Readonly<Record<string, number>>;
}

function parseCountRecord(
  value: unknown,
  label: string,
): Readonly<Record<string, number>> {
  if (!isRecord(value)) {
    throw new Error(`${label} is not an object`);
  }
  const result: Record<string, number> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (typeof entry !== 'number' || !Number.isInteger(entry) || entry < 0) {
      throw new Error(`${label}.${key} is not a non-negative integer`);
    }
    result[key] = entry;
  }
  return result;
}

function parseCommandResult(value: unknown, label: string): CommandResultJson {
  if (!isRecord(value)) {
    throw new Error(`${label} is not an object`);
  }
  return {
    count: readIntegerField(value, 'count', label),
    errors: readIntegerField(value, 'errors', label),
    histogram: parseHistogramJson(value.histogram, `${label}.histogram`),
  };
}

function parseEventResult(value: unknown): EventResultJson {
  if (!isRecord(value)) {
    throw new Error('events is not an object');
  }
  return {
    saysAttempted: readIntegerField(value, 'saysAttempted', 'events'),
    saysSent: readIntegerField(value, 'saysSent', 'events'),
    saysFailed: readIntegerField(value, 'saysFailed', 'events'),
    received: readIntegerField(value, 'received', 'events'),
    gaps: readIntegerField(value, 'gaps', 'events'),
    duplicates: readIntegerField(value, 'duplicates', 'events'),
    staleRoom: readIntegerField(value, 'staleRoom', 'events'),
    lateEpoch: readIntegerField(value, 'lateEpoch', 'events'),
    unparsed: readIntegerField(value, 'unparsed', 'events'),
    histogram: parseHistogramJson(value.histogram, 'events.histogram'),
  };
}

export function parseLoadResultJson(value: unknown): LoadResultJson {
  if (!isRecord(value)) {
    throw new Error('load result is not an object');
  }
  if (value.version !== 1) {
    throw new Error(`load result version is not 1: ${String(value.version)}`);
  }
  const commandsRaw = value.commands;
  if (!isRecord(commandsRaw)) {
    throw new Error('commands is not an object');
  }
  const measuredFromMs = readNumberField(value, 'measuredFromMs', 'result');
  const measuredUntilMs = readNumberField(value, 'measuredUntilMs', 'result');
  if (measuredUntilMs < measuredFromMs) {
    throw new Error('result measurement window ends before it starts');
  }
  return {
    version: 1,
    generator: readIntegerField(value, 'generator', 'result'),
    clients: readIntegerField(value, 'clients', 'result', 1),
    setupMs: readNumberField(value, 'setupMs', 'result'),
    startedAtMs: readNumberField(value, 'startedAtMs', 'result'),
    measuredFromMs,
    measuredUntilMs,
    commands: {
      look: parseCommandResult(commandsRaw.look, 'commands.look'),
      who: parseCommandResult(commandsRaw.who, 'commands.who'),
      move: parseCommandResult(commandsRaw.move, 'commands.move'),
      say: parseCommandResult(commandsRaw.say, 'commands.say'),
    },
    events: parseEventResult(value.events),
    errors: parseCountRecord(value.errors, 'errors'),
    rooms: parseCountRecord(value.rooms, 'rooms'),
  };
}
