import {
  ConfigStore,
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkSQLStore,
  SparkStageStore,
  SparkStagesStore,
} from "../interfaces/AppStore";
import {
  GpuActivityBreakdown,
  GpuActivityComponent,
  GpuBottleneck,
  GpuDiagnosisStore,
  GpuSemaphoreBreakdown,
  GpuSemaphoreComponent,
  RapidsConfigView,
  StageGpuDiagnosis,
  StageOperatorTime,
} from "../interfaces/GpuDiagnosis";
import {
  ResourceMetricsSummary,
  StageResourceUsage,
} from "../interfaces/ResourceMetrics";
import { RAPIDS_THRESHOLDS as T } from "./Alerts/RapidsRulesConfig";
import { isRapidsNode } from "./SqlReducerUtils";

const RAPIDS_PLUGIN = "com.nvidia.spark.SQLPlugin";

// node metrics that measure time spent inside one operator
const OPERATOR_TIME_METRICS = [
  "op time",
  "op time (shuffle read)",
  "op time (shuffle write partitioning)",
];

const emptySemaphore = (): GpuSemaphoreBreakdown => ({ waitMs: 0, heldMs: 0, notHeldMs: 0 });

const emptyActivity = (): GpuActivityBreakdown => ({
  operatorsMs: 0,
  shuffleReadMs: 0,
  scanIoWaitMs: 0,
  shuffleWriteMs: 0,
  untrackedMs: 0,
});

/** Parses Spark byte-size config values ("512m", "1g", "1073741824"); plain numbers are bytes. */
export function parseConfigBytes(value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const m = value.trim().toLowerCase().match(/^(\d+(?:\.\d+)?)\s*([kmgtp]?)(i?b)?$/);
  if (!m) return undefined;
  const exp = { "": 0, k: 1, m: 2, g: 3, t: 4, p: 5 }[m[2]] ?? 0;
  return parseFloat(m[1]) * Math.pow(1024, exp);
}

function parseIntConf(value: string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const n = parseFloat(value);
  return isNaN(n) ? undefined : n;
}

function isTrue(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined) return defaultValue;
  return value.trim().toLowerCase() === "true";
}

export function isRapidsApp(
  config: ConfigStore | undefined,
  sql: SparkSQLStore | undefined,
): boolean {
  const props = config?.sparkProperties ?? {};
  const plugins = (props["spark.plugins"] ?? "").split(",").map((p) => p.trim());
  const pluginConfigured =
    plugins.includes(RAPIDS_PLUGIN) && isTrue(props["spark.rapids.sql.enabled"], true);
  if (pluginConfigured) return true;
  return (sql?.sqls ?? []).some((s) => s.nodes.some((n) => isRapidsNode(n.nodeName)));
}

export function extractRapidsConfig(props: Record<string, string>): RapidsConfigView {
  const shuffleManager = props["spark.shuffle.manager"];
  return {
    concurrentGpuTasks: parseIntConf(props["spark.rapids.sql.concurrentGpuTasks"]),
    concurrentGpuTasksDynamic: isTrue(props["spark.rapids.sql.concurrentGpuTasks.dynamic"], true),
    maxConcurrentGpuTasks: parseIntConf(props["spark.rapids.sql.maxConcurrentGpuTasks"]),
    executorCores: parseIntConf(props["spark.executor.cores"]),
    taskGpuAmount: parseIntConf(props["spark.task.resource.gpu.amount"]),
    batchSizeBytes: parseConfigBytes(props["spark.rapids.sql.batchSizeBytes"]),
    maxPartitionBytes: parseConfigBytes(props["spark.sql.files.maxPartitionBytes"]),
    pinnedPoolBytes: parseConfigBytes(props["spark.rapids.memory.pinnedPool.size"]),
    offHeapLimitEnabled: isTrue(props["spark.rapids.memory.host.offHeapLimit.enabled"], false),
    shuffleManager,
    isRapidsShuffleManager:
      shuffleManager !== undefined && shuffleManager.includes("RapidsShuffleManager"),
    shuffleMode: (props["spark.rapids.shuffle.mode"] ?? "MULTITHREADED").toUpperCase(),
    skipMerge: isTrue(props["spark.rapids.shuffle.multithreaded.skipMerge"], false),
    kudoSerializer: isTrue(props["spark.rapids.shuffle.kudo.serializer.enabled"], true),
    kudoReadMode: (props["spark.rapids.shuffle.kudo.serializer.read.mode"] ?? "GPU").toUpperCase(),
    shuffleReaderThreads: parseIntConf(props["spark.rapids.shuffle.multiThreaded.reader.threads"]),
    shuffleWriterThreads: parseIntConf(props["spark.rapids.shuffle.multiThreaded.writer.threads"]),
    multiThreadedReadThreads: parseIntConf(props["spark.rapids.sql.multiThreadedRead.numThreads"]),
    parquetReaderType: props["spark.rapids.sql.format.parquet.reader.type"],
    shufflePartitions: parseIntConf(props["spark.sql.shuffle.partitions"]),
    aqeEnabled: isTrue(props["spark.sql.adaptive.enabled"], true),
    aqeParallelismFirst: isTrue(props["spark.sql.adaptive.coalescePartitions.parallelismFirst"], true),
    aqeInitialPartitionNum: parseIntConf(props["spark.sql.adaptive.coalescePartitions.initialPartitionNum"]),
    numConnectionsPerPeer: parseIntConf(props["spark.shuffle.io.numConnectionsPerPeer"]),
    shuffleCompress: isTrue(props["spark.shuffle.compress"], true),
    externalShuffleService: isTrue(props["spark.shuffle.service.enabled"], false),
    metricsLevel: props["spark.rapids.sql.metrics.level"],
  };
}

// How spark-rapids (26.x) measures the node metrics used here:
//  - "op time*" wraps an operator's iterator and subtracts the op time of its children in the
//    same stage (GpuOpTimeTrackingRDD), so op times of different operators are disjoint.
//    Work an operator does while building its iterator (e.g. GpuShuffledSizedHashJoinExec
//    reading its build side inside zipPartitions) is not covered by any op time.
//  - "op time (shuffle read)" of a GpuColumnarExchange and of an AQE GpuCustomShuffleReader
//    measure different reads (the AQE reader builds its own ShuffledBatchRDD), so they add up.
//  - "scan time" is the wall time of the scan's hasNext: waiting for the host reader threads,
//    acquiring the GPU semaphore, the host->device copy and the GPU decode. "buffer time" is the
//    part of it the task spent blocked on the host reader ("filter time" / "I/O schedule time"
//    complete it, but only exist at DEBUG metrics level).
interface StageNodeData {
  sql: EnrichedSparkSQL;
  // "op time" of scan nodes, falls back to "scan time" on versions without op time
  scanOpMs: number;
  scanTimeMs: number;
  decodeTimeMs: number;
  bufferTimeMs: number;
  shuffleReadOpMs: number;
  // "op time" of the other operators plus "op time (shuffle write partitioning)"
  operatorOpMs: number;
  hasOpTime: boolean;
  hasRapidsNodes: boolean;
  operators: StageOperatorTime[];
}

function nodeStageIds(node: EnrichedSqlNode): number[] {
  if (node.stage?.type === "onestage") return [node.stage.stageId];
  if (node.stage?.type === "exchange") return [node.stage.writeStage, node.stage.readStage];
  return node.nodeIdFromMetrics !== undefined ? [node.nodeIdFromMetrics] : [];
}

/** Attributes RAPIDS node metrics to stages, using the stage embedded in each metric value. */
function collectStageNodeData(sql: SparkSQLStore | undefined): Map<number, StageNodeData> {
  const result = new Map<number, StageNodeData>();
  const get = (stageId: number, s: EnrichedSparkSQL): StageNodeData => {
    let data = result.get(stageId);
    if (data === undefined) {
      data = {
        sql: s,
        scanOpMs: 0,
        scanTimeMs: 0,
        decodeTimeMs: 0,
        bufferTimeMs: 0,
        shuffleReadOpMs: 0,
        operatorOpMs: 0,
        hasOpTime: false,
        hasRapidsNodes: false,
        operators: [],
      };
      result.set(stageId, data);
    }
    return data;
  };

  for (const s of sql?.sqls ?? []) {
    for (const node of s.nodes) {
      const defaultStages = nodeStageIds(node);
      defaultStages.forEach((stageId) => get(stageId, s));
      if (node.rapidsMetrics === undefined) continue;
      const isScan = node.nodeName.includes("Scan");
      for (const [name, metric] of Object.entries(node.rapidsMetrics)) {
        const stageId =
          metric.stageId ?? (defaultStages.length === 1 ? defaultStages[0] : undefined);
        if (stageId === undefined) continue;
        const data = get(stageId, s);
        data.hasRapidsNodes = true;
        if (isScan && name === "scan time") {
          data.scanTimeMs += metric.value;
        } else if (isScan && name === "GPU decode time") {
          data.decodeTimeMs += metric.value;
        } else if (isScan && name === "buffer time") {
          data.bufferTimeMs += metric.value;
        }
        if (OPERATOR_TIME_METRICS.includes(name)) {
          data.hasOpTime = true;
          if (name === "op time (shuffle read)") data.shuffleReadOpMs += metric.value;
          else if (isScan) data.scanOpMs += metric.value;
          else data.operatorOpMs += metric.value;
        }
        if (OPERATOR_TIME_METRICS.includes(name) && metric.value > 0) {
          data.operators.push({
            nodeId: node.nodeId,
            nodeName: node.nodeName,
            enrichedName: node.enrichedName,
            metricName: name,
            timeMs: metric.value,
          });
        }
      }
    }
  }
  return result;
}

function sumAcc(acc: Record<string, number>, keys: string[]): number {
  return keys.reduce((sum, k) => sum + (acc[k] ?? 0), 0);
}

function validPct(v: number | undefined): number | undefined {
  return v === undefined || v < 0 ? undefined : v;
}

/** GPU activity of a stage: GPM SM activity when available, otherwise utilization.gpu. */
export function stageGpuActivityPct(resource: StageResourceUsage | undefined): number | undefined {
  return validPct(resource?.avgSmActivityPct) ?? validPct(resource?.avgGpuUtilPct);
}

function pct(v: number): string {
  return `${Math.round(v * 100)}%`;
}

/** Disjoint candidate components for the primary bottleneck (all in ms of task time). */
export interface StageTimeCandidates {
  semaphoreWaitMs: number;
  gpuHeldMs: number;
  shuffleWriteMs: number;
  shuffleReadMs: number;
  scanIoWaitMs: number;
  hostWorkMs: number;
  spillMs: number;
  // lower bound of gpuHeldMs spent waiting for scan / shuffle input
  heldWhileWaitingMinMs: number;
}

export function classifyStage(
  t: StageTimeCandidates,
  taskTimeMs: number,
  resource: StageResourceUsage | undefined,
): { bottleneck: GpuBottleneck; secondary: GpuBottleneck[]; reason: string } {
  const gpuPct = stageGpuActivityPct(resource);
  const cpuPct = validPct(resource?.avgCpuUtilPct);
  const share = (ms: number) => (taskTimeMs > 0 ? ms / taskTimeMs : 0);

  // "held" is semaphore wall time, not kernel time. Use the sampled SM activity when we have it;
  // otherwise, if at least half of the held time provably went to waiting for input, the GPU
  // was not the limit either.
  const mostlyWaiting = t.gpuHeldMs > 0 && t.heldWhileWaitingMinMs >= t.gpuHeldMs / 2;
  const gpuHeld: GpuBottleneck =
    gpuPct !== undefined
      ? gpuPct < T.gpuIdlePct
        ? "gpu-underutilized"
        : "gpu-compute"
      : mostlyWaiting
        ? "gpu-underutilized"
        : "gpu-compute";

  const candidates: { b: GpuBottleneck; share: number }[] = [
    { b: "semaphore", share: share(t.semaphoreWaitMs) },
    { b: gpuHeld, share: share(t.gpuHeldMs) },
    { b: "shuffle-write", share: share(t.shuffleWriteMs) },
    { b: "shuffle-read", share: share(t.shuffleReadMs) },
    { b: "scan-io", share: share(t.scanIoWaitMs) },
    { b: "cpu", share: share(t.hostWorkMs) },
    { b: "spill", share: share(t.spillMs) },
  ];
  candidates.sort((a, b) => b.share - a.share);

  const primary = candidates[0];
  const secondary = candidates
    .slice(1)
    .filter((c) => c.share >= T.secondaryMinShare)
    .map((c) => c.b);

  const parts: string[] = [];
  parts.push(`${pct(primary.share)} of task time`);
  if (primary.share < T.primaryMinShare) parts.push("(no single dominant component)");
  if (
    (primary.b === "gpu-compute" || primary.b === "gpu-underutilized") &&
    share(t.heldWhileWaitingMinMs) >= T.secondaryMinShare
  ) {
    parts.push(
      `at least ${pct(share(t.heldWhileWaitingMinMs))} of task time held the GPU while waiting for scan / shuffle input`,
    );
  }
  if (gpuPct !== undefined) parts.push(`GPU activity ${Math.round(gpuPct)}%`);
  if (cpuPct !== undefined) parts.push(`executor CPU ${Math.round(cpuPct)}%`);
  return { bottleneck: primary.b, secondary, reason: parts.join(", ") };
}

/** Axis 1: exact split of the run time by GPU semaphore state. */
function semaphoreBreakdown(waitMs: number, heldMs: number, runMs: number): GpuSemaphoreBreakdown {
  // wait + held <= run time for single threaded tasks; tasks with extra threads (Python UDFs)
  // can count a wait per thread, so clamp
  const scale = waitMs + heldMs > runMs ? runMs / (waitMs + heldMs) : 1;
  const wait = waitMs * scale;
  const held = heldMs * scale;
  return { waitMs: wait, heldMs: held, notHeldMs: Math.max(0, runMs - wait - held) };
}

/** Axis 2: split of the run time by what the task thread did, from op time. */
function activityBreakdown(
  nodeData: StageNodeData | undefined,
  shuffleWriteMs: number,
  shuffleFetchWaitMs: number,
  runMs: number,
): { measured: GpuActivityBreakdown; scaled: GpuActivityBreakdown; coveredMs: number } {
  const scanOpMs = nodeData === undefined ? 0 : nodeData.scanOpMs > 0 ? nodeData.scanOpMs : nodeData.scanTimeMs;
  const bufferMs = nodeData?.bufferTimeMs ?? 0;
  const scanIoWaitMs = scanOpMs > 0 ? Math.min(bufferMs, scanOpMs) : bufferMs;
  const shuffleReadOpMs = nodeData?.shuffleReadOpMs ?? 0;
  const measured: GpuActivityBreakdown = {
    operatorsMs: (nodeData?.operatorOpMs ?? 0) + Math.max(0, scanOpMs - scanIoWaitMs),
    // the reader's op time includes the fetch wait; without op time fall back to fetch wait
    shuffleReadMs: shuffleReadOpMs > 0 ? shuffleReadOpMs : shuffleFetchWaitMs,
    scanIoWaitMs,
    shuffleWriteMs,
    untrackedMs: 0,
  };
  const coveredMs =
    measured.operatorsMs + measured.shuffleReadMs + measured.scanIoWaitMs + measured.shuffleWriteMs;
  measured.untrackedMs = Math.max(0, runMs - coveredMs);
  // more than the run time only happens when a shared operator's metric total (one value for
  // all stages that ran it) was attributed to this stage; scale it back
  const scale = coveredMs > runMs ? runMs / coveredMs : 1;
  const scaled: GpuActivityBreakdown = {
    operatorsMs: measured.operatorsMs * scale,
    shuffleReadMs: measured.shuffleReadMs * scale,
    scanIoWaitMs: measured.scanIoWaitMs * scale,
    shuffleWriteMs: measured.shuffleWriteMs * scale,
    untrackedMs: Math.max(0, runMs - coveredMs * scale),
  };
  return { measured, scaled, coveredMs };
}

function diagnoseStage(
  stage: SparkStageStore,
  nodeData: StageNodeData | undefined,
  resource: StageResourceUsage | undefined,
): StageGpuDiagnosis | undefined {
  const b = stage.breakdown;
  if (b === undefined || b.executorRunTimeMs <= 0) return undefined;
  const acc = stage.accumulators ?? {};
  const hasGpuAccumulators = Object.keys(acc).some((k) => k.startsWith("gpu"));
  const isGpuStage = hasGpuAccumulators || (nodeData?.hasRapidsNodes ?? false);
  const taskTimeMs = b.executorRunTimeMs;

  const hasSemaphoreMetrics = acc["gpuTime"] !== undefined || acc["gpuSemaphoreWait"] !== undefined;
  const semaphore = semaphoreBreakdown(acc["gpuSemaphoreWait"] ?? 0, acc["gpuTime"] ?? 0, taskTimeMs);
  const { measured, scaled: activity, coveredMs } = activityBreakdown(
    nodeData,
    b.shuffleWriteTimeMs,
    b.shuffleFetchWaitMs,
    taskTimeMs,
  );
  const hasOpTime = nodeData?.hasOpTime ?? false;

  // semaphore wait, shuffle write, shuffle read and scan IO wait are disjoint parts of the task
  // thread (the GPU is acquired outside the reader / writer / host reader waits)
  const waitingMs =
    semaphore.waitMs + activity.shuffleWriteMs + activity.shuffleReadMs + activity.scanIoWaitMs;
  const workMs = Math.max(0, taskTimeMs - waitingMs);
  const heldWhileWaitingMinMs = hasSemaphoreMetrics ? Math.max(0, semaphore.heldMs - workMs) : 0;
  // the fetch wait is spent on the task thread (RapidsShuffleThreadedReaderBase pulls the fetcher
  // iterator there), so held time that does not fit next to it was held while blocked on fetch
  const heldDuringFetchMinMs = hasSemaphoreMetrics
    ? Math.max(0, semaphore.heldMs - Math.max(0, taskTimeMs - b.shuffleFetchWaitMs - semaphore.waitMs))
    : 0;
  // shuffle write never holds the GPU (partitioning releases it), the input waits may
  const hostWorkMs = hasSemaphoreMetrics
    ? Math.max(
        0,
        semaphore.notHeldMs - activity.shuffleWriteMs - activity.shuffleReadMs - activity.scanIoWaitMs,
      )
    : activity.untrackedMs;

  const spillTimeMs = sumAcc(acc, [
    "gpuSpillToHostTime",
    "gpuReadSpillFromHostTime",
    "gpuSpillToDiskTime",
    "gpuReadSpillFromDiskTime",
    "gpuRetryBlockTime",
  ]);
  const { bottleneck, secondary, reason } = classifyStage(
    {
      semaphoreWaitMs: semaphore.waitMs,
      // old spark-rapids without gpuTime: approximate with the GPU operators' op time
      gpuHeldMs: hasSemaphoreMetrics ? semaphore.heldMs : activity.operatorsMs,
      shuffleWriteMs: activity.shuffleWriteMs,
      shuffleReadMs: activity.shuffleReadMs,
      scanIoWaitMs: activity.scanIoWaitMs,
      hostWorkMs,
      spillMs: spillTimeMs,
      heldWhileWaitingMinMs,
    },
    taskTimeMs,
    resource,
  );

  const executors = resource?.numExecutors ?? 0;
  const durationMs = stage.stageRealTimeDurationMs ?? 0;
  const avgGpuHolders =
    hasSemaphoreMetrics && executors > 0 && durationMs > 0
      ? semaphore.heldMs / (durationMs * executors)
      : undefined;

  const operators = [...(nodeData?.operators ?? [])]
    .sort((x, y) => y.timeMs - x.timeMs)
    .slice(0, 5);

  return {
    stageId: stage.stageId,
    attemptId: stage.attemptId,
    sqlId: nodeData?.sql.id,
    sqlDescription: nodeData?.sql.description,
    status: stage.status,
    isGpuStage,
    numTasks: stage.numTasks,
    durationMs: stage.stageRealTimeDurationMs,
    taskTimeMs,
    cpuTimeMs: b.executorCpuTimeMs,
    gcTimeMs: b.jvmGcTimeMs,
    hasSemaphoreMetrics,
    semaphore,
    hasOpTime,
    activityMeasured: measured,
    activity,
    opTimeCoverage: hasOpTime ? coveredMs / taskTimeMs : undefined,
    scanOpMs: nodeData === undefined ? 0 : nodeData.scanOpMs > 0 ? nodeData.scanOpMs : nodeData.scanTimeMs,
    scanDecodeMs: nodeData?.decodeTimeMs ?? 0,
    shuffleFetchWaitMs: b.shuffleFetchWaitMs,
    maxConcurrentGpuTasks: acc["gpuMaxConcurrentGpuTasks"],
    avgGpuHolders,
    heldWhileWaitingMinMs,
    heldDuringFetchMinMs,
    hostWorkMs,
    spillToHostBytes: acc["gpuSpillToHostBytes"] ?? 0,
    spillToDiskBytes: acc["gpuSpillToDiskBytes"] ?? 0,
    sparkSpillBytes: b.diskBytesSpilled,
    retryCount: acc["gpuRetryCount"] ?? 0,
    splitRetryCount: acc["gpuSplitAndRetryCount"] ?? 0,
    retryBlockTimeMs: acc["gpuRetryBlockTime"] ?? 0,
    medianTaskInputBytes: b.medianTaskInputBytes,
    resource,
    topOperators: operators,
    bottleneck,
    secondary,
    reason,
  };
}

function weightedAvg(
  stages: StageResourceUsage[],
  f: (s: StageResourceUsage) => number,
): number | undefined {
  let sum = 0;
  let weight = 0;
  for (const s of stages) {
    const v = f(s);
    if (v >= 0 && s.sampledSeconds > 0) {
      sum += v * s.sampledSeconds;
      weight += s.sampledSeconds;
    }
  }
  return weight === 0 ? undefined : sum / weight;
}

export function calculateGpuDiagnosis(
  sql: SparkSQLStore | undefined,
  stages: SparkStagesStore | undefined,
  config: ConfigStore | undefined,
  resourceMetrics: ResourceMetricsSummary | undefined,
): GpuDiagnosisStore | undefined {
  if (!isRapidsApp(config, sql)) return undefined;
  const props = config?.sparkProperties ?? {};
  const nodeData = collectStageNodeData(sql);
  const resourceByStage = new Map<string, StageResourceUsage>();
  for (const r of resourceMetrics?.stages ?? []) {
    resourceByStage.set(`${r.stageId}_${r.stageAttemptId}`, r);
  }

  const diagnosed: StageGpuDiagnosis[] = [];
  for (const stage of stages ?? []) {
    const d = diagnoseStage(
      stage,
      nodeData.get(stage.stageId),
      resourceByStage.get(`${stage.stageId}_${stage.attemptId}`),
    );
    if (d !== undefined && d.isGpuStage) diagnosed.push(d);
  }
  diagnosed.sort((a, b) => b.taskTimeMs - a.taskTimeMs);

  const totalSemaphore = emptySemaphore();
  const totalActivity = emptyActivity();
  const bottleneckTaskTimeMs: Record<GpuBottleneck, number> = {
    "gpu-compute": 0,
    "gpu-underutilized": 0,
    semaphore: 0,
    "shuffle-write": 0,
    "shuffle-read": 0,
    "scan-io": 0,
    cpu: 0,
    spill: 0,
  };
  let totalTaskTimeMs = 0;
  for (const d of diagnosed) {
    totalTaskTimeMs += d.taskTimeMs;
    for (const k of Object.keys(totalSemaphore) as GpuSemaphoreComponent[]) {
      totalSemaphore[k] += d.semaphore[k];
    }
    for (const k of Object.keys(totalActivity) as GpuActivityComponent[]) {
      totalActivity[k] += d.activity[k];
    }
    bottleneckTaskTimeMs[d.bottleneck] += d.taskTimeMs;
  }

  const sampledStages = resourceMetrics?.stages ?? [];
  const rapidsConfig = extractRapidsConfig(props);
  const maxSeen = diagnosed
    .map((d) => d.maxConcurrentGpuTasks)
    .filter((v): v is number => v !== undefined);
  return {
    isRapidsApp: true,
    hasResourceSamples: resourceMetrics?.enabled === true,
    gpuSource: resourceMetrics?.gpuSource ?? "none",
    config: rapidsConfig,
    dynamicConcurrency: rapidsConfig.concurrentGpuTasksDynamic && maxSeen.length > 0,
    maxConcurrentGpuTasksSeen: maxSeen.length > 0 ? Math.max(...maxSeen) : undefined,
    totalTaskTimeMs,
    totalSemaphore,
    totalActivity,
    bottleneckTaskTimeMs,
    avgSmActivityPct: weightedAvg(sampledStages, (s) => s.avgSmActivityPct),
    avgGpuUtilPct: weightedAvg(sampledStages, (s) => s.avgGpuUtilPct),
    avgCpuUtilPct: weightedAvg(sampledStages, (s) => s.avgCpuUtilPct),
    stages: diagnosed,
  };
}
