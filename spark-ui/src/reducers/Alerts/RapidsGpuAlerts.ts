import { duration } from "moment";
import { Alert, Alerts, EnrichedSqlNode, SparkSQLStore } from "../../interfaces/AppStore";
import {
  GpuDiagnosisStore,
  RapidsConfigView,
  StageGpuDiagnosis,
} from "../../interfaces/GpuDiagnosis";
import { humanFileSize, humanizeTimeDiff } from "../../utils/FormatUtils";
import { parseMetricNumber } from "../../utils/MetricValueParser";
import { stageGpuActivityPct } from "../GpuDiagnosisReducer";
import { isRapidsNode } from "../SqlReducerUtils";
import { RAPIDS_DEFAULTS as D, RAPIDS_THRESHOLDS as T } from "./RapidsRulesConfig";

// ---------------------------------------------------------------------------------------------
// RAPIDS (spark-rapids / cudf) GPU alerts. Three groups:
//   1. stage bottleneck rules, driven by the per-stage task-time breakdown (GpuDiagnosisReducer)
//   2. SQL rules (operators that fell back to the CPU)
//   3. application config rules, learned from tuning runs (see RapidsRulesConfig.ts)
// Every rule can be switched off with spark.dataflint.alert.disabled=<rule name>.
// ---------------------------------------------------------------------------------------------

const ms = (v: number) => humanizeTimeDiff(duration(v));
const share = (part: number, total: number) =>
  total > 0 ? `${Math.round((part / total) * 100)}%` : "0%";
const numbered = (items: string[]) => items.map((s, i) => `${i + 1}. ${s}`).join("\n");

function currentValue(value: number | string | undefined, fallback: string): string {
  return value === undefined ? `not set, default ${fallback}` : `currently ${value}`;
}

function stageAlert(
  d: StageGpuDiagnosis,
  name: string,
  title: string,
  message: string,
  suggestions: string[],
  shortSuggestion: string,
  type: "error" | "warning" = "warning",
): Alert {
  const location =
    d.sqlId !== undefined
      ? `In: SQL query "${d.sqlDescription}" (id: ${d.sqlId}), Stage ${d.stageId}`
      : `In: Stage ${d.stageId}`;
  return {
    id: `${name}_stage_${d.stageId}_${d.attemptId}`,
    name,
    title,
    location,
    message,
    suggestion: `\n${numbered(suggestions)}\n`,
    shortSuggestion,
    type,
    source:
      d.sqlId !== undefined
        ? { type: "stage", sqlId: d.sqlId, stageId: d.stageId }
        : { type: "status", metric: "gpu" },
  };
}

function appAlert(
  name: string,
  title: string,
  message: string,
  suggestions: string[],
  shortSuggestion: string,
  type: "error" | "warning" = "warning",
): Alert {
  return {
    id: name,
    name,
    title,
    location: "In: Application configuration (RAPIDS GPU)",
    message,
    suggestion: `\n${numbered(suggestions)}\n`,
    shortSuggestion,
    type,
    source: { type: "status", metric: "gpu" },
  };
}

function gpuActivityText(d: StageGpuDiagnosis): string {
  const gpu = stageGpuActivityPct(d.resource);
  if (gpu === undefined) return "GPU activity unknown (enable the DataFlint resource monitor)";
  const kind = (d.resource?.avgSmActivityPct ?? -1) >= 0 ? "SM activity" : "GPU utilization";
  return `${kind} ${Math.round(gpu)}%`;
}

/** "about 12 tasks per GPU held it at once on average (max 32)", or "" without the data. */
function holdersText(d: StageGpuDiagnosis): string {
  const parts: string[] = [];
  if (d.avgGpuHolders !== undefined) parts.push(`about ${d.avgGpuHolders.toFixed(1)} tasks per GPU on average`);
  if (d.maxConcurrentGpuTasks !== undefined) parts.push(`at most ${d.maxConcurrentGpuTasks}`);
  return parts.length === 0 ? "" : `held the GPU semaphore at the same time: ${parts.join(", ")}`;
}

// With dynamic concurrency (spark-rapids 25.06+, GpuSemaphore / GpuStageMemoryEstimator) a task
// takes one permit per 32MiB of its estimated GPU memory, the estimate being the 80th
// percentile of the peak GPU memory of the stage's tasks; the GPU has one permit per 32MiB.
// spark.rapids.sql.concurrentGpuTasks only sets the estimate a stage starts with.
const DYNAMIC_CONCURRENCY_NOTE =
  "GPU concurrency is dynamic (spark.rapids.sql.concurrentGpuTasks.dynamic=true): spark-rapids admits tasks until the 80th percentile of their measured peak GPU memory fills the GPU, spark.rapids.sql.concurrentGpuTasks only sets the starting estimate of each stage";

/**
 * The scan's "GPU decode time" dominates the stage but the SMs are idle and the executor CPU is
 * saturated: the decode call is waiting on host side work, not on GPU kernels.
 */
function isHostBoundDecode(d: StageGpuDiagnosis): boolean {
  const gpu = stageGpuActivityPct(d.resource);
  const cpu = d.resource?.avgCpuUtilPct ?? -1;
  return (
    gpu !== undefined &&
    gpu < T.gpuIdlePct &&
    cpu >= T.cpuBusyPct &&
    d.scanDecodeMs >= T.minComponentMs &&
    d.scanDecodeMs / d.taskTimeMs >= T.hostBoundDecodeShare &&
    d.scanOpMs > 0 &&
    d.scanDecodeMs >= d.scanOpMs * T.decodeShareOfScan
  );
}

// ------------------------------------ stage rules --------------------------------------------

function semaphoreRule(
  d: StageGpuDiagnosis,
  c: RapidsConfigView,
  alerts: Alerts,
  diag: GpuDiagnosisStore,
) {
  const sem = d.semaphore.waitMs;
  if (sem < T.minComponentMs || sem / d.taskTimeMs < T.semaphoreShare) return;
  const gpu = stageGpuActivityPct(d.resource);
  const concurrency = c.concurrentGpuTasks ?? D.concurrentGpuTasks;
  const cores = c.executorCores;
  const memoryPressure =
    d.spillToHostBytes + d.spillToDiskBytes >= T.spillBytes ||
    d.retryCount + d.splitRetryCount >= T.retryCount;
  const holders = holdersText(d);
  const message = `Tasks waited ${ms(sem)} (${share(sem, d.taskTimeMs)} of task time) to get onto the GPU, ${gpuActivityText(d)}${
    holders ? `; tasks that ${holders}` : ""
  }`;

  if (gpu !== undefined && gpu >= T.gpuBusyPct) {
    alerts.push(
      stageAlert(
        d,
        "gpuSemaphoreWait",
        "GPU Saturated - Tasks Queue On The GPU Semaphore",
        message,
        [
          "The GPU is already busy, admitting more tasks will not make this stage faster",
          `Reduce GPU work in this stage, the most expensive operators are: ${
            d.topOperators.map((o) => `${o.enrichedName} (${ms(o.timeMs)})`).join(", ") || "n/a"
          }`,
          "Spread the work over more GPUs (more executors / GPUs), or reduce data earlier (filters, column pruning, broadcast joins)",
        ],
        "GPU bound - reduce GPU work or add GPUs",
      ),
    );
    return;
  }

  const suggestions: string[] = [];
  if (diag.dynamicConcurrency) {
    // the cap is GPU memory, not the configured concurrency
    suggestions.push(
      `${DYNAMIC_CONCURRENCY_NOTE}. Waiting here means the admitted tasks' estimated memory filled the GPU; raising concurrentGpuTasks (${currentValue(c.concurrentGpuTasks, String(D.concurrentGpuTasks))}) changes little`,
    );
    suggestions.push(
      memoryPressure
        ? "This stage also spilled / retried, so the memory estimate is real: fix memory pressure first (more partitions, smaller batches)"
        : "To admit more tasks lower the GPU memory per task: more / smaller partitions (spark.sql.adaptive.coalescePartitions.initialPartitionNum, spark.sql.files.maxPartitionBytes) or a smaller spark.rapids.sql.batchSizeBytes - smaller batches cost GPU efficiency, measure both",
    );
    suggestions.push(
      "Or pin a static concurrency: spark.rapids.sql.concurrentGpuTasks.dynamic=false with a larger spark.rapids.sql.concurrentGpuTasks, and watch the GPU memory pressure alerts",
    );
  } else if (memoryPressure) {
    suggestions.push(
      `This stage also spilled / retried on the GPU, keep spark.rapids.sql.concurrentGpuTasks (${currentValue(c.concurrentGpuTasks, String(D.concurrentGpuTasks))}) and fix memory pressure first (more partitions, smaller batches)`,
    );
  } else {
    suggestions.push(
      `Increase spark.rapids.sql.concurrentGpuTasks (${currentValue(c.concurrentGpuTasks, String(D.concurrentGpuTasks))}) to ${Math.min(
        concurrency * 2,
        cores ?? concurrency * 2,
      )}, more tasks can share the GPU (concurrency 4 vs 32: +7.6% runtime on NDS1K)`,
    );
  }
  if (!diag.dynamicConcurrency && cores !== undefined && cores / concurrency >= T.coresPerGpuTaskRatio) {
    suggestions.push(
      `spark.executor.cores=${cores} is ${Math.round(cores / concurrency)}x the GPU concurrency: most task threads only queue on the semaphore. Lower executor cores towards the GPU concurrency (32 cores + concurrency 32 was fastest, 96 cores was slower)`,
    );
  }
  if (gpu === undefined) {
    suggestions.push(
      "Enable spark.dataflint.resourceMonitor.enabled=true to see whether the GPU is actually busy while tasks wait",
    );
  }
  alerts.push(
    stageAlert(
      d,
      "gpuSemaphoreWait",
      "High GPU Semaphore Wait",
      message,
      suggestions,
      memoryPressure
        ? "fix GPU memory pressure first"
        : diag.dynamicConcurrency
          ? "less GPU memory per task"
          : `concurrentGpuTasks=${Math.min(concurrency * 2, cores ?? concurrency * 2)}`,
    ),
  );
}

function shuffleWriteRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  const w = d.activity.shuffleWriteMs;
  if (w < T.minComponentMs || w / d.taskTimeMs < T.shuffleWriteShare) return;
  const suggestions: string[] = [];
  if (!c.isRapidsShuffleManager) {
    suggestions.push(
      "Use the RAPIDS shuffle manager: spark.shuffle.manager=com.nvidia.spark.rapids.spark<version>.RapidsShuffleManager (MULTITHREADED mode)",
    );
  } else if (c.shuffleMode === "MULTITHREADED" && !c.skipMerge && !c.externalShuffleService) {
    suggestions.push(
      "Set spark.rapids.shuffle.multithreaded.skipMerge=true, otherwise every map task merges and rewrites its shuffle files (NDS-H 1K: ~400s -> 281s)",
    );
  }
  if (c.isRapidsShuffleManager) {
    suggestions.push(
      `Raise spark.rapids.shuffle.multiThreaded.writer.threads (${currentValue(c.shuffleWriterThreads, String(D.shuffleThreads))}), e.g. 40`,
    );
  }
  if (!c.shuffleCompress) {
    suggestions.push("Enable spark.shuffle.compress=true (disabling it was 23% slower)");
  }
  const diskWrite = d.resource?.avgDiskWriteMBps ?? -1;
  if (diskWrite >= 0) {
    suggestions.push(
      `Executors wrote ${Math.round(diskWrite)} MB/s to disk during this stage, if this is near the device limit spread spark.local.dir over more disks`,
    );
  }
  suggestions.push("Reduce the shuffled data: broadcast small sides, filter / project before the exchange");
  alerts.push(
    stageAlert(
      d,
      "gpuShuffleWrite",
      "GPU Stage Bound By Shuffle Write",
      `Shuffle write took ${ms(w)} (${share(w, d.taskTimeMs)} of task time)`,
      suggestions,
      !c.skipMerge && c.isRapidsShuffleManager ? "skipMerge=true" : "speed up shuffle write",
    ),
  );
}

function shuffleReadRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  const read = d.activity.shuffleReadMs;
  if (read < T.minComponentMs || read / d.taskTimeMs < T.shuffleFetchShare) return;
  const fetch = d.shuffleFetchWaitMs;
  // fetch wait = waiting for blocks over the network / from disk; the rest of the RAPIDS shuffle
  // read time is host side deserialization / decompression / concatenation
  const fetchDominated = fetch >= read / 2;
  const conns = c.numConnectionsPerPeer ?? D.numConnectionsPerPeer;
  const suggestions: string[] = [];
  if (fetchDominated && conns < 4) {
    suggestions.push(
      `Set spark.shuffle.io.numConnectionsPerPeer=4 (${currentValue(c.numConnectionsPerPeer, "1")}); a single connection per peer serialised the fetches (SSB 140.7s -> 110.4s; NDS-H 1K q8 34.2s -> 28.3s, its join stages' fetch wait 79% -> 2-5%)`,
    );
  }
  if (c.isRapidsShuffleManager) {
    suggestions.push(
      `Raise spark.rapids.shuffle.multiThreaded.reader.threads (${currentValue(c.shuffleReaderThreads, String(D.shuffleThreads))})${
        fetchDominated ? "" : ", deserialization / decompression of the blocks runs on these threads"
      }`,
    );
  }
  if (!fetchDominated) {
    const cpu = d.resource?.avgCpuUtilPct ?? -1;
    suggestions.push(
      cpu >= 0
        ? `Executor CPU was ${Math.round(cpu)}% busy during this stage${cpu >= T.cpuBusyPct ? " - the host CPU is the limit, give each GPU more cores" : ""}`
        : "Enable spark.dataflint.resourceMonitor.enabled=true to see whether the host CPU is saturated while reading shuffle data",
    );
  }
  suggestions.push(
    "Check partition skew in this stage and the number of reduce partitions (spark.sql.adaptive.coalescePartitions.initialPartitionNum)",
  );
  alerts.push(
    stageAlert(
      d,
      "gpuShuffleRead",
      fetchDominated ? "GPU Stage Waiting For Shuffle Data" : "GPU Stage Bound By Shuffle Read (Deserialization)",
      `Reading shuffle data took ${ms(read)} (${share(read, d.taskTimeMs)} of task time), of which ${ms(fetch)} was fetch wait`,
      suggestions,
      fetchDominated && conns < 4 ? "numConnectionsPerPeer=4" : "more shuffle reader threads",
    ),
  );
}

function scanIoRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  const s = d.activity.scanIoWaitMs;
  if (s < T.minComponentMs || s / d.taskTimeMs < T.scanIoShare) return;
  const suggestions: string[] = [];
  suggestions.push(
    `Raise spark.rapids.sql.multiThreadedRead.numThreads (${currentValue(c.multiThreadedReadThreads, String(D.multiThreadedReadThreads))}) so more files are read / decompressed in parallel`,
  );
  if (c.parquetReaderType !== undefined && c.parquetReaderType.toUpperCase() === "PERFILE") {
    suggestions.push("spark.rapids.sql.format.parquet.reader.type=PERFILE reads one file at a time, use AUTO / MULTITHREADED");
  }
  const disk = d.resource?.avgDiskReadMBps ?? -1;
  const cpu = d.resource?.avgCpuUtilPct ?? -1;
  if (cpu >= T.cpuBusyPct) {
    suggestions.push(
      `Executor CPU was ${Math.round(cpu)}% busy: host side decompression / parsing is the limit, give each GPU more CPU cores or use a cheaper codec`,
    );
  } else if (disk >= 0) {
    suggestions.push(
      `Executors read ${Math.round(disk)} MB/s from disk, compare with the storage bandwidth; if it is the limit, faster storage or caching helps more than GPU settings`,
    );
  }
  alerts.push(
    stageAlert(
      d,
      "gpuScanIO",
      "GPU Waiting For Input Data (Scan IO / Decode)",
      `Scan tasks were blocked ${ms(s)} (${share(s, d.taskTimeMs)} of task time, "buffer time") until the host reader threads had read and decompressed their input, ${gpuActivityText(d)}`,
      suggestions,
      "more multiThreadedRead threads",
    ),
  );
}

/** kudo shuffle blocks are concatenated on the GPU, under the GPU semaphore */
function isKudoGpuRead(c: RapidsConfigView): boolean {
  return c.kudoSerializer && c.kudoReadMode === "GPU" && c.shuffleMode !== "UCX";
}

/**
 * Most of the held time is the task waiting for shuffle data while it holds the semaphore:
 * provably blocked on fetch, or (kudo GPU read mode) provably waiting for input in a stage that
 * mostly reads shuffle data. Measured on NDS-H 1K q9 / q8: kudo read.mode=CPU dropped the held
 * time of these stages from 82-87% (and 39-47%) of task time to 2-5% with the same stage time.
 */
function isHeldOnShuffleRead(d: StageGpuDiagnosis, c: RapidsConfigView): boolean {
  const g = d.semaphore.heldMs;
  if (g <= 0) return false;
  if (d.heldDuringFetchMinMs >= T.minComponentMs && d.heldDuringFetchMinMs >= g * T.heldOnShuffleShareOfHeld) {
    return true;
  }
  return (
    isKudoGpuRead(c) &&
    d.activity.shuffleReadMs / d.taskTimeMs >= T.shuffleFetchShare &&
    d.heldWhileWaitingMinMs >= g * T.heldOnShuffleShareOfHeld
  );
}

// Why a task holds the GPU semaphore while it waits for shuffle blocks (spark-rapids 26.x):
//  - kudo GPU read mode: KudoGpuShuffleCoalesceIterator.next buffers host batches up to
//    batchSizeBytes, then acquires the semaphore and concatenates on the GPU
//    (GpuShuffleCoalesceExec). The task keeps the semaphore until an operator releases it,
//    usually GpuPartitioning after producing each output batch, so an operator that pulls more
//    input before producing output (the next coalesced batch, a join's stream side after its
//    build side, an aggregate) reads it while holding the semaphore.
//  - GpuShuffledSizedHashJoinExec with host shuffle input: getJoinInfo reads both sides on the
//    host, then setupForJoin prefetches the build side (acquires the semaphore) and then the first
//    stream batch, reading up to batchSizeBytes of stream data with the semaphore held.
function heldOnShuffleReadAlert(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  const g = d.semaphore.heldMs;
  const fetch = d.shuffleFetchWaitMs;
  const proven = d.heldDuringFetchMinMs;
  const semShare = d.semaphore.waitMs / d.taskTimeMs;
  const harmless = semShare < T.harmlessSemaphoreShare;
  const conns = c.numConnectionsPerPeer ?? D.numConnectionsPerPeer;
  const holders = holdersText(d);

  const message = `Tasks held the GPU semaphore for ${ms(g)} (${share(g, d.taskTimeMs)} of task time) but ${gpuActivityText(d)}: they held it while waiting for shuffle data. ${
    proven >= T.minComponentMs
      ? `At least ${ms(proven)} (${share(proven, d.taskTimeMs)} of task time) of the held time the task thread was blocked on shuffle fetch (fetch wait ${share(fetch, d.taskTimeMs)})`
      : `At least ${ms(d.heldWhileWaitingMinMs)} of the held time went to reading shuffle input (shuffle read ${share(d.activity.shuffleReadMs, d.taskTimeMs)} of task time)`
  }. ${
    isKudoGpuRead(c)
      ? "With the kudo GPU read mode (the default) a task takes the semaphore to concatenate shuffle blocks on the GPU and keeps it while it reads more input, e.g. a shuffled join reading its stream side after its build side. "
      : ""
  }${
    harmless
      ? `Holding cost other tasks little here: semaphore wait was ${share(d.semaphore.waitMs, d.taskTimeMs)} of task time${holders ? ` (tasks that ${holders})` : ""}. The stage is bound by the shuffle fetch, not by the GPU`
      : `Other tasks waited ${share(d.semaphore.waitMs, d.taskTimeMs)} of task time for the semaphore meanwhile`
  }`;

  const suggestions: string[] = [];
  if (conns < 4) {
    suggestions.push(
      `Set spark.shuffle.io.numConnectionsPerPeer=4 (${currentValue(c.numConnectionsPerPeer, "1")}): NDS-H 1K q9 / q8 join stages went from 79% fetch wait to 2-5% and from 11.6s to 6.1s, q8 34.2s -> 28.3s`,
    );
  }
  if (c.isRapidsShuffleManager) {
    suggestions.push(
      `Raise spark.rapids.shuffle.multiThreaded.reader.threads (${currentValue(c.shuffleReaderThreads, String(D.shuffleThreads))}) if fetch wait stays high`,
    );
  }
  if (harmless) {
    suggestions.push(
      `Do not tune GPU concurrency or batch sizes for this alert: the held time is input wait. spark.rapids.shuffle.kudo.serializer.read.mode=CPU removes it from the held time (82-87% -> 3%) but did not make these stages faster, and q9 was 6% slower`,
    );
  } else {
    suggestions.push(
      isKudoGpuRead(c)
        ? "Tasks queue on the semaphore while these tasks wait for shuffle data. Try spark.rapids.shuffle.kudo.serializer.read.mode=CPU (concatenates on the host without the semaphore) and measure, or let more tasks share the GPU"
        : "Tasks queue on the semaphore while these tasks wait for shuffle data: let more tasks share the GPU (spark.rapids.sql.concurrentGpuTasks.dynamic=true, the default since 25.06)",
    );
  }
  suggestions.push("Also check partition skew and the number of reduce partitions of this stage");
  alerts.push(
    stageAlert(
      d,
      "gpuHeldOnShuffleRead",
      "GPU Held While Waiting For Shuffle Data",
      message,
      suggestions,
      conns < 4 ? "numConnectionsPerPeer=4" : harmless ? "speed up shuffle fetch" : "kudo read.mode=CPU",
    ),
  );
}

function gpuUnderutilizedRule(
  d: StageGpuDiagnosis,
  c: RapidsConfigView,
  alerts: Alerts,
  diag: GpuDiagnosisStore,
) {
  if (!d.hasSemaphoreMetrics) return;
  // explained by gpuHostBoundDecode, whose suggestions fit better
  if (isHostBoundDecode(d)) return;
  const g = d.semaphore.heldMs;
  const gpu = stageGpuActivityPct(d.resource);
  // with samples: SM activity decides; without: the stage was classified from the held time
  // that provably went to waiting for input
  const idle = gpu !== undefined ? gpu < T.gpuIdlePct : d.bottleneck === "gpu-underutilized";
  if (!idle) return;
  if (g < T.minComponentMs || g / d.taskTimeMs < T.gpuHeldShare) return;
  if (isHeldOnShuffleRead(d, c)) {
    heldOnShuffleReadAlert(d, c, alerts);
    return;
  }
  const rx = d.resource?.avgPcieRxMiBps ?? -1;
  const suggestions: string[] = [];
  const waiting = d.heldWhileWaitingMinMs;
  if (waiting >= T.minComponentMs) {
    suggestions.push(
      `At least ${ms(waiting)} of the held time was spent waiting for the next scan / shuffle batch: spark-rapids keeps the GPU semaphore between input batches, so speed up the input first (see the scan IO / shuffle read alerts of this stage)`,
    );
  }
  if (rx >= 0) {
    suggestions.push(
      `PCIe host->device traffic averaged ${Math.round(rx)} MiB/s: if high, the stage is copy bound - prune columns / filter earlier and make sure spark.rapids.memory.pinnedPool.size is set`,
    );
  }
  suggestions.push(
    `Use larger batches: spark.rapids.sql.batchSizeBytes (${currentValue(c.batchSizeBytes !== undefined ? humanFileSize(c.batchSizeBytes) : undefined, "1g")})`,
  );
  const manyHolders = (d.avgGpuHolders ?? 0) >= T.manyGpuHolders;
  if (!manyHolders) {
    suggestions.push(
      diag.dynamicConcurrency
        ? `Few tasks shared the GPU. ${DYNAMIC_CONCURRENCY_NOTE}; a large per-task memory estimate keeps concurrency low - more / smaller partitions lower it`
        : `More concurrent tasks overlap copies with kernels: spark.rapids.sql.concurrentGpuTasks (${currentValue(c.concurrentGpuTasks, String(D.concurrentGpuTasks))})`,
    );
  }
  suggestions.push("Profile with nsys to see whether kernels are short / synchronising");
  const holders = holdersText(d);
  alerts.push(
    stageAlert(
      d,
      "gpuUnderutilized",
      "GPU Held But SMs Mostly Idle",
      `Tasks held the GPU semaphore for ${ms(g)} in total (${share(g, d.taskTimeMs)} of task time, summed over tasks) but ${gpuActivityText(d)} (device wide).${
        holders ? ` Tasks that ${holders}${manyHolders ? " - the GPU was not short of tasks" : ""}.` : ""
      } Holding the semaphore is not GPU work: it covers waiting for input and host side work between kernels`,
      suggestions,
      "larger batches / fewer copies",
    ),
  );
}

function hostBoundDecodeRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  if (!isHostBoundDecode(d)) return;
  const cpu = d.resource?.avgCpuUtilPct ?? -1;
  const holders = holdersText(d);
  alerts.push(
    stageAlert(
      d,
      "gpuHostBoundDecode",
      "GPU Scan Decode Bound By Host CPU",
      `Parquet "GPU decode time" was ${ms(d.scanDecodeMs)} (${share(d.scanDecodeMs, d.taskTimeMs)} of task time, ${share(d.scanDecodeMs, d.scanOpMs)} of the scan's op time) while ${gpuActivityText(d)} and executor CPU was ${Math.round(cpu)}% busy${
        holders ? `; tasks that ${holders}` : ""
      }. The decode call holds the GPU semaphore but spends its time on host side work, not on GPU kernels`,
      [
        "Profile one executor during this stage (perf / async-profiler, with native frames): a CPU-saturated scan with an idle GPU on this machine was traced to a per-page getenv hotspot in libcudf",
        `The host cores are shared by ${currentValue(c.executorCores, "?")} task threads, the reader threads (spark.rapids.sql.multiThreadedRead.numThreads ${currentValue(c.multiThreadedReadThreads, String(D.multiThreadedReadThreads))}) and the shuffle threads: give each GPU more cores, or shrink pools that oversubscribe them`,
        "Larger scan batches amortise per-batch host cost: spark.sql.files.maxPartitionBytes / spark.rapids.sql.reader.batchSizeBytes - measure, the gain depends on where the host time goes",
        "More concurrency or a bigger GPU will not help this stage: the GPU is waiting on the host",
      ],
      "profile host side decode",
    ),
  );
}

function cpuBoundRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  if (d.bottleneck !== "cpu") return;
  const cpu = d.resource?.avgCpuUtilPct ?? -1;
  if (cpu < T.cpuBusyPct || d.hostWorkMs < T.minComponentMs) return;
  alerts.push(
    stageAlert(
      d,
      "gpuHostCpuBound",
      "GPU Stage Bound By Host CPU",
      `At least ${share(d.hostWorkMs, d.taskTimeMs)} of task time is host side work without the GPU (outside shuffle and scan IO) and executor CPU was ${Math.round(cpu)}% busy, ${gpuActivityText(d)}`,
      [
        "Look for operators that run on the CPU in this query (see the GPU CPU-fallback alert), row based UDFs, and row/columnar transitions",
        `Give each GPU more CPU cores (spark.executor.cores ${currentValue(c.executorCores, "?")})`,
        "Profile the executor (async-profiler / perf): a CPU-saturated scan with an idle GPU was traced to a per-page getenv hotspot in libcudf",
      ],
      "remove CPU work / add cores",
    ),
  );
}

function spillRule(
  d: StageGpuDiagnosis,
  c: RapidsConfigView,
  alerts: Alerts,
  diag: GpuDiagnosisStore,
) {
  const spill = d.spillToHostBytes + d.spillToDiskBytes;
  const retries = d.retryCount + d.splitRetryCount;
  if (spill < T.spillBytes && retries < T.retryCount) return;
  const what: string[] = [];
  if (spill > 0) what.push(`spilled ${humanFileSize(spill)} of GPU memory`);
  if (retries > 0) what.push(`hit ${retries} GPU OOM retries`);
  alerts.push(
    stageAlert(
      d,
      "gpuMemoryPressure",
      "GPU Memory Pressure (Spill / OOM Retry)",
      `This stage ${what.join(" and ")}`,
      [
        diag.dynamicConcurrency
          ? `Concurrency already follows measured task memory${
              d.maxConcurrentGpuTasks !== undefined ? ` (up to ${d.maxConcurrentGpuTasks} tasks per GPU here)` : ""
            }; cap it with spark.rapids.sql.maxConcurrentGpuTasks (internal, ${currentValue(c.maxConcurrentGpuTasks, "0 = no limit")}) or spark.rapids.sql.concurrentGpuTasks.dynamic=false with a lower spark.rapids.sql.concurrentGpuTasks`
          : `Lower spark.rapids.sql.concurrentGpuTasks (${currentValue(c.concurrentGpuTasks, String(D.concurrentGpuTasks))}) so fewer tasks share GPU memory`,
        "Use more, smaller partitions (spark.sql.adaptive.coalescePartitions.initialPartitionNum / spark.sql.shuffle.partitions), too few partitions caused spill in tuning runs",
        "Check partition skew - a few huge partitions are a common cause",
        `Make sure host memory for spill is available: spark.rapids.memory.pinnedPool.size / spark.rapids.memory.host.spillStorageSize`,
      ],
      "fewer concurrent GPU tasks / more partitions",
    ),
  );
}

function smallScanTasksRule(d: StageGpuDiagnosis, c: RapidsConfigView, alerts: Alerts) {
  const median = d.medianTaskInputBytes;
  if (median === undefined || median <= 0 || median >= T.smallScanTaskInputBytes) return;
  if (d.numTasks < 200) return;
  const current = c.maxPartitionBytes ?? 128 * 1024 * 1024;
  if (current >= T.recommendedMaxPartitionBytes) return;
  alerts.push(
    stageAlert(
      d,
      "gpuSmallScanTasks",
      "Too Many Small GPU Scan Tasks",
      `${d.numTasks} scan tasks with a median input of ${humanFileSize(median)}, each GPU task has a fixed cost (semaphore, H2D copy, kernel launches)`,
      [
        `Set spark.sql.files.maxPartitionBytes=512m (currently ${humanFileSize(current)}); 1g-2g are also fine, 128m was 1.69x slower on NDS3K`,
      ],
      "maxPartitionBytes=512m",
    ),
  );
}

// ------------------------------------- SQL rules --------------------------------------------

const NON_COMPUTE_NODES = new Set([
  "AdaptiveSparkPlan",
  "ShuffleQueryStage",
  "BroadcastQueryStage",
  "TableCacheQueryStage",
  "ResultQueryStage",
  "ReusedExchange",
  "ReusedSubquery",
  "Subquery",
  "SubqueryBroadcast",
  "InputAdapter",
  "CommandResult",
  "LocalTableScan",
  "CollectLimit",
  "AQEShuffleRead",
]);

function isCpuComputeNode(node: EnrichedSqlNode): boolean {
  const name = node.nodeName;
  if (isRapidsNode(name) || NON_COMPUTE_NODES.has(name)) return false;
  if (name.startsWith("WholeStageCodegen") || name.startsWith("Execute ")) return false;
  const rows = node.metrics.find((m) => m.name === "rows");
  const rowCount = rows !== undefined ? parseMetricNumber(rows.value) ?? 0 : 0;
  return rowCount > 0 || (node.duration ?? 0) >= T.minFallbackNodeDurationMs;
}

function cpuFallbackRule(sql: SparkSQLStore, alerts: Alerts) {
  for (const s of sql.sqls) {
    if (s.status !== "COMPLETED" || !s.nodes.some((n) => isRapidsNode(n.nodeName))) continue;
    const fallback = s.nodes.filter(isCpuComputeNode);
    if (fallback.length === 0) continue;
    const transitions = s.nodes.filter((n) => n.nodeName === "GpuRowToColumnar").length;
    const names = Array.from(new Set(fallback.map((n) => n.nodeName)));
    alerts.push({
      id: `gpuCpuFallback_${s.id}`,
      name: "gpuCpuFallback",
      title: "Operators Fell Back To CPU",
      location: `In: SQL query "${s.description}" (id: ${s.id})`,
      message: `${fallback.length} operator(s) ran on the CPU in a GPU query: ${names.slice(0, 6).join(", ")}${
        transitions > 0 ? `; ${transitions} row->columnar transition(s) copy data back to the GPU` : ""
      }`,
      suggestion: `\n${numbered([
        "Run with spark.rapids.sql.explain=NOT_ON_GPU and read the driver log for the reason of each fallback",
        "Unsupported expressions / UDFs are the usual cause; rewrite them with built-in functions, or enable the expression if it is only disabled as incompatible (spark.rapids.sql.incompatibleOps.enabled)",
        "Each fallback adds GPU->CPU->GPU transitions (columnar <-> row conversion) which are often more expensive than the operator itself",
      ])}\n`,
      shortSuggestion: "check spark.rapids.sql.explain=NOT_ON_GPU",
      type: "warning",
      source: { type: "sql", sqlId: s.id, sqlNodeId: fallback[0].nodeId },
    });
  }
}

// ---------------------------------- config / app rules --------------------------------------

function configRules(diag: GpuDiagnosisStore, sql: SparkSQLStore, alerts: Alerts) {
  const c = diag.config;
  const allNodes = sql.sqls.flatMap((s) => s.nodes);
  const hasGpuScan = allNodes.some((n) => isRapidsNode(n.nodeName) && n.nodeName.includes("Scan"));
  const hasGpuShuffle = allNodes.some((n) => n.nodeName === "GpuColumnarExchange");

  if (c.pinnedPoolBytes === undefined || c.pinnedPoolBytes === 0) {
    alerts.push(
      appAlert(
        "gpuPinnedPool",
        "No Pinned Host Memory Pool",
        c.offHeapLimitEnabled
          ? "spark.rapids.memory.host.offHeapLimit.enabled=true but spark.rapids.memory.pinnedPool.size is not set, so there is no pinned pool and host spill storage is ignored"
          : "spark.rapids.memory.pinnedPool.size is not set, host<->GPU copies go through pageable memory",
        [
          "Set spark.rapids.memory.pinnedPool.size, e.g. off-heap budget / 8 capped at 16g (NDS1K: 592s -> 319s just by adding pinnedPool=16g)",
        ],
        "pinnedPool.size=16g",
        c.offHeapLimitEnabled ? "error" : "warning",
      ),
    );
  }

  if (hasGpuShuffle && !c.isRapidsShuffleManager) {
    alerts.push(
      appAlert(
        "gpuShuffleManager",
        "RAPIDS Shuffle Manager Not Used",
        `spark.shuffle.manager is ${c.shuffleManager ?? "the Spark default"}, GPU shuffles go through the generic serializer`,
        [
          "Set spark.shuffle.manager=com.nvidia.spark.rapids.spark<spark version, e.g. 353>.RapidsShuffleManager (MULTITHREADED mode by default)",
        ],
        "use RapidsShuffleManager",
      ),
    );
  } else if (
    c.isRapidsShuffleManager &&
    c.shuffleMode === "MULTITHREADED" &&
    !c.skipMerge &&
    !c.externalShuffleService
  ) {
    alerts.push(
      appAlert(
        "gpuShuffleSkipMerge",
        "Multithreaded Shuffle Merges Map Output",
        "spark.rapids.shuffle.multithreaded.skipMerge is false: every map task merges and rewrites its shuffle files on local disk",
        [
          "Set spark.rapids.shuffle.multithreaded.skipMerge=true (requires no external shuffle service). NDS-H 1K: ~400s -> 281s, q5 95-122s -> 22s",
        ],
        "skipMerge=true",
      ),
    );
  }

  if (!c.shuffleCompress) {
    alerts.push(
      appAlert(
        "gpuShuffleCompress",
        "Shuffle Compression Disabled",
        "spark.shuffle.compress=false makes GPU shuffles write and fetch much more data",
        ["Remove spark.shuffle.compress=false (it was 23% slower in tuning runs)"],
        "spark.shuffle.compress=true",
      ),
    );
  }

  const maxPartition = c.maxPartitionBytes ?? D.maxPartitionBytes;
  if (hasGpuScan && maxPartition < T.recommendedMaxPartitionBytes) {
    alerts.push(
      appAlert(
        "gpuMaxPartitionBytes",
        "Scan Splits Too Small For GPU",
        `spark.sql.files.maxPartitionBytes is ${humanFileSize(maxPartition)}, GPU scans prefer large splits`,
        [
          "Set spark.sql.files.maxPartitionBytes between 512m and 2g (clamp(batchSizeBytes, 128m, 2g)); 128m was 1.69x slower than 512m on NDS3K",
        ],
        "maxPartitionBytes=512m",
      ),
    );
  }

  if (c.aqeEnabled && c.aqeParallelismFirst && hasGpuShuffle) {
    alerts.push(
      appAlert(
        "gpuAqeCoalesce",
        "AQE Coalesces Partitions For CPU Parallelism",
        "spark.sql.adaptive.coalescePartitions.parallelismFirst=true keeps many small shuffle partitions, which is CPU-style tuning",
        [
          "Set spark.sql.adaptive.coalescePartitions.parallelismFirst=false so AQE coalesces to the advisory partition size",
          `Keep spark.sql.adaptive.coalescePartitions.initialPartitionNum moderate (${currentValue(c.aqeInitialPartitionNum, "spark.sql.shuffle.partitions")}), 200 worked well; 800 was 1.16-1.29x slower`,
        ],
        "parallelismFirst=false",
      ),
    );
  }

  if (c.batchSizeBytes !== undefined && c.batchSizeBytes < T.minBatchSizeBytes) {
    alerts.push(
      appAlert(
        "gpuBatchSize",
        "Small GPU Batch Size",
        `spark.rapids.sql.batchSizeBytes is ${humanFileSize(c.batchSizeBytes)}, small batches leave the GPU under-used`,
        ["Use spark.rapids.sql.batchSizeBytes=1g (256m was 1.77x slower on NDS3K)"],
        "batchSizeBytes=1g",
      ),
    );
  }

  const total = diag.totalTaskTimeMs;
  const sem = diag.totalSemaphore.waitMs;
  const concurrency = c.concurrentGpuTasks ?? D.concurrentGpuTasks;
  const semaphoreHeavy = total > 0 && sem / total >= T.appSemaphoreShare;
  const gpuBusy = (diag.avgSmActivityPct ?? diag.avgGpuUtilPct ?? 0) >= T.gpuBusyPct;
  // with dynamic concurrency the configured value is not a cap (the semaphore stage alerts cover it)
  if (!diag.dynamicConcurrency && semaphoreHeavy && !gpuBusy && concurrency <= T.lowConcurrentGpuTasks) {
    alerts.push(
      appAlert(
        "gpuConcurrency",
        "Low GPU Task Concurrency",
        `GPU stages spent ${share(sem, total)} of their task time waiting for the GPU semaphore with spark.rapids.sql.concurrentGpuTasks=${concurrency}`,
        [
          "Raise spark.rapids.sql.concurrentGpuTasks to 4 or more on large-memory GPUs (concurrency 1 was 1.11-1.15x slower)",
          c.executorCores !== undefined && c.executorCores / concurrency >= T.coresPerGpuTaskRatio
            ? `Or lower spark.executor.cores (${c.executorCores}) towards the concurrency`
            : "Watch the GPU memory pressure alerts after raising it",
        ],
        "concurrentGpuTasks=4",
      ),
    );
  }

  if (diag.avgSmActivityPct !== undefined && diag.avgSmActivityPct < T.appLowSmActivityPct && total > 0) {
    // the two breakdowns overlap, so they are reported separately
    const a = diag.totalActivity;
    const parts = [
      ["GPU operators", a.operatorsMs],
      ["shuffle write", a.shuffleWriteMs],
      ["shuffle read", a.shuffleReadMs],
      ["scan IO wait", a.scanIoWaitMs],
      ["not covered by op time", a.untrackedMs],
    ] as [string, number][];
    parts.sort((x, y) => y[1] - x[1]);
    const s = diag.totalSemaphore;
    alerts.push(
      appAlert(
        "gpuLowUtilization",
        "GPU Mostly Idle",
        `Average SM activity while GPU stages ran was ${Math.round(diag.avgSmActivityPct)}%. Task time went to: ${parts
          .slice(0, 3)
          .map(([n, v]) => `${n} ${share(v, total)}`)
          .join(", ")}. GPU semaphore: waiting ${share(s.waitMs, total)}, held ${share(s.heldMs, total)}, not held ${share(s.notHeldMs, total)}`,
        [
          "Open the GPU tab to see the per stage breakdown and fix the largest components first",
          "Shuffle and scan IO usually dominate on fast GPUs, the shuffle / scan alerts list the matching settings",
        ],
        "see GPU tab",
      ),
    );
  }
}

export function reduceRapidsGpuAlerts(
  diag: GpuDiagnosisStore | undefined,
  sql: SparkSQLStore,
  alerts: Alerts,
) {
  if (diag === undefined) return;
  const stageRules = [
    semaphoreRule,
    shuffleWriteRule,
    shuffleReadRule,
    scanIoRule,
    gpuUnderutilizedRule,
    hostBoundDecodeRule,
    cpuBoundRule,
    spillRule,
    smallScanTasksRule,
  ];
  // stages are sorted by task time, so the cap keeps the most expensive stages per rule
  const perRule = new Map<string, number>();
  for (const d of diag.stages) {
    if (d.taskTimeMs < T.minStageTaskTimeMs) continue;
    for (const rule of stageRules) {
      const produced: Alerts = [];
      rule(d, diag.config, produced, diag);
      for (const a of produced) {
        const n = perRule.get(a.name) ?? 0;
        if (n < T.maxStageAlertsPerRule) alerts.push(a);
        perRule.set(a.name, n + 1);
      }
    }
  }
  cpuFallbackRule(sql, alerts);
  configRules(diag, sql, alerts);
}
