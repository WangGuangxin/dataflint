import { StageResourceUsage } from "./ResourceMetrics";

/**
 * Where a GPU (RAPIDS) stage spends its task time.
 *  - gpu-compute:       tasks hold the GPU semaphore and the SMs are busy -> the GPU is the limit
 *  - gpu-underutilized: tasks hold the GPU semaphore but SMs are mostly idle (H2D/D2H copies,
 *                       syncs, tiny batches, or waiting for the next input batch while holding it)
 *  - semaphore:         tasks wait to get onto the GPU (spark.rapids.sql.concurrentGpuTasks)
 *  - shuffle-write:     serialize / compress / write shuffle files (after partitioning on the GPU)
 *  - shuffle-read:      fetching + deserializing shuffle blocks ("op time (shuffle read)")
 *  - scan-io:           task blocked until the host has read / decompressed input ("buffer time")
 *  - cpu:               host work while not holding the GPU, outside shuffle / scan IO
 *  - spill:             memory pressure dominated (spill / OOM retry)
 */
export type GpuBottleneck =
  | "gpu-compute"
  | "gpu-underutilized"
  | "semaphore"
  | "shuffle-write"
  | "shuffle-read"
  | "scan-io"
  | "cpu"
  | "spill";

export const GPU_BOTTLENECK_LABELS: Record<GpuBottleneck, string> = {
  "gpu-compute": "GPU compute",
  "gpu-underutilized": "GPU held, SMs idle",
  semaphore: "GPU semaphore wait",
  "shuffle-write": "Shuffle write",
  "shuffle-read": "Shuffle read",
  "scan-io": "Scan IO / host decode",
  cpu: "CPU / other host work",
  spill: "Spill / memory pressure",
};

// Task time (executorRunTime, summed over the tasks of a stage) is split along two independent
// axes. Each axis is a partition of the task time on its own; the two axes overlap each other
// (e.g. a task can hold the GPU semaphore while it waits for the next shuffle batch), so their
// components must never be added together.

/**
 * Axis 1 - GPU semaphore state, from the spark-rapids task accumulators. Exact: waiting and
 * holding are disjoint states of the task thread, the rest of the run time is "not held".
 * "Held" is wall time between acquire and release, not GPU kernel time: spark-rapids only
 * releases the semaphore before shuffle write / columnar-to-row / at task end, so a task keeps
 * it while it waits for its next scan or shuffle batch.
 */
export interface GpuSemaphoreBreakdown {
  waitMs: number; // gpuSemaphoreWait
  heldMs: number; // gpuTime
  notHeldMs: number; // executorRunTime - wait - held
}

export type GpuSemaphoreComponent = keyof GpuSemaphoreBreakdown;

/**
 * Axis 2 - what the task thread was doing, from the RAPIDS operator metrics. "op time" is the
 * exclusive wall time of an operator (its iterator time minus its children's op time), so the
 * components below are disjoint regions of the task thread.
 */
export interface GpuActivityBreakdown {
  // exclusive op time of the GPU operators, including the scan minus its IO wait and the shuffle
  // write partitioning; semaphore wait happens inside these (wherever the GPU is acquired)
  operatorsMs: number;
  // "op time (shuffle read)" of exchanges / AQE shuffle readers: fetch wait + deserialization
  shuffleReadMs: number;
  // scan "buffer time": the task blocked until host threads read / decompressed its input
  scanIoWaitMs: number;
  // Spark shuffle write time; the RAPIDS writer excludes the time spent pulling its input
  shuffleWriteMs: number;
  // run time no op time covers: work done when an operator builds its iterator (e.g. the build
  // side of a shuffled hash join), CPU operators, task setup
  untrackedMs: number;
}

export type GpuActivityComponent = keyof GpuActivityBreakdown;

export interface StageOperatorTime {
  nodeId: number;
  nodeName: string;
  enrichedName: string;
  metricName: string;
  timeMs: number;
}

export interface StageGpuDiagnosis {
  stageId: number;
  attemptId: number;
  sqlId: string | undefined;
  sqlDescription: string | undefined;
  status: string;
  isGpuStage: boolean;
  numTasks: number;
  durationMs: number | undefined;
  taskTimeMs: number;
  cpuTimeMs: number;
  gcTimeMs: number;
  // gpuTime / gpuSemaphoreWait accumulators exist (spark-rapids 25.x+)
  hasSemaphoreMetrics: boolean;
  semaphore: GpuSemaphoreBreakdown;
  // false when the stage has no RAPIDS op time metrics (old spark-rapids or
  // spark.rapids.sql.exec.opTimeTrackingRDD.enabled=false)
  hasOpTime: boolean;
  // components as measured. They can exceed the task time when an operator is shared by two
  // stages (a reused exchange): SQL metrics have one total per operator, attributed to one stage
  activityMeasured: GpuActivityBreakdown;
  // the same, scaled down when needed so that it adds up to taskTimeMs
  activity: GpuActivityBreakdown;
  // share of the task time covered by op time + shuffle write (undefined without op time)
  opTimeCoverage: number | undefined;
  // op time of the stage's scans (scan time on versions without op time) and its "GPU decode time"
  scanOpMs: number;
  scanDecodeMs: number;
  shuffleFetchWaitMs: number;
  // gpuMaxConcurrentGpuTasks (spark-rapids 25.10+): most tasks that held the semaphore of one GPU
  // at the same time
  maxConcurrentGpuTasks: number | undefined;
  // average number of tasks holding the semaphore of one GPU while the stage ran:
  // held time / (stage wall time x executors that ran it). Needs the resource samples.
  avgGpuHolders: number | undefined;
  // lower bound of the time tasks held the GPU while waiting for scan / shuffle input:
  // held time that does not fit into the time left after semaphore wait, shuffle and scan IO
  heldWhileWaitingMinMs: number;
  // lower bound of the time tasks held the GPU while their thread was blocked on shuffle fetch:
  // held - (run - fetch wait - semaphore wait). Spark's fetch wait is time the task thread spends
  // blocked in ShuffleBlockFetcherIterator.next, disjoint from semaphore wait and from GPU work
  heldDuringFetchMinMs: number;
  // lower bound of host work done without the GPU, outside shuffle and scan IO
  hostWorkMs: number;
  spillToHostBytes: number;
  spillToDiskBytes: number;
  sparkSpillBytes: number;
  retryCount: number;
  splitRetryCount: number;
  retryBlockTimeMs: number;
  medianTaskInputBytes: number | undefined;
  resource: StageResourceUsage | undefined;
  topOperators: StageOperatorTime[];
  bottleneck: GpuBottleneck;
  secondary: GpuBottleneck[];
  reason: string;
}

export interface RapidsConfigView {
  concurrentGpuTasks: number | undefined;
  // spark.rapids.sql.concurrentGpuTasks.dynamic (spark-rapids 25.06+, default true)
  concurrentGpuTasksDynamic: boolean;
  // spark.rapids.sql.maxConcurrentGpuTasks (internal, 0 / unset = no limit)
  maxConcurrentGpuTasks: number | undefined;
  executorCores: number | undefined;
  taskGpuAmount: number | undefined;
  batchSizeBytes: number | undefined;
  maxPartitionBytes: number | undefined;
  pinnedPoolBytes: number | undefined;
  offHeapLimitEnabled: boolean;
  shuffleManager: string | undefined;
  isRapidsShuffleManager: boolean;
  shuffleMode: string;
  skipMerge: boolean;
  // spark.rapids.shuffle.kudo.serializer.enabled (internal, default true)
  kudoSerializer: boolean;
  // spark.rapids.shuffle.kudo.serializer.read.mode (internal, default GPU): GPU concatenates the
  // shuffle blocks on the GPU and takes the GPU semaphore to do it
  kudoReadMode: string;
  shuffleReaderThreads: number | undefined;
  shuffleWriterThreads: number | undefined;
  multiThreadedReadThreads: number | undefined;
  parquetReaderType: string | undefined;
  shufflePartitions: number | undefined;
  aqeEnabled: boolean;
  aqeParallelismFirst: boolean;
  aqeInitialPartitionNum: number | undefined;
  numConnectionsPerPeer: number | undefined;
  shuffleCompress: boolean;
  externalShuffleService: boolean;
  metricsLevel: string | undefined;
}

export interface GpuDiagnosisStore {
  isRapidsApp: boolean;
  hasResourceSamples: boolean;
  gpuSource: string;
  config: RapidsConfigView;
  // GPU concurrency is sized from measured task memory, so spark.rapids.sql.concurrentGpuTasks is
  // only the starting point of each stage: dynamic mode is on and the app reports
  // gpuMaxConcurrentGpuTasks (proof of a spark-rapids version with memory based permits)
  dynamicConcurrency: boolean;
  // highest gpuMaxConcurrentGpuTasks over the GPU stages
  maxConcurrentGpuTasksSeen: number | undefined;
  totalTaskTimeMs: number;
  totalSemaphore: GpuSemaphoreBreakdown;
  totalActivity: GpuActivityBreakdown;
  // task time of the GPU stages grouped by their primary bottleneck
  bottleneckTaskTimeMs: Record<GpuBottleneck, number>;
  avgSmActivityPct: number | undefined;
  avgGpuUtilPct: number | undefined;
  avgCpuUtilPct: number | undefined;
  stages: StageGpuDiagnosis[];
}
