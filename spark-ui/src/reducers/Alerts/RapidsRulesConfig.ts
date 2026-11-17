// Thresholds for the RAPIDS (GPU) bottleneck classification and alerts, kept in one place so
// they can be tuned without touching the rule logic.
//
// "share" values are fractions of a stage's total task time (executorRunTime summed over tasks).
// Numbers in the comments come from tuning runs on a 2x RTX PRO 6000 (96GB) / 352-core ARM host
// with spark-rapids 26.08-26.10, NDS / NDS-H / SSB at SF1K-3K.

export const RAPIDS_THRESHOLDS = {
  // ---- stage selection ----
  // stages below this task time are ignored for stage alerts (noise)
  minStageTaskTimeMs: 60_000,
  // a stage alert also needs its component to be at least this long in absolute time
  minComponentMs: 30_000,
  // at most this many stage alerts per rule (the stages with the largest task time win)
  maxStageAlertsPerRule: 10,

  // ---- bottleneck classification ----
  // a component becomes the primary bottleneck when it is the largest one and above this share
  primaryMinShare: 0.25,
  // other components above this share are listed as secondary bottlenecks
  secondaryMinShare: 0.15,
  // SM activity (GPM) or utilization.gpu above this => the GPU is genuinely busy
  gpuBusyPct: 60,
  // below this, a stage that holds the GPU most of its time is "held but idle"
  gpuIdlePct: 25,
  // process CPU above this % of executor cores => host CPU is saturated
  cpuBusyPct: 85,

  // ---- semaphore ----
  // NDS3K OOB vs tuned: 4,243s vs 2,562s of semaphore wait; concurrency 4 vs 32 cost +7.6%
  semaphoreShare: 0.25,
  // executor cores / concurrentGpuTasks above this ratio is a "shape" problem (96 cores worse
  // than 32 cores; 32 cores with concurrency 32 was the best)
  coresPerGpuTaskRatio: 4,

  // ---- shuffle ----
  // share of task time reading shuffle data (max of fetch wait and RAPIDS "op time (shuffle
  // read)"). SSB reduce stages spent 83% in fetch wait; numConnectionsPerPeer=4: 140.7s -> 110.4s.
  // NDS-H 1K q9 / q8 join stages: fetch wait 77-80% -> 2-5%, stage time 11.6s -> 6.1s
  shuffleFetchShare: 0.3,
  // without skipMerge every map task merges and rewrites its shuffle files (NDS-H 400s -> 281s)
  shuffleWriteShare: 0.3,

  // ---- scan ----
  scanIoShare: 0.25,
  // 128m splits were 1.69x slower than 512m on NDS3K; median scan task input 11.6MB was a 5.7x
  // slowdown case
  smallScanTaskInputBytes: 64 * 1024 * 1024,
  recommendedMaxPartitionBytes: 512 * 1024 * 1024,

  // ---- GPU held but idle ----
  gpuHeldShare: 0.4,
  // on average this many tasks per GPU held the semaphore => the GPU was not short of tasks
  manyGpuHolders: 4,
  // held time explained by the shuffle read when at least this share of it provably went to
  // waiting for input. NDS-H 1K q9 / q8 join stages: held 82-87% of task time, fetch wait 77-80%,
  // at least 64-70% held while blocked on fetch; kudo read.mode=CPU: held 3%, same stage time
  heldOnShuffleShareOfHeld: 0.5,
  // below this semaphore wait share, holding the GPU while waiting did not delay other tasks
  // (those q9 / q8 stages: 3-4% with dynamic concurrency admitting up to 32 tasks per GPU)
  harmlessSemaphoreShare: 0.1,

  // ---- host bound GPU decode ----
  // scan "GPU decode time" share of task time, and of the scan's op time. NDS-H 1K q9: decode
  // was 95-97% of scan op time with SM 9-18% and executor CPU 87-100%
  hostBoundDecodeShare: 0.25,
  decodeShareOfScan: 0.5,

  // ---- memory pressure ----
  spillBytes: 1024 * 1024 * 1024,
  retryCount: 10,

  // ---- config rules ----
  // batchSizeBytes=256m was 1.77x slower than 1g on NDS3K
  minBatchSizeBytes: 512 * 1024 * 1024,
  // concurrentGpuTasks=1 cost 1.11x-1.15x; <=2 is flagged when semaphore wait is visible
  lowConcurrentGpuTasks: 2,
  // app-level semaphore share that makes the concurrency config rule fire
  appSemaphoreShare: 0.1,
  // app-level SM activity below this (while tasks run) => GPU under-used overall
  appLowSmActivityPct: 30,

  // ---- CPU fallback ----
  minFallbackNodeDurationMs: 5_000,
};

// Default values of spark-rapids settings when they are not set explicitly.
export const RAPIDS_DEFAULTS = {
  // unset: max(1, min(4, GPU memory / (4 x batchSizeBytes))) (GpuSemaphore.computeDefaultMemory),
  // 4 on any GPU with >= 16GB and the default 1g batches. With concurrentGpuTasks.dynamic=true
  // (the default since 25.06) it is only the starting estimate of each stage.
  concurrentGpuTasks: 4,
  batchSizeBytes: 1024 * 1024 * 1024,
  maxPartitionBytes: 128 * 1024 * 1024,
  multiThreadedReadThreads: 20,
  shuffleThreads: 20,
  numConnectionsPerPeer: 1,
};
