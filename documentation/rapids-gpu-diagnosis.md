# RAPIDS (cudf / spark-rapids) GPU profiling & diagnosis

DataFlint can explain where the task time of a GPU (RAPIDS Accelerator) application goes and
which settings to change. It has three parts:

1. **Collection**: an executor plugin samples CPU, IO and GPU utilization every second.
2. **Bottleneck analysis**: the task time of every GPU stage is split into components and the
   stage gets a primary bottleneck.
3. **Rules**: alerts with concrete setting changes, shown in the Alerts tab and the new **GPU** tab.

## 1. Collection (executor resource monitor)

The DataFlint plugin must also be loaded on the executors (it already is when it is listed in `spark.plugins`):

```
spark.plugins=com.nvidia.spark.SQLPlugin,io.dataflint.spark.SparkDataflintPlugin
```

| Setting | Default | Meaning |
|---|---|---|
| `spark.dataflint.resourceMonitor.enabled` | auto | `auto`: on when `com.nvidia.spark.SQLPlugin` is in `spark.plugins` |
| `spark.dataflint.resourceMonitor.intervalMs` | 1000 | sampling period (GPU minimum is 1s) |
| `spark.dataflint.resourceMonitor.reportIntervalMs` | 5000 | batch interval executor -> driver / event log |
| `spark.dataflint.resourceMonitor.gpu.enabled` | true | needs `nvidia-smi` on the executor host |
| `spark.dataflint.resourceMonitor.gpu.gpm` | true | ask for GPM metrics (Hopper or newer) |
| `spark.dataflint.resourceMonitor.gpu.nvidiaSmiPath` | PATH lookup | explicit `nvidia-smi` |
| `spark.dataflint.resourceMonitor.gpu.devices` | executor `gpu` resource | GPU indices to sample when the executor has no GPU resource (local mode) |

Each executor samples the following every interval:

* **Process CPU**: in cores and as a % of `spark.executor.cores`. Also host CPU % and GC time.
* **Process IO**: `/proc/self/io` disk read/write bytes, plus rchar/wchar, which include page cache and sockets.
* **GPU**: one long-lived `nvidia-smi dmon -s pum --gpm-metrics 2,3,10,20,21` per executor, limited to the executor's GPU:
  * `utilization.gpu`, memory bandwidth %, framebuffer used and power.
  * GPM metrics: **SM Activity**, **SM Occupancy**, **DRAM Activity**, and **PCIe TX/RX**.
  * Without GPM the GPM fields are reported as missing (-1).
  * `utilization.gpu` only says that *a* kernel was running, so it over-states SM use. Measured here: 74% utilization.gpu vs 50% SM activity. The rules use SM activity when it is available.
  * GPM values arrive about one sample (~1s) after the matching power / `utilization.gpu` values. That only matters for very short stages.
  * Under RAPIDS, "GPU memory used" reflects the pre-allocated RMM pool, not live usage.
* **Stage tags**: the stages whose tasks were running at that moment. `ExecutorPlugin.onTaskStart` / `onTaskSucceeded` / `onTaskFailed` tracks them.

How samples are aggregated and stored:

* **Attribution**: a sample is attributed to each running stage, weighted by that stage's share of the running tasks. That gives per-stage averages. Operators map to stages through the SQL plan.
* **Transport**: batches go to the driver through the plugin RPC channel. The driver posts them as `DataflintResourceSamplesEvent`, so they are written to the **event log** and replayed by the History Server.
* **When batches are sent**: every report interval, and also immediately whenever the set of running stages changes or the executor goes idle. If the application stops in the middle of its last stage, the final ≤ `reportIntervalMs` of samples can be lost. A last flush is attempted at executor shutdown, but in local mode Spark has already stopped its RPC layer by then.
* **Endpoint**: aggregated data is served at `/dataflint/resourcemetrics/json` (per-stage usage plus a down-sampled per-executor timeline).

Applications run without the monitor are still diagnosed from task metrics. The utilization columns then show `n/a`.

## 2. Bottleneck analysis

Code: `spark-ui/src/reducers/GpuDiagnosisReducer.ts`. The measurement details below come from the
spark-rapids 26.10 source.

### Why there are two breakdowns

The RAPIDS metrics measure two different things, and they overlap:

* `gpuTime` is the wall time between acquiring and releasing the GPU semaphore. It is **not**
  kernel time (`GpuSemaphore.scala`, `releaseSemaphore`).
* spark-rapids releases the semaphore in only a few places: after shuffle partitioning (before
  the shuffle write), at columnar-to-row / bring-back-to-host, and at task end. A task therefore
  usually keeps the semaphore while it waits for its next scan or shuffle batch.
  (`GpuShuffleCoalesceExec` buffers the next host batch and only then calls
  `acquireIfNecessary`, which does nothing while the task holds the semaphore.)
* A scan's `scan time` includes acquiring the semaphore. So `scan time - GPU decode time` is
  **not** IO time. In one NDS run it was 500s of semaphore wait, with 1.4s of buffer time.

Adding these metrics into one stacked bar double counts. The analysis therefore splits the task
time (`executorRunTime`) twice:

**Axis 1: GPU semaphore state.** Exact, because waiting and holding are disjoint states of the
task thread:

| Component | Source |
|---|---|
| semaphore wait | stage accumulator `gpuSemaphoreWait` |
| holding GPU | stage accumulator `gpuTime` |
| not holding GPU | the rest of `executorRunTime` |

**Axis 2: what the task thread did.** The components are disjoint because `op time` is exclusive
per operator: `GpuOpTimeTrackingRDD` subtracts the children's op time.

| Component | Source |
|---|---|
| GPU operators | `op time` of non-scan operators + `op time (shuffle write partitioning)` + scan `op time` minus its IO wait |
| shuffle read | `op time (shuffle read)` of exchanges **plus** AQE `GpuCustomShuffleReader`. They measure different reads, because the reader builds its own `ShuffledBatchRDD`. This includes the fetch wait. Without op time, falls back to `shuffleFetchWaitTime` |
| scan IO wait | scan `buffer time`: the task blocked until the host reader threads had read and decompressed the input. `filter time` / `I/O schedule time` complete it, but exist only at DEBUG level |
| shuffle write | stage `shuffleWriteTime`. The RAPIDS threaded writer subtracts the time spent pulling its input |
| not covered by op time | the rest. Mostly work an operator does while it **builds** its iterator, which no op time wraps. Example: `GpuShuffledSizedHashJoinExec` reads its whole build side inside `zipPartitions`. Also CPU operators and task setup |

Checked against three event logs (Σ op time + shuffle write vs `executorRunTime`):

| Run | Coverage | Notes |
|---|---|---|
| NDS-H | 96% | per-stage median 1.00 |
| NDS | 97% | |
| NDS with many symmetric / sized hash joins | 72% | one stage at 8%, almost all of it semaphore wait during the eager join build |

Wait + held never exceeded the run time, except for one stage (1.1×).

**Limitation:** SQL metrics have one total per operator. An operator that runs in two stages,
such as a reused exchange read twice, is attributed to one of them. If that pushes a stage's
axis 2 above its task time, the axis is scaled down. The stage table tooltip shows the raw
coverage.

### Primary bottleneck

The largest of these disjoint candidates:

* semaphore wait
* GPU held
* shuffle write
* shuffle read
* scan IO wait
* host work: not holding the GPU and outside shuffle / scan IO. This is a lower bound.
* spill

"GPU held" becomes:

* `gpu-compute` when the sampled SM activity is ≥ 25%.
* `gpu-underutilized` when SM activity is below 25%.

Without samples, "GPU held" is also `gpu-underutilized` when at least half of the held time
provably went to waiting for input. That is the case when held time is larger than what is left
after semaphore wait, shuffle and scan IO (`heldWhileWaitingMinMs`).

Other notes:

* **Stage attribution:** node metrics are attributed to stages through the stage embedded in
  each metric value, `(stage 8.0: task 21)`.
* **Version note:** with spark-rapids 26.x, the default `spark.rapids.sql.metrics.level=MODERATE`
  has no node-level semaphore, spill or retry metrics. They come from the stage-level `gpu*` task
  accumulators.

## 3. Rules

All thresholds are in `spark-ui/src/reducers/Alerts/RapidsRulesConfig.ts`. The rules are in
`spark-ui/src/reducers/Alerts/RapidsGpuAlerts.ts`. Each rule can be disabled with
`spark.dataflint.alert.disabled=<name>`.

Stage rules apply to stages with ≥ 60s of task time. Each rule reports at most 10 stages, the most expensive first.

### GPU concurrency is dynamic in spark-rapids 25.06+

The concurrency rules depend on which semaphore the app ran with (`GpuSemaphore.scala`,
`PrioritySemaphore.scala`, `RapidsConfResourceEntries.scala` in spark-rapids 26.10):

- The GPU has one permit per 32 MiB of its memory. A task takes `estimate / 32 MiB` permits.
- With `spark.rapids.sql.concurrentGpuTasks.dynamic=true` (the default), the estimate is the 80th
  percentile of the peak GPU memory of the stage's tasks (`GpuStageMemoryEstimator`).
  `spark.rapids.sql.concurrentGpuTasks` only sets the starting estimate (GPU memory /
  concurrentGpuTasks). When it is unset it is `max(1, min(4, GPU memory / (4 × batchSizeBytes)))`,
  which is 4 on any GPU with ≥ 16 GB and 1g batches.
- `spark.rapids.sql.maxConcurrentGpuTasks` (internal, default 0 = no limit) is the only hard cap
  on the number of tasks.

On NDS-H 1K, with `concurrentGpuTasks=4`, `gpuMaxConcurrentGpuTasks` reached 32 per GPU
(= executor cores), so the configured value said nothing about the real concurrency.

The UI treats an app as dynamic when `concurrentGpuTasks.dynamic` is not `false` and its stages
report `gpuMaxConcurrentGpuTasks` (spark-rapids 25.10+). Each stage shows:

- `gpuMaxConcurrentGpuTasks`: the most tasks holding one GPU's semaphore at once.
- the average number of holders per GPU: held time / (stage wall time × executors that ran
  the stage). It needs the resource samples for the executor count.

Holding the semaphore is not GPU work. `gpuTime` is summed over tasks, and SM activity is
device wide. "Held 40% of task time, SM 18%" means that about 12 tasks per GPU held the
semaphore at the same time, while the SMs were mostly idle.

| Name | Fires when | Suggests |
|---|---|---|
| `gpuSemaphoreWait` | semaphore ≥ 25% of task time | GPU busy (≥ 60% activity): GPU bound, so reduce GPU work or add GPUs. Dynamic: the admitted tasks' memory estimate filled the GPU, so lower memory per task (more / smaller partitions, smaller batches) or pin a static concurrency (`dynamic=false`). Static: raise `concurrentGpuTasks`, or lower `spark.executor.cores` when cores ≥ 4× concurrency. Holds back when the stage also spills |
| `gpuShuffleWrite` | shuffle write ≥ 30% | RapidsShuffleManager, `multithreaded.skipMerge=true`, more writer threads, keep `spark.shuffle.compress` on, disk write MB/s |
| `gpuShuffleRead` | shuffle read ≥ 30% | fetch-wait dominated: `spark.shuffle.io.numConnectionsPerPeer=4`. Deserialization dominated: more reader threads, check CPU |
| `gpuScanIO` | scan IO wait (buffer time) ≥ 25% | `multiThreadedRead.numThreads`, avoid the PERFILE reader, then disk vs CPU (decompression) bound |
| `gpuUnderutilized` | GPU held ≥ 40% and SM activity < 25% (without samples: classified `gpu-underutilized`) | speed up the input first when the GPU is held while waiting for it; PCIe traffic / pinned pool, larger `batchSizeBytes`, nsys. More concurrency only when < 4 tasks per GPU held the semaphore on average. The message gives the average and max number of holders. Skipped when `gpuHostBoundDecode` fires |
| `gpuHeldOnShuffleRead` | the `gpuUnderutilized` condition, and either (a) held time that provably overlapped shuffle fetch, `held − (run − fetch wait − semaphore wait)`, is ≥ 50% of the held time, or (b) kudo GPU read mode, shuffle read ≥ 30% of task time and ≥ 50% of the held time provably waited for input | the held time is input wait, not GPU work. Fix the fetch: `spark.shuffle.io.numConnectionsPerPeer=4`, reader threads. When semaphore wait < 10% the message says the holding cost nothing and advises against tuning concurrency / batches; otherwise it suggests trying kudo `read.mode=CPU`. Replaces `gpuUnderutilized` for the stage |
| `gpuHostBoundDecode` | scan `GPU decode time` ≥ 25% of task time and ≥ 50% of the scan's op time, SM activity < 25%, executor CPU ≥ 85% | the decode call holds the GPU but waits on host work: profile native frames (a libcudf per-page getenv hotspot on this machine), check cores vs task / reader / shuffle threads, larger scan batches. Not more concurrency |
| `gpuHostCpuBound` | host work (not holding the GPU, outside shuffle / scan IO) is primary and executor CPU ≥ 85% | CPU fallback / UDFs, more cores per GPU, profiling |
| `gpuMemoryPressure` | spill ≥ 1 GiB or ≥ 10 OOM retries | lower concurrency (dynamic: cap with `maxConcurrentGpuTasks` or `dynamic=false`), more partitions, skew, host spill memory |
| `gpuSmallScanTasks` | ≥ 200 scan tasks with median input < 64 MiB | `maxPartitionBytes` 512m–2g |
| `gpuCpuFallback` | non-Gpu compute operators with rows in a GPU query | `spark.rapids.sql.explain=NOT_ON_GPU`, rewrite expressions / UDFs |
| `gpuPinnedPool` | `pinnedPool.size` unset/0 (error with `offHeapLimit.enabled`) | `pinnedPool.size`, e.g. 16g |
| `gpuShuffleManager` / `gpuShuffleSkipMerge` | GPU shuffles without RapidsShuffleManager / MULTITHREADED shuffle without skipMerge | RapidsShuffleManager / `skipMerge=true` |
| `gpuShuffleCompress` | `spark.shuffle.compress=false` | turn it back on |
| `gpuMaxPartitionBytes` | GPU scans with `maxPartitionBytes` < 512m | 512m–2g |
| `gpuAqeCoalesce` | AQE `parallelismFirst=true` with GPU shuffles | `parallelismFirst=false`, moderate `initialPartitionNum` |
| `gpuBatchSize` | `batchSizeBytes` < 512m | 1g |
| `gpuConcurrency` | static concurrency only: app semaphore ≥ 10% with `concurrentGpuTasks` ≤ 2 on a non-busy GPU | 4 or more |
| `gpuLowUtilization` | sampled app-level SM activity < 30% | top activity components plus the semaphore split |

The numbers quoted in the suggestions come from tuning runs on a 2× RTX PRO 6000 (96 GB) / 352-core ARM host. The runs used spark-rapids 26.08–26.10 on NDS / NDS-H / SSB at SF1K–3K. Re-check them for other hardware.

## 4. Validation: GPU held but SMs idle in shuffle read stages

Why `GPU Held But SMs Mostly Idle` fired on most shuffle read stages of NDS-H 1K
(`app-20261116091111-0000`), checked against the spark-rapids 26.10 source and an A/B run
(`ab-runs/dataflint-heldidle-exp-20261116`: q9 + q8, 2 iterations per variant, two rounds).

**Mechanism (code).** Shuffle input is read on the task thread
(`RapidsShuffleThreadedReaderBase.next` → `ShuffleBlockFetcherIterator.next`), so Spark's fetch
wait is task thread time, disjoint from semaphore wait. With the kudo serializer in GPU read mode
(`spark.rapids.shuffle.kudo.serializer.read.mode=GPU`, the default),
`KudoGpuShuffleCoalesceIterator.next` buffers host batches up to `batchSizeBytes`, then acquires
the GPU semaphore and concatenates them on the GPU (`GpuShuffleCoalesceExec`). The semaphore is
released only when an operator gives it up, usually `GpuPartitioning` after each output batch. In
`GpuShuffledSizedHashJoinExec` with host shuffle input, `getJoinInfo` reads both sides on the host,
then `setupForJoin` prefetches the build side, which acquires the semaphore, and then the first
stream batch, reading up to 1 GiB of stream data while the semaphore is held.

**Measurement (q9 stages 21/82, q8 stages 151/213).**

| variant | held | fetch wait | semaphore wait | stage time |
|---|---|---|---|---|
| base | 82–87% | 77–80% | 4% | 11.4–12.1s (q8: 8.6–8.9s) |
| kudo `read.mode=CPU` | **2–3%** | 82–85% | 0% | unchanged |
| `numConnectionsPerPeer=4` | 66–77% | **2–5%** | 4% | **6.1–6.3s (q8: 4.6–4.8s)** |

- Held time that provably overlapped fetch wait was ≥ 64–70% of task time.
- The q8 stages 142/204 have no provable fetch overlap, only op-time evidence. They also dropped
  from held 39–47% to 4–5% with CPU read mode.
- End to end, q8 went from 34.2s to 28.3s with `numConnectionsPerPeer=4`.
- With CPU read mode, q9 was about 6% slower (115s vs 109s).

**Stacks.** A separate base run sampled executor stacks with jstack once a second, giving 527
dumps. Of the task thread samples blocked in `RapidsShuffleBlockFetcherIterator.next`:
- **83.5%** were inside the stream side `setupForJoin`, called from `GpuShuffledSizedHashJoinExec.scala:755`
  after the build side `setupForJoin` at line 746 had taken the semaphore;
- 16.4% were in the probe loop (lines 713/723), which runs before the semaphore is taken.

**Conclusion.**
- The held but idle time in these stages is tasks holding the semaphore while they wait for shuffle
  blocks.
- With dynamic concurrency, semaphore wait was 3–6% and up to 32 tasks per GPU held it at once, so
  the holding did not slow other tasks.
- The stage is bound by the shuffle fetch, and that is where the fix belongs.
- Under a static, low concurrency the same holding would turn into semaphore wait for other tasks.
