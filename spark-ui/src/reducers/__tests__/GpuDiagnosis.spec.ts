import {
  Alerts,
  ConfigStore,
  EnrichedSparkSQL,
  EnrichedSqlNode,
  SparkSQLStore,
  SparkStageStore,
} from "../../interfaces/AppStore";
import { StageResourceUsage } from "../../interfaces/ResourceMetrics";
import {
  metricTotalText,
  parseDurationMs,
  parseMetricNumber,
  parseSizeBytes,
} from "../../utils/MetricValueParser";
import { reduceRapidsGpuAlerts } from "../Alerts/RapidsGpuAlerts";
import {
  calculateGpuDiagnosis,
  extractRapidsConfig,
  parseConfigBytes,
} from "../GpuDiagnosisReducer";
import { calculateStageAccumulators } from "../MetricsReducer";
import { calcRapidsNodeMetrics } from "../SqlReducerUtils";

describe("MetricValueParser", () => {
  it("parses SQL metric durations", () => {
    expect(parseMetricNumber("total (min, med, max (stageId: taskId))\n6.0 s (201 ms, 249 ms, 309 ms (stage 8.0: task 21))")).toBe(6000);
    expect(parseMetricNumber("37 ms")).toBe(37);
    expect(parseMetricNumber("total (min, med, max (stageId: taskId))\n4.0 m (50 ms, 1.6 s, 1.7 s (stage 16.0: task 832))")).toBe(240000);
    expect(parseDurationMs("1.5 h")).toBe(5400000);
  });

  it("parses sizes and counts", () => {
    expect(parseSizeBytes("14.7 MiB")).toBeCloseTo(14.7 * 1024 * 1024);
    expect(parseMetricNumber("158.0 B")).toBe(158);
    expect(parseMetricNumber("730,806,711")).toBe(730806711);
  });

  it("parses RAPIDS accumulator formats", () => {
    expect(parseMetricNumber("00:04:41.581")).toBe(281581);
    expect(parseMetricNumber("49.72GB (53389098783 bytes)")).toBe(53389098783);
    expect(parseMetricNumber("32")).toBe(32);
  });

  it("extracts the total from statistics metrics", () => {
    expect(metricTotalText("total (min, med, max)\n10 ms (1 ms, 2 ms, 3 ms)")).toBe("10 ms");
  });
});

describe("RAPIDS config parsing", () => {
  it("parses Spark byte sizes", () => {
    expect(parseConfigBytes("512m")).toBe(512 * 1024 * 1024);
    expect(parseConfigBytes("1g")).toBe(1024 ** 3);
    expect(parseConfigBytes("1073741824")).toBe(1024 ** 3);
    expect(parseConfigBytes("16gb")).toBe(16 * 1024 ** 3);
  });

  it("reads defaults", () => {
    const c = extractRapidsConfig({
      "spark.shuffle.manager": "com.nvidia.spark.rapids.spark353.RapidsShuffleManager",
    });
    expect(c.isRapidsShuffleManager).toBe(true);
    expect(c.shuffleMode).toBe("MULTITHREADED");
    expect(c.skipMerge).toBe(false);
    expect(c.kudoSerializer).toBe(true);
    expect(c.kudoReadMode).toBe("GPU");
    expect(c.aqeParallelismFirst).toBe(true);
    expect(c.pinnedPoolBytes).toBeUndefined();
  });
});

// ------------------------------------------------------------------------------------------
// fixtures
// ------------------------------------------------------------------------------------------

const TUNED_CONF: Record<string, string> = {
  "spark.plugins": "com.nvidia.spark.SQLPlugin",
  "spark.executor.cores": "32",
  "spark.rapids.sql.concurrentGpuTasks": "4",
  "spark.rapids.sql.batchSizeBytes": "1g",
  "spark.sql.files.maxPartitionBytes": "512m",
  "spark.rapids.memory.pinnedPool.size": "16g",
  "spark.shuffle.manager": "com.nvidia.spark.rapids.spark353.RapidsShuffleManager",
  "spark.rapids.shuffle.multithreaded.skipMerge": "true",
  "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
  "spark.shuffle.io.numConnectionsPerPeer": "4",
};

function config(props: Record<string, string>): ConfigStore {
  return { sparkProperties: props } as unknown as ConfigStore;
}

function stage(
  stageId: number,
  runMs: number,
  extra: Partial<NonNullable<SparkStageStore["breakdown"]>>,
  accumulators: { name: string; value: string }[],
): SparkStageStore {
  return {
    stageId,
    attemptId: 0,
    status: "COMPLETE",
    numTasks: 200,
    stageRealTimeDurationMs: 10000,
    breakdown: {
      executorRunTimeMs: runMs,
      executorCpuTimeMs: 0,
      jvmGcTimeMs: 0,
      shuffleFetchWaitMs: 0,
      shuffleWriteTimeMs: 0,
      memoryBytesSpilled: 0,
      diskBytesSpilled: 0,
      inputBytes: 0,
      medianTaskInputBytes: undefined,
      submissionTimeEpoc: undefined,
      completionTimeEpoc: undefined,
      ...extra,
    },
    accumulators: calculateStageAccumulators({ accumulatorUpdates: accumulators } as any),
  } as unknown as SparkStageStore;
}

function gpuNode(nodeId: number, nodeName: string, stageId: number, metrics: { name: string; value: string }[]): EnrichedSqlNode {
  return {
    nodeId,
    nodeName,
    enrichedName: nodeName,
    metrics: [],
    type: "transformation",
    stage: { type: "onestage", stageId, status: "COMPLETE", stageDuration: 1, restOfStageDuration: undefined },
    rapidsMetrics: calcRapidsNodeMetrics(nodeName, metrics),
  } as unknown as EnrichedSqlNode;
}

function sqlStore(nodes: EnrichedSqlNode[]): SparkSQLStore {
  return {
    sqls: [{ id: "3", description: "q3", status: "COMPLETED", nodes } as unknown as EnrichedSparkSQL],
  };
}

function resource(stageId: number, sm: number, cpu: number): StageResourceUsage {
  return {
    stageId, stageAttemptId: 0, sampledSeconds: 100, numExecutors: 2, avgActiveTasks: 32,
    avgProcessCpuCores: 10, avgCpuUtilPct: cpu, avgHostCpuPct: 20, avgGcPct: 1,
    avgDiskReadMBps: 500, avgDiskWriteMBps: 900, avgReadCharsMBps: 600,
    avgGpuUtilPct: sm + 20, maxGpuUtilPct: 100, avgSmActivityPct: sm, avgSmOccupancyPct: 10,
    avgDramActivityPct: 10, avgGpuMemUsedMiB: 1000, maxGpuMemUsedMiB: 2000,
    avgPcieRxMiBps: 8000, avgPcieTxMiBps: 100,
  };
}

const sec = (s: number) => {
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const r = s % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(r).padStart(2, "0")}.000`;
};

describe("GPU diagnosis", () => {
  it("returns undefined for CPU applications", () => {
    expect(calculateGpuDiagnosis(sqlStore([]), [], config({}), undefined)).toBeUndefined();
  });

  it("classifies a shuffle write bound stage like NDS-H stage 439", () => {
    // 940s task time, 604s shuffle write, 281s gpuTime, 28s semaphore, 173s fetch wait
    const s = stage(439, 940_907, { shuffleWriteTimeMs: 604_805, shuffleFetchWaitMs: 173_243 }, [
      { name: "gpuSemaphoreWait", value: "00:00:28.535" },
      { name: "gpuTime", value: "00:04:41.581" },
    ]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(TUNED_CONF), undefined)!;
    const d = diag.stages[0];
    expect(d.isGpuStage).toBe(true);
    expect(d.bottleneck).toBe("shuffle-write");
    // each axis adds up to the task time on its own
    const sum = (o: object) => Object.values(o).reduce((a: number, b: number) => a + b, 0);
    expect(sum(d.semaphore)).toBeCloseTo(d.taskTimeMs);
    expect(sum(d.activity)).toBeCloseTo(d.taskTimeMs);
    expect(d.semaphore.notHeldMs).toBeCloseTo(940_907 - 28_535 - 281_581);
    // no op time: shuffle read falls back to the fetch wait, the rest is not covered
    expect(d.hasOpTime).toBe(false);
    expect(d.activity.shuffleReadMs).toBe(173_243);
    expect(d.activity.untrackedMs).toBeCloseTo(940_907 - 604_805 - 173_243);
    // 281.6s held, but only 940.9 - 28.5 - 604.8 - 173.2 = 134.4s is left outside semaphore
    // wait, shuffle write and fetch wait: >= 147.2s of the held time waited for shuffle input,
    // which is more than half of it -> the GPU was not the limit
    expect(d.heldWhileWaitingMinMs).toBeCloseTo(281_581 - (940_907 - 28_535 - 604_805 - 173_243));
    expect(d.secondary).toEqual(["gpu-underutilized", "shuffle-read"]);

    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    expect(alerts.map((a) => a.name)).toContain("gpuShuffleWrite");
    // tuned config: no config alerts
    expect(alerts.filter((a) => a.source.type === "status")).toHaveLength(1);
  });

  it("semaphore wait with an idle GPU suggests more concurrency", () => {
    const conf = { ...TUNED_CONF, "spark.rapids.sql.concurrentGpuTasks": "2", "spark.executor.cores": "64" };
    const s = stage(5, 400_000, {}, [
      { name: "gpuSemaphoreWait", value: sec(200) },
      { name: "gpuTime", value: sec(150) },
    ]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(conf), {
      enabled: true, gpuSource: "nvidia-smi-dmon+gpm", executors: [], timeline: [],
      stages: [resource(5, 20, 30)],
    })!;
    expect(diag.stages[0].bottleneck).toBe("semaphore");
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    const sem = alerts.find((a) => a.name === "gpuSemaphoreWait")!;
    expect(sem.title).toBe("High GPU Semaphore Wait");
    expect(sem.suggestion).toContain("concurrentGpuTasks");
    expect(sem.suggestion).toContain("spark.executor.cores=64");
    expect(alerts.map((a) => a.name)).toContain("gpuConcurrency");
    expect(alerts.map((a) => a.name)).toContain("gpuLowUtilization");
  });

  it("semaphore wait with a busy GPU is reported as GPU saturation", () => {
    const s = stage(6, 400_000, {}, [
      { name: "gpuSemaphoreWait", value: sec(200) },
      { name: "gpuTime", value: sec(180) },
    ]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(6, 85, 30)],
    })!;
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    expect(alerts.find((a) => a.name === "gpuSemaphoreWait")!.title).toContain("GPU Saturated");
  });

  it("GPU held with idle SMs is gpu-underutilized", () => {
    const s = stage(7, 300_000, {}, [{ name: "gpuTime", value: sec(250) }]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(7, 10, 20)],
    })!;
    expect(diag.stages[0].bottleneck).toBe("gpu-underutilized");
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    expect(alerts.find((a) => a.name === "gpuUnderutilized")!.suggestion).toContain("PCIe");
  });

  it("attributes scan host wait from node metrics", () => {
    const scan = gpuNode(1, "GpuScan parquet", 9, [
      { name: "scan time", value: "total (min, med, max (stageId: taskId))\n5.0 m (1 s, 2 s, 3 s (stage 9.0: task 1))" },
      { name: "GPU decode time", value: "total (min, med, max (stageId: taskId))\n1.0 m (1 s, 2 s, 3 s (stage 9.0: task 1))" },
      { name: "buffer time", value: "total (min, med, max (stageId: taskId))\n4.0 m (1 s, 2 s, 3 s (stage 9.0: task 1))" },
      { name: "op time", value: "total (min, med, max (stageId: taskId))\n5.0 m (1 s, 2 s, 3 s (stage 9.0: task 1))" },
    ]);
    const s = stage(9, 500_000, {}, [{ name: "gpuTime", value: sec(100) }]);
    const diag = calculateGpuDiagnosis(sqlStore([scan]), [s], config(TUNED_CONF), undefined)!;
    const d = diag.stages[0];
    expect(d.sqlId).toBe("3");
    expect(d.activity.scanIoWaitMs).toBe(240_000);
    expect(d.activity.operatorsMs).toBe(60_000);
    expect(d.scanDecodeMs).toBe(60_000);
    expect(d.bottleneck).toBe("scan-io");
    expect(d.topOperators[0].nodeName).toBe("GpuScan parquet");
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([scan]), alerts);
    const scanAlert = alerts.find((a) => a.name === "gpuScanIO")!;
    expect(scanAlert.source).toEqual({ type: "stage", sqlId: "3", stageId: 9 });
  });

  const total = (v: string, stageId: number) =>
    `total (min, med, max (stageId: taskId))\n${v} (1 ms, 2 ms, 3 ms (stage ${stageId}.0: task 1))`;

  it("does not report semaphore wait inside a scan as scan IO", () => {
    // app-20261109095012-0002: scan op time 504s with 1.4s buffer time and 3.8s decode, the
    // rest is the scan acquiring the GPU semaphore
    const scan = gpuNode(1, "GpuScan parquet", 20, [
      { name: "scan time", value: total("8.4 m", 20) },
      { name: "op time", value: total("8.4 m", 20) },
      { name: "buffer time", value: total("1.4 s", 20) },
      { name: "GPU decode time", value: total("3.8 s", 20) },
    ]);
    const s = stage(20, 600_000, {}, [
      { name: "gpuSemaphoreWait", value: sec(480) },
      { name: "gpuTime", value: sec(60) },
    ]);
    const d = calculateGpuDiagnosis(sqlStore([scan]), [s], config(TUNED_CONF), undefined)!.stages[0];
    expect(d.activity.scanIoWaitMs).toBe(1_400);
    expect(d.activity.operatorsMs).toBe(504_000 - 1_400);
    expect(d.bottleneck).toBe("semaphore");
    expect(d.secondary).not.toContain("scan-io");
  });

  it("adds exchange and AQE reader shuffle read op times", () => {
    const exchange = gpuNode(1, "GpuColumnarExchange", 21, [
      { name: "op time (shuffle read)", value: total("2.0 m", 21) },
    ]);
    const reader = gpuNode(2, "GpuCustomShuffleReader", 21, [
      { name: "op time (shuffle read)", value: total("1.0 m", 21) },
    ]);
    const s = stage(21, 400_000, { shuffleFetchWaitMs: 50_000 }, [{ name: "gpuTime", value: sec(100) }]);
    const d = calculateGpuDiagnosis(sqlStore([exchange, reader]), [s], config(TUNED_CONF), undefined)!.stages[0];
    expect(d.activity.shuffleReadMs).toBe(180_000);
    expect(d.opTimeCoverage).toBeCloseTo(180_000 / 400_000);
  });

  it("detects GPU held while waiting for shuffle input", () => {
    // run 500s: 400s held, 300s reading shuffle -> at least 200s held while reading
    const reader = gpuNode(1, "GpuCustomShuffleReader", 22, [
      { name: "op time (shuffle read)", value: total("5.0 m", 22) },
    ]);
    const s = stage(22, 500_000, {}, [{ name: "gpuTime", value: sec(400) }]);
    const nodes = sqlStore([reader]);
    const diag = calculateGpuDiagnosis(nodes, [s], config(TUNED_CONF), undefined)!;
    const d = diag.stages[0];
    expect(d.heldWhileWaitingMinMs).toBe(200_000);
    // no utilization samples: half of the held time provably waited for input
    expect(d.bottleneck).toBe("gpu-underutilized");
    expect(d.reason).toContain("held the GPU while waiting");
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, nodes, alerts);
    // kudo GPU read mode (default) and a stage that mostly reads shuffle data: explained as
    // holding the semaphore while waiting for shuffle data
    expect(alerts.map((a) => a.name)).toContain("gpuHeldOnShuffleRead");
    expect(alerts.map((a) => a.name)).not.toContain("gpuUnderutilized");
    expect(alerts.map((a) => a.name)).toContain("gpuShuffleRead");

    // kudo CPU read mode: no shuffle specific explanation, the generic advice stays
    const cpuRead = calculateGpuDiagnosis(
      nodes,
      [s],
      config({ ...TUNED_CONF, "spark.rapids.shuffle.kudo.serializer.read.mode": "CPU" }),
      undefined,
    )!;
    const cpuAlerts: Alerts = [];
    reduceRapidsGpuAlerts(cpuRead, nodes, cpuAlerts);
    expect(cpuAlerts.find((a) => a.name === "gpuUnderutilized")!.suggestion).toContain(
      "keeps the GPU semaphore between input batches",
    );
  });

  it("explains held time blocked on shuffle fetch like NDS-H 1K q9 stage 21", () => {
    // app-20261116144055-0000 stage 21: run 702.0s, fetch wait 542.4s, held 602.2s, semaphore
    // wait 25.4s, 200 tasks over 12.1s on 2 executors, SM 2%. Held time that does not fit next to
    // the fetch wait: 602.2 - (702.0 - 542.4 - 25.4) = 468.1s. Same stage with kudo
    // read.mode=CPU: held 3%, same stage time; with numConnectionsPerPeer=4: 11.6s -> 6.1s.
    const conf = { ...TUNED_CONF };
    delete conf["spark.shuffle.io.numConnectionsPerPeer"];
    const s = {
      ...stage(21, 702_008, { shuffleFetchWaitMs: 542_438 }, [
        { name: "gpuTime", value: "00:10:02.218" },
        { name: "gpuSemaphoreWait", value: "00:00:25.428" },
        { name: "gpuMaxConcurrentGpuTasks", value: "32" },
      ]),
      stageRealTimeDurationMs: 12_111,
    } as SparkStageStore;
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(conf), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(21, 2, 20)],
    })!;
    const d = diag.stages[0];
    expect(d.heldDuringFetchMinMs).toBeCloseTo(602_218 - (702_008 - 542_438 - 25_428));
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    const a = alerts.find((x) => x.name === "gpuHeldOnShuffleRead")!;
    expect(a.title).toBe("GPU Held While Waiting For Shuffle Data");
    expect(a.message).toContain("blocked on shuffle fetch (fetch wait 77%)");
    expect(a.message).toContain("kudo GPU read mode");
    expect(a.message).toContain("Holding cost other tasks little here: semaphore wait was 4%");
    expect(a.suggestion).toContain("numConnectionsPerPeer=4");
    expect(a.suggestion).toContain("Do not tune GPU concurrency");
    expect(a.shortSuggestion).toBe("numConnectionsPerPeer=4");
    expect(alerts.map((x) => x.name)).not.toContain("gpuUnderutilized");

    // the same stage with tasks queueing on the semaphore: holding while fetching hurts
    const busy = {
      ...stage(21, 702_008, { shuffleFetchWaitMs: 400_000 }, [
        { name: "gpuTime", value: sec(450) },
        { name: "gpuSemaphoreWait", value: sec(150) },
      ]),
      stageRealTimeDurationMs: 12_111,
    } as SparkStageStore;
    const busyDiag = calculateGpuDiagnosis(sqlStore([]), [busy], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(21, 2, 20)],
    })!;
    const busyAlerts: Alerts = [];
    reduceRapidsGpuAlerts(busyDiag, sqlStore([]), busyAlerts);
    const b = busyAlerts.find((x) => x.name === "gpuHeldOnShuffleRead")!;
    expect(b.message).toContain("Other tasks waited 21% of task time for the semaphore");
    expect(b.suggestion).toContain("read.mode=CPU");
    expect(b.shortSuggestion).toBe("kudo read.mode=CPU");
  });

  it("explains held time copying already fetched shuffle data like NDS-H 1K q9 stage 448", () => {
    // app-20261117030702-0000 (numConnectionsPerPeer=4) stage 448: run 357s, held 70%, semaphore
    // wait 4%, fetch wait 2%, shuffle read op time 63%, SM 4%, PCIe rx 16 GiB/s
    const reader = gpuNode(1, "GpuShuffleCoalesce", 448, [
      { name: "op time (shuffle read)", value: total("3.8 m", 448) },
    ]);
    const s = stage(448, 357_000, { shuffleFetchWaitMs: 6_000 }, [
      { name: "gpuTime", value: sec(250) },
      { name: "gpuSemaphoreWait", value: sec(14) },
    ]);
    const nodes = sqlStore([reader]);
    const diag = calculateGpuDiagnosis(nodes, [s], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(448, 4, 74)],
    })!;
    expect(diag.stages[0].heldDuringFetchMinMs).toBe(0);
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, nodes, alerts);
    const a = alerts.find((x) => x.name === "gpuHeldOnShuffleRead")!;
    expect(a.title).toBe("GPU Held While Copying Shuffle Data");
    expect(a.message).toContain("fetch wait was only 2%");
    expect(a.message).toContain("host to device traffic 8000 MiB/s");
    expect(a.message).toContain("bound by the shuffle read copies");
    // the fetch is already fast: no connection / reader thread advice
    expect(a.suggestion).not.toContain("numConnectionsPerPeer");
    expect(a.suggestion).toContain("shuffle fewer bytes");
    expect(a.shortSuggestion).toBe("shuffle fewer bytes");
  });

  it("scales op time attributed from operators shared by two stages", () => {
    const exchange = gpuNode(1, "GpuColumnarExchange", 23, [
      { name: "op time (shuffle read)", value: total("10.0 m", 23) },
    ]);
    const s = stage(23, 300_000, {}, [{ name: "gpuTime", value: sec(100) }]);
    const d = calculateGpuDiagnosis(sqlStore([exchange]), [s], config(TUNED_CONF), undefined)!.stages[0];
    expect(d.opTimeCoverage).toBeCloseTo(2);
    expect(d.activityMeasured.shuffleReadMs).toBe(600_000);
    expect(d.activity.shuffleReadMs).toBeCloseTo(300_000);
    expect(d.activity.untrackedMs).toBe(0);
  });

  it("semaphore wait with dynamic concurrency does not suggest raising concurrentGpuTasks", () => {
    const conf = { ...TUNED_CONF, "spark.rapids.sql.concurrentGpuTasks": "2", "spark.executor.cores": "64" };
    const s = stage(5, 400_000, {}, [
      { name: "gpuSemaphoreWait", value: sec(200) },
      { name: "gpuTime", value: sec(150) },
      // spark-rapids 25.10+ reports it; proves memory based permits
      { name: "gpuMaxConcurrentGpuTasks", value: "6" },
    ]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(conf), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(5, 20, 30)],
    })!;
    expect(diag.dynamicConcurrency).toBe(true);
    expect(diag.maxConcurrentGpuTasksSeen).toBe(6);
    // 150s held / (10s stage x 2 executors)
    expect(diag.stages[0].avgGpuHolders).toBeCloseTo(7.5);
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    const sem = alerts.find((a) => a.name === "gpuSemaphoreWait")!;
    expect(sem.message).toContain("at most 6");
    expect(sem.suggestion).toContain("concurrentGpuTasks.dynamic=true");
    expect(sem.suggestion).not.toContain("Increase spark.rapids.sql.concurrentGpuTasks");
    expect(sem.suggestion).not.toContain("spark.executor.cores=64");
    expect(sem.shortSuggestion).toBe("less GPU memory per task");
    expect(alerts.map((a) => a.name)).not.toContain("gpuConcurrency");

    // explicitly static: the old advice applies
    const staticDiag = calculateGpuDiagnosis(
      sqlStore([]),
      [s],
      config({ ...conf, "spark.rapids.sql.concurrentGpuTasks.dynamic": "false" }),
      { enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(5, 20, 30)] },
    )!;
    expect(staticDiag.dynamicConcurrency).toBe(false);
    const staticAlerts: Alerts = [];
    reduceRapidsGpuAlerts(staticDiag, sqlStore([]), staticAlerts);
    expect(staticAlerts.find((a) => a.name === "gpuSemaphoreWait")!.suggestion).toContain(
      "Increase spark.rapids.sql.concurrentGpuTasks",
    );
    expect(staticAlerts.map((a) => a.name)).toContain("gpuConcurrency");
  });

  it("reports a decode dominated scan with idle SMs and a busy CPU as host bound", () => {
    // app-20261116091111-0000 (NDS-H 1K) q9 stage 434: run 792s, held 318.6s, SM 18%, CPU 87%,
    // scan op time 5.2m of which GPU decode time 4.7m, buffer time 19.8s, 2 executors x 13.4s
    const scan = gpuNode(1, "GpuScan parquet", 434, [
      { name: "scan time", value: total("5.2 m", 434) },
      { name: "op time", value: total("5.2 m", 434) },
      { name: "GPU decode time", value: total("4.7 m", 434) },
      { name: "buffer time", value: total("19.8 s", 434) },
    ]);
    const s = {
      ...stage(434, 791_960, {}, [
        { name: "gpuSemaphoreWait", value: "00:00:11.422" },
        { name: "gpuTime", value: "00:05:18.550" },
        { name: "gpuMaxConcurrentGpuTasks", value: "32" },
      ]),
      stageRealTimeDurationMs: 13_355,
    } as SparkStageStore;
    const diag = calculateGpuDiagnosis(sqlStore([scan]), [s], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(434, 18, 87)],
    })!;
    const d = diag.stages[0];
    expect(d.scanOpMs).toBe(312_000);
    expect(d.avgGpuHolders).toBeCloseTo(318_550 / (13_355 * 2));
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([scan]), alerts);
    const decode = alerts.find((a) => a.name === "gpuHostBoundDecode")!;
    expect(decode.message).toContain("SM activity 18%");
    expect(decode.message).toContain("executor CPU was 87% busy");
    expect(decode.message).toContain("about 11.9 tasks per GPU on average, at most 32");
    // the generic held-but-idle advice (PCIe, concurrency) does not fit this stage
    expect(alerts.map((a) => a.name)).not.toContain("gpuUnderutilized");
  });

  it("does not suggest more concurrency when many tasks already held the GPU", () => {
    const s = {
      ...stage(7, 300_000, {}, [
        { name: "gpuTime", value: sec(250) },
        { name: "gpuMaxConcurrentGpuTasks", value: "32" },
      ]),
      stageRealTimeDurationMs: 12_500,
    } as SparkStageStore;
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(TUNED_CONF), {
      enabled: true, gpuSource: "gpm", executors: [], timeline: [], stages: [resource(7, 10, 20)],
    })!;
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    const a = alerts.find((x) => x.name === "gpuUnderutilized")!;
    expect(a.message).toContain("summed over tasks");
    expect(a.message).toContain("about 10.0 tasks per GPU on average, at most 32");
    expect(a.message).toContain("the GPU was not short of tasks");
    expect(a.suggestion).not.toContain("concurrentGpuTasks");
  });

  it("flags memory pressure", () => {
    const s = stage(10, 200_000, {}, [
      { name: "gpuTime", value: sec(100) },
      { name: "gpuSpillToHostBytes", value: "48.87GB (52473000000 bytes)" },
      { name: "gpuSpillToHostTime", value: sec(4) },
    ]);
    const diag = calculateGpuDiagnosis(sqlStore([]), [s], config(TUNED_CONF), undefined)!;
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore([]), alerts);
    expect(alerts.map((a) => a.name)).toContain("gpuMemoryPressure");
  });
});

describe("RAPIDS config rules", () => {
  it("flags the known bad defaults", () => {
    const props = {
      "spark.plugins": "com.nvidia.spark.SQLPlugin",
      "spark.rapids.memory.host.offHeapLimit.enabled": "true",
      "spark.shuffle.manager": "com.nvidia.spark.rapids.spark353.RapidsShuffleManager",
      "spark.rapids.sql.batchSizeBytes": "256m",
    };
    const nodes = [
      gpuNode(1, "GpuScan parquet", 1, []),
      gpuNode(2, "GpuColumnarExchange", 1, []),
    ];
    const diag = calculateGpuDiagnosis(sqlStore(nodes), [], config(props), undefined)!;
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore(nodes), alerts);
    const names = alerts.map((a) => a.name);
    expect(names).toEqual(
      expect.arrayContaining([
        "gpuPinnedPool",
        "gpuShuffleSkipMerge",
        "gpuMaxPartitionBytes",
        "gpuAqeCoalesce",
        "gpuBatchSize",
      ]),
    );
    expect(alerts.find((a) => a.name === "gpuPinnedPool")!.type).toBe("error");
  });

  it("detects CPU fallback operators in GPU queries", () => {
    const nodes = [
      gpuNode(1, "GpuScan parquet", 1, []),
      { ...gpuNode(2, "BatchEvalPython", 1, []), metrics: [{ name: "rows", value: "1,000" }] } as EnrichedSqlNode,
      gpuNode(3, "GpuRowToColumnar", 1, []),
      { ...gpuNode(4, "AdaptiveSparkPlan", 1, []), metrics: [{ name: "rows", value: "5" }] } as EnrichedSqlNode,
    ];
    const diag = calculateGpuDiagnosis(sqlStore(nodes), [], config(TUNED_CONF), undefined)!;
    const alerts: Alerts = [];
    reduceRapidsGpuAlerts(diag, sqlStore(nodes), alerts);
    const fallback = alerts.find((a) => a.name === "gpuCpuFallback")!;
    expect(fallback.message).toContain("BatchEvalPython");
    expect(fallback.message).not.toContain("AdaptiveSparkPlan");
    expect(fallback.message).toContain("row->columnar");
  });
});
