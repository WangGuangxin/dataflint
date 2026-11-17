// Mirrors org.apache.spark.dataflint.resource.ResourceMetricsSummary (served at /resourcemetrics/json).
// Every numeric field uses -1 for "not available".

export interface ExecutorResourceInfo {
  executorId: string;
  host: string;
  numCores: number;
  gpuAddresses: string[];
  gpuSource: string;
  firstSampleMs: number;
  lastSampleMs: number;
  numSamples: number;
}

export interface ResourceTimelinePoint {
  timeMs: number;
  executorId: string;
  activeTasks: number;
  processCpuCores: number;
  cpuUtilPct: number;
  hostCpuPct: number;
  diskReadMBps: number;
  diskWriteMBps: number;
  gpuUtilPct: number;
  smActivityPct: number;
  smOccupancyPct: number;
  dramActivityPct: number;
  gpuMemUsedMiB: number;
  pcieRxMiBps: number;
  pcieTxMiBps: number;
}

export interface StageResourceUsage {
  stageId: number;
  stageAttemptId: number;
  sampledSeconds: number;
  numExecutors: number;
  avgActiveTasks: number;
  avgProcessCpuCores: number;
  avgCpuUtilPct: number;
  avgHostCpuPct: number;
  avgGcPct: number;
  avgDiskReadMBps: number;
  avgDiskWriteMBps: number;
  avgReadCharsMBps: number;
  avgGpuUtilPct: number;
  maxGpuUtilPct: number;
  avgSmActivityPct: number;
  avgSmOccupancyPct: number;
  avgDramActivityPct: number;
  avgGpuMemUsedMiB: number;
  maxGpuMemUsedMiB: number;
  avgPcieRxMiBps: number;
  avgPcieTxMiBps: number;
}

export interface ResourceMetricsSummary {
  enabled: boolean;
  gpuSource: string;
  executors: ExecutorResourceInfo[];
  stages: StageResourceUsage[];
  timeline: ResourceTimelinePoint[];
}
