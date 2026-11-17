import {
  Alert,
  Box,
  Chip,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import { ApexOptions } from "apexcharts";
import { duration } from "moment";
import React, { FC, useMemo } from "react";
import ReactApexChart from "react-apexcharts";
import { useNavigate } from "react-router-dom";
import { useAppSelector } from "../Hooks";
import {
  GPU_BOTTLENECK_LABELS,
  GpuActivityComponent,
  GpuBottleneck,
  GpuDiagnosisStore,
  GpuSemaphoreComponent,
  StageGpuDiagnosis,
} from "../interfaces/GpuDiagnosis";
import { ResourceTimelinePoint } from "../interfaces/ResourceMetrics";
import { humanFileSize, humanizeTimeDiff } from "../utils/FormatUtils";

// Categorical slots of the validated dark palette plus a neutral for the remainder. Each bar has
// its own legend; the stacking order of each bar passed the adjacent-pair CVD check.
interface Segment<K extends string> {
  key: K;
  label: string;
  color: string;
  help: string;
}

// Axis 1: exact split by GPU semaphore state
const SEMAPHORE_SEGMENTS: Segment<GpuSemaphoreComponent>[] = [
  {
    key: "heldMs",
    label: "Holding GPU",
    color: "#3987e5",
    help: "gpuTime: wall time between semaphore acquire and release. Includes waiting for the next scan / shuffle batch, spark-rapids keeps the semaphore between input batches",
  },
  {
    key: "waitMs",
    label: "Semaphore wait",
    color: "#d95926",
    help: "gpuSemaphoreWait: waiting to get onto the GPU (spark.rapids.sql.concurrentGpuTasks)",
  },
  {
    key: "notHeldMs",
    label: "Not holding GPU",
    color: "#77766f",
    help: "the rest of the task time: shuffle write, the first input batch, CPU operators, task setup",
  },
];

// Axis 2: what the task thread did, from RAPIDS op time (disjoint per operator)
const ACTIVITY_SEGMENTS: Segment<GpuActivityComponent>[] = [
  {
    key: "operatorsMs",
    label: "GPU operators",
    color: "#3987e5",
    help: "exclusive op time of the GPU operators (scan minus its IO wait, joins, aggregates, partitioning, ...). Semaphore waits happen inside the operator that acquires the GPU",
  },
  {
    key: "shuffleWriteMs",
    label: "Shuffle write",
    color: "#199e70",
    help: "shuffle write time: serialize / compress / write, after GPU partitioning (the GPU is released)",
  },
  {
    key: "shuffleReadMs",
    label: "Shuffle read",
    color: "#c98500",
    help: "op time (shuffle read): fetch wait + deserialization, before the batch is copied to the GPU",
  },
  {
    key: "scanIoWaitMs",
    label: "Scan IO wait",
    color: "#d55181",
    help: "scan buffer time: blocked until the host reader threads read / decompressed the input",
  },
  {
    key: "untrackedMs",
    label: "Not covered by op time",
    color: "#77766f",
    help: "work no op time covers: e.g. a shuffled hash join reading its build side when its iterator is created, CPU operators, task setup",
  },
];

const UTIL_SERIES_COLORS = ["#3987e5", "#d95926", "#199e70"];

const fmtMs = (v: number) => humanizeTimeDiff(duration(v));
const fmtPct = (v: number | undefined) =>
  v === undefined || v < 0 ? "n/a" : `${Math.round(v)}%`;

function SegmentBar<K extends string>({
  segments,
  values,
  total,
  height = 14,
}: {
  segments: Segment<K>[];
  values: Record<K, number>;
  total: number;
  height?: number;
}): JSX.Element | null {
  if (total <= 0) return null;
  return (
    <Box sx={{ display: "flex", gap: "2px", width: "100%", height }}>
      {segments.map((c) => {
        const value = values[c.key];
        const share = value / total;
        if (share < 0.005) return null;
        return (
          <Tooltip key={c.key} title={`${c.label}: ${fmtMs(value)} (${Math.round(share * 100)}%)`} arrow>
            <Box
              sx={{
                flexGrow: share,
                flexBasis: 0,
                minWidth: 3,
                backgroundColor: c.color,
                borderRadius: "4px",
              }}
            />
          </Tooltip>
        );
      })}
    </Box>
  );
}

function SegmentLegend<K extends string>({
  segments,
  values,
  total,
}: {
  segments: Segment<K>[];
  values?: Record<K, number>;
  total?: number;
}): JSX.Element {
  return (
    <Box sx={{ display: "flex", flexWrap: "wrap", columnGap: 2, rowGap: 0.5, mt: 1 }}>
      {segments.map((c) => (
        <Tooltip key={c.key} title={c.help} arrow>
          <Box sx={{ display: "flex", alignItems: "center", gap: 0.75 }}>
            <Box sx={{ width: 10, height: 10, borderRadius: "2px", backgroundColor: c.color }} />
            <Typography variant="body2" color="text.secondary">
              {c.label}
              {values && total
                ? ` ${Math.round((values[c.key] / total) * 100)}% · ${fmtMs(values[c.key])}`
                : ""}
            </Typography>
          </Box>
        </Tooltip>
      ))}
    </Box>
  );
}

const AxisBlock: FC<{ title: string; caption: string; children: React.ReactNode }> = ({
  title,
  caption,
  children,
}) => (
  <Box sx={{ mt: 2 }}>
    <Typography variant="subtitle2">{title}</Typography>
    <Typography variant="caption" color="text.secondary" component="div" sx={{ mb: 1 }}>
      {caption}
    </Typography>
    {children}
  </Box>
);

const StatTile: FC<{ label: string; value: string; hint?: string }> = ({ label, value, hint }) => (
  <Paper sx={{ p: 2, flex: "1 1 160px", minWidth: 160 }}>
    <Typography variant="body2" color="text.secondary">
      {label}
    </Typography>
    <Typography variant="h5">{value}</Typography>
    {hint && (
      <Typography variant="caption" color="text.secondary">
        {hint}
      </Typography>
    )}
  </Paper>
);

const bottleneckChipColor = (b: GpuBottleneck): "default" | "primary" | "warning" | "error" =>
  b === "gpu-compute" ? "primary" : b === "spill" ? "error" : b === "cpu" ? "default" : "warning";

const ExecutorTimeline: FC<{ executorId: string; points: ResourceTimelinePoint[]; startTime: number }> = ({
  executorId,
  points,
  startTime,
}) => {
  const hasSm = points.some((p) => p.smActivityPct >= 0);
  const series = [
    ...(hasSm
      ? [{ name: "SM activity %", data: points.map((p) => [p.timeMs - startTime, p.smActivityPct >= 0 ? p.smActivityPct : null]) }]
      : []),
    { name: "GPU utilization %", data: points.map((p) => [p.timeMs - startTime, p.gpuUtilPct >= 0 ? p.gpuUtilPct : null]) },
    { name: "Executor CPU %", data: points.map((p) => [p.timeMs - startTime, p.cpuUtilPct >= 0 ? Math.round(p.cpuUtilPct) : null]) },
  ];
  const options: ApexOptions = {
    chart: { type: "line", height: 220, animations: { enabled: false }, toolbar: { show: false }, background: "transparent" },
    colors: UTIL_SERIES_COLORS.slice(hasSm ? 0 : 1),
    stroke: { width: 2, curve: "straight" },
    dataLabels: { enabled: false },
    legend: { position: "top", horizontalAlign: "left" },
    grid: { borderColor: "#333" },
    tooltip: { shared: true, x: { formatter: (v: number) => humanizeTimeDiff(duration(v), false) } },
    xaxis: {
      type: "numeric",
      labels: { formatter: (v: string) => humanizeTimeDiff(duration(Number(v)), false) },
    },
    yaxis: { min: 0, max: 100, tickAmount: 4, labels: { formatter: (v: number) => `${Math.round(v)}%` } },
    theme: { mode: "dark" },
    title: { text: `Executor ${executorId}`, align: "left", style: { fontSize: "13px" } },
  };
  return <ReactApexChart options={options} series={series as any} type="line" height={220} />;
};

const StageTable: FC<{ stages: StageGpuDiagnosis[] }> = ({ stages }) => {
  const navigate = useNavigate();
  return (
    <TableContainer component={Paper} sx={{ maxHeight: 520 }}>
      <Table size="small" stickyHeader>
        <TableHead>
          <TableRow>
            <TableCell>Stage</TableCell>
            <TableCell>Query</TableCell>
            <TableCell align="right">Task time</TableCell>
            <TableCell>Bottleneck</TableCell>
            <TableCell sx={{ minWidth: 160 }}>GPU semaphore</TableCell>
            <TableCell sx={{ minWidth: 200 }}>Activity (op time)</TableCell>
            <TableCell align="right">SM act.</TableCell>
            <TableCell align="right">GPU util</TableCell>
            <TableCell align="right">CPU</TableCell>
            <TableCell>Most expensive operator</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {stages.map((d) => {
            const top = d.topOperators[0];
            const memory = d.spillToHostBytes + d.spillToDiskBytes;
            return (
              <TableRow
                key={`${d.stageId}_${d.attemptId}`}
                hover
                sx={{ cursor: d.sqlId !== undefined ? "pointer" : "default" }}
                onClick={() =>
                  d.sqlId !== undefined && navigate(`/summary?sqlid=${d.sqlId}&stageId=${d.stageId}`)
                }
              >
                <TableCell>{d.stageId}</TableCell>
                <TableCell sx={{ maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {d.sqlId !== undefined ? `${d.sqlId}: ${d.sqlDescription}` : "-"}
                </TableCell>
                <TableCell align="right">{fmtMs(d.taskTimeMs)}</TableCell>
                <TableCell>
                  <Tooltip title={d.reason} arrow>
                    <Chip
                      size="small"
                      label={GPU_BOTTLENECK_LABELS[d.bottleneck]}
                      color={bottleneckChipColor(d.bottleneck)}
                      variant="outlined"
                    />
                  </Tooltip>
                  {memory > 0 && (
                    <Tooltip title={`GPU spill ${humanFileSize(memory)}, ${d.retryCount + d.splitRetryCount} OOM retries`}>
                      <Chip size="small" label="spill" color="error" variant="outlined" sx={{ ml: 0.5 }} />
                    </Tooltip>
                  )}
                </TableCell>
                <TableCell>
                  {d.hasSemaphoreMetrics ? (
                    <SegmentBar<GpuSemaphoreComponent>
                      segments={SEMAPHORE_SEGMENTS}
                      values={d.semaphore}
                      total={d.taskTimeMs}
                    />
                  ) : (
                    "n/a"
                  )}
                </TableCell>
                <TableCell>
                  <Tooltip
                    title={
                      d.opTimeCoverage === undefined
                        ? "no RAPIDS op time metrics for this stage"
                        : `op time + shuffle write cover ${Math.round(d.opTimeCoverage * 100)}% of the task time${
                            d.opTimeCoverage > 1.02
                              ? " (over 100%: an operator shared with another stage, e.g. a reused exchange, has one metric total for both stages; scaled down)"
                              : ""
                          }`
                    }
                    arrow
                  >
                    <Box>
                      <SegmentBar<GpuActivityComponent>
                        segments={ACTIVITY_SEGMENTS}
                        values={d.activity}
                        total={d.taskTimeMs}
                      />
                    </Box>
                  </Tooltip>
                </TableCell>
                <TableCell align="right">{fmtPct(d.resource?.avgSmActivityPct)}</TableCell>
                <TableCell align="right">{fmtPct(d.resource?.avgGpuUtilPct)}</TableCell>
                <TableCell align="right">{fmtPct(d.resource?.avgCpuUtilPct)}</TableCell>
                <TableCell sx={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {top !== undefined ? `${top.enrichedName} · ${fmtMs(top.timeMs)}` : "-"}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

const ConfigTable: FC<{ diag: GpuDiagnosisStore }> = ({ diag }) => {
  const c = diag.config;
  const bytes = (v: number | undefined) => (v === undefined ? "not set" : humanFileSize(v));
  const val = (v: number | string | undefined) => (v === undefined ? "not set" : String(v));
  const rows: [string, string][] = [
    ["spark.executor.cores", val(c.executorCores)],
    [
      "spark.rapids.sql.concurrentGpuTasks",
      diag.dynamicConcurrency
        ? `${val(c.concurrentGpuTasks)} (starting point only, dynamic; max observed ${val(diag.maxConcurrentGpuTasksSeen)} per GPU)`
        : val(c.concurrentGpuTasks),
    ],
    ["spark.rapids.sql.concurrentGpuTasks.dynamic", String(c.concurrentGpuTasksDynamic)],
    ["spark.task.resource.gpu.amount", val(c.taskGpuAmount)],
    ["spark.rapids.sql.batchSizeBytes", bytes(c.batchSizeBytes)],
    ["spark.sql.files.maxPartitionBytes", bytes(c.maxPartitionBytes)],
    ["spark.rapids.memory.pinnedPool.size", bytes(c.pinnedPoolBytes)],
    ["spark.rapids.sql.multiThreadedRead.numThreads", val(c.multiThreadedReadThreads)],
    ["spark.shuffle.manager", val(c.shuffleManager)],
    ["spark.rapids.shuffle.mode", c.shuffleMode],
    ["spark.rapids.shuffle.multithreaded.skipMerge", String(c.skipMerge)],
    ["spark.rapids.shuffle.kudo.serializer.read.mode", c.kudoSerializer ? c.kudoReadMode : "(kudo disabled)"],
    ["spark.rapids.shuffle.multiThreaded.reader / writer.threads", `${val(c.shuffleReaderThreads)} / ${val(c.shuffleWriterThreads)}`],
    ["spark.shuffle.io.numConnectionsPerPeer", val(c.numConnectionsPerPeer)],
    ["spark.sql.adaptive.coalescePartitions.parallelismFirst", String(c.aqeParallelismFirst)],
  ];
  return (
    <TableContainer component={Paper}>
      <Table size="small">
        <TableBody>
          {rows.map(([k, v]) => (
            <TableRow key={k}>
              <TableCell sx={{ fontFamily: "monospace" }}>{k}</TableCell>
              <TableCell>{v}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export const GpuTab: FC<{}> = (): JSX.Element => {
  const diag = useAppSelector((state) => state.spark.gpuDiagnosis);
  const resourceMetrics = useAppSelector((state) => state.spark.resourceMetrics);
  const startTime = useAppSelector((state) => state.spark.runMetadata?.startTime) ?? 0;
  const gpuAlerts = useAppSelector(
    (state) => state.spark.alerts?.alerts.filter((a) => a.name.startsWith("gpu")).length ?? 0,
  );

  const timelines = useMemo(() => {
    const byExecutor = new Map<string, ResourceTimelinePoint[]>();
    for (const p of resourceMetrics?.timeline ?? []) {
      const list = byExecutor.get(p.executorId) ?? [];
      list.push(p);
      byExecutor.set(p.executorId, list);
    }
    return Array.from(byExecutor.entries()).slice(0, 8);
  }, [resourceMetrics]);

  if (diag === undefined) {
    return (
      <Box sx={{ height: "100%", display: "flex", justifyContent: "center", alignItems: "center" }}>
        <Alert severity="info">Not a RAPIDS (GPU) application</Alert>
      </Box>
    );
  }

  const total = diag.totalTaskTimeMs;
  const topStages = diag.stages.slice(0, 100);
  const bottlenecks = (Object.entries(diag.bottleneckTaskTimeMs) as [GpuBottleneck, number][])
    .filter(([, v]) => v > 0)
    .sort((a, b) => b[1] - a[1]);

  return (
    <Box sx={{ height: "100%", overflow: "auto", p: 2, display: "flex", flexDirection: "column", gap: 2 }}>
      {!diag.hasResourceSamples && (
        <Alert severity="info">
          No GPU / CPU utilization samples for this application. Load the DataFlint plugin on the
          executors (spark.plugins=com.nvidia.spark.SQLPlugin,io.dataflint.spark.SparkDataflintPlugin)
          - sampling starts automatically for RAPIDS apps, or set spark.dataflint.resourceMonitor.enabled=true.
          The breakdown below is computed from task metrics only.
        </Alert>
      )}

      <Box sx={{ display: "flex", flexWrap: "wrap", gap: 2 }}>
        <StatTile label="GPU stage task time" value={fmtMs(total)} hint={`${diag.stages.length} GPU stages`} />
        <StatTile
          label="Avg SM activity"
          value={fmtPct(diag.avgSmActivityPct)}
          hint={diag.avgSmActivityPct !== undefined ? "GPM, while tasks ran" : "needs Hopper+ GPM"}
        />
        <StatTile label="Avg GPU utilization" value={fmtPct(diag.avgGpuUtilPct)} hint="nvidia-smi, over-states SM use" />
        <StatTile label="Avg executor CPU" value={fmtPct(diag.avgCpuUtilPct)} hint="of executor cores" />
        <StatTile label="GPU alerts" value={String(gpuAlerts)} hint="see the Alerts tab" />
      </Box>

      <Paper sx={{ p: 2 }}>
        <Typography variant="h6">Where GPU stage task time goes</Typography>
        <Typography variant="body2" color="text.secondary">
          Task time of all GPU stages, split two independent ways. Each bar adds up to the task time
          on its own; the bars overlap each other (a task can hold the GPU while it waits for
          shuffle data), so do not add components across bars.
        </Typography>
        <AxisBlock
          title="GPU semaphore state"
          caption="From the gpuSemaphoreWait / gpuTime task accumulators. Exact. Holding the GPU is not kernel time - compare with the sampled SM activity."
        >
          <SegmentBar<GpuSemaphoreComponent>
            segments={SEMAPHORE_SEGMENTS}
            values={diag.totalSemaphore}
            total={total}
            height={22}
          />
          <SegmentLegend<GpuSemaphoreComponent>
            segments={SEMAPHORE_SEGMENTS}
            values={diag.totalSemaphore}
            total={total}
          />
        </AxisBlock>
        <AxisBlock
          title="What the task threads did"
          caption="From RAPIDS op time (exclusive per operator), shuffle write time and scan buffer time. Hover a legend entry for what each part measures."
        >
          <SegmentBar<GpuActivityComponent>
            segments={ACTIVITY_SEGMENTS}
            values={diag.totalActivity}
            total={total}
            height={22}
          />
          <SegmentLegend<GpuActivityComponent>
            segments={ACTIVITY_SEGMENTS}
            values={diag.totalActivity}
            total={total}
          />
        </AxisBlock>
        <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1, mt: 2 }}>
          <Typography variant="body2" color="text.secondary" sx={{ mr: 1 }}>
            Stages by primary bottleneck:
          </Typography>
          {bottlenecks.map(([b, v]) => (
            <Chip
              key={b}
              size="small"
              variant="outlined"
              color={bottleneckChipColor(b)}
              label={`${GPU_BOTTLENECK_LABELS[b]} · ${total > 0 ? Math.round((v / total) * 100) : 0}% of task time`}
            />
          ))}
        </Box>
      </Paper>

      {timelines.length > 0 && (
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6">Utilization timeline</Typography>
          <Typography variant="body2" color="text.secondary">
            Sampled every second on each executor ({resourceMetrics?.gpuSource}).
          </Typography>
          <Box sx={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(420px, 1fr))", gap: 2 }}>
            {timelines.map(([executorId, points]) => (
              <ExecutorTimeline key={executorId} executorId={executorId} points={points} startTime={startTime} />
            ))}
          </Box>
        </Paper>
      )}

      <Box>
        <Typography variant="h6" sx={{ mb: 1 }}>
          GPU stages by task time
        </Typography>
        <SegmentLegend<GpuSemaphoreComponent> segments={SEMAPHORE_SEGMENTS} />
        <SegmentLegend<GpuActivityComponent> segments={ACTIVITY_SEGMENTS} />
        <Box sx={{ mt: 1 }}>
          <StageTable stages={topStages} />
        </Box>
      </Box>

      <Box>
        <Typography variant="h6" sx={{ mb: 1 }}>
          RAPIDS settings
        </Typography>
        <ConfigTable diag={diag} />
      </Box>
    </Box>
  );
};

export default GpuTab;
