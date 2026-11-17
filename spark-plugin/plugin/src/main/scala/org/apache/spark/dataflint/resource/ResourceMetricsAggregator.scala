package org.apache.spark.dataflint.resource

import scala.collection.mutable

case class ExecutorResourceInfo(
    executorId: String,
    host: String,
    numCores: Int,
    gpuAddresses: Seq[String],
    gpuSource: String,
    firstSampleMs: Long,
    lastSampleMs: Long,
    numSamples: Int)

/** One (possibly down-sampled) point of an executor's utilization timeline. -1 = not available. */
case class ResourceTimelinePoint(
    timeMs: Long,
    executorId: String,
    activeTasks: Double,
    processCpuCores: Double,
    cpuUtilPct: Double,
    hostCpuPct: Double,
    diskReadMBps: Double,
    diskWriteMBps: Double,
    gpuUtilPct: Double,
    smActivityPct: Double,
    smOccupancyPct: Double,
    dramActivityPct: Double,
    gpuMemUsedMiB: Double,
    pcieRxMiBps: Double,
    pcieTxMiBps: Double)

/**
 * Utilization observed while tasks of a stage were running. Every sample is weighted by the
 * share of the executor's running tasks that belonged to the stage, so when two stages overlap
 * on an executor each one "sees" the sample proportionally.
 */
case class StageResourceUsage(
    stageId: Int,
    stageAttemptId: Int,
    sampledSeconds: Double,
    numExecutors: Int,
    avgActiveTasks: Double,
    avgProcessCpuCores: Double,
    avgCpuUtilPct: Double,
    avgHostCpuPct: Double,
    avgGcPct: Double,
    avgDiskReadMBps: Double,
    avgDiskWriteMBps: Double,
    avgReadCharsMBps: Double,
    avgGpuUtilPct: Double,
    maxGpuUtilPct: Double,
    avgSmActivityPct: Double,
    avgSmOccupancyPct: Double,
    avgDramActivityPct: Double,
    avgGpuMemUsedMiB: Double,
    maxGpuMemUsedMiB: Double,
    avgPcieRxMiBps: Double,
    avgPcieTxMiBps: Double)

case class ResourceMetricsSummary(
    enabled: Boolean,
    gpuSource: String,
    executors: Seq[ExecutorResourceInfo],
    stages: Seq[StageResourceUsage],
    timeline: Seq[ResourceTimelinePoint])

object ResourceMetricsAggregator {
  private val MB = 1024.0 * 1024.0

  /** Average of the non-negative values, -1 when none. */
  private def avgValid(values: Seq[Double]): Double = {
    val valid = values.filter(_ >= 0)
    if (valid.isEmpty) -1.0 else valid.sum / valid.size
  }

  private def maxValid(values: Seq[Double]): Double = {
    val valid = values.filter(_ >= 0)
    if (valid.isEmpty) -1.0 else valid.max
  }

  private def gpuAvg(s: ResourceSample, f: GpuDeviceSample => Double): Double = avgValid(s.gpus.map(f))

  private class WeightedAvg {
    private var sum = 0.0
    private var weight = 0.0
    private var maxV = -1.0
    def add(value: Double, w: Double): Unit = if (value >= 0 && w > 0) {
      sum += value * w
      weight += w
      if (value > maxV) maxV = value
    }
    def avg: Double = if (weight == 0) -1.0 else sum / weight
    def max: Double = maxV
  }

  private class StageAcc {
    val executors = mutable.Set[String]()
    var seconds = 0.0
    val activeTasks, cpuCores, cpuUtil, hostCpu, gc, diskRead, diskWrite, rchar, gpuUtil, sm, smOcc, dram,
      gpuMem, pcieRx, pcieTx = new WeightedAvg
  }

  def aggregate(batches: Seq[ResourceSampleBatch], maxPointsPerExecutor: Int = 600): ResourceMetricsSummary = {
    if (batches.isEmpty) return ResourceMetricsSummary(enabled = false, "none", Seq.empty, Seq.empty, Seq.empty)

    val byExecutor = batches.groupBy(_.executorId)
    val stageAccs = mutable.Map[(Int, Int), StageAcc]()

    val executors = byExecutor.toList.map { case (execId, execBatches) =>
      val last = execBatches.maxBy(_.samples.headOption.map(_.timeMs).getOrElse(0L))
      val samples = execBatches.flatMap(_.samples).sortBy(_.timeMs)
      val cores = math.max(1, last.numCores)

      samples.foreach { s =>
        val total = s.stageTasks.map(_.numTasks).sum
        if (total > 0) {
          val cpuUtil = if (s.processCpuCores >= 0) math.min(100.0, s.processCpuCores / cores * 100.0) else -1.0
          val gcPct = if (s.intervalMs > 0) math.min(100.0, s.gcTimeMs.toDouble / s.intervalMs * 100.0) else -1.0
          s.stageTasks.foreach { st =>
            val w = st.numTasks.toDouble / total
            val acc = stageAccs.getOrElseUpdate((st.stageId, st.stageAttemptId), new StageAcc)
            acc.executors += execId
            acc.seconds += w * s.intervalMs / 1000.0
            acc.activeTasks.add(st.numTasks, w)
            acc.cpuCores.add(s.processCpuCores, w)
            acc.cpuUtil.add(cpuUtil, w)
            acc.hostCpu.add(s.hostCpuPct, w)
            acc.gc.add(gcPct, w)
            acc.diskRead.add(if (s.diskReadBytesPerSec >= 0) s.diskReadBytesPerSec / MB else -1, w)
            acc.diskWrite.add(if (s.diskWriteBytesPerSec >= 0) s.diskWriteBytesPerSec / MB else -1, w)
            acc.rchar.add(if (s.readCharsPerSec >= 0) s.readCharsPerSec / MB else -1, w)
            acc.gpuUtil.add(gpuAvg(s, g => g.gpuUtilPct), w)
            acc.sm.add(gpuAvg(s, g => g.smActivityPct), w)
            acc.smOcc.add(gpuAvg(s, g => g.smOccupancyPct), w)
            acc.dram.add(gpuAvg(s, g => g.dramActivityPct), w)
            acc.gpuMem.add(maxValid(s.gpus.map(g => g.memUsedMiB)), w)
            acc.pcieRx.add(gpuAvg(s, g => g.pcieRxMiBps), w)
            acc.pcieTx.add(gpuAvg(s, g => g.pcieTxMiBps), w)
          }
        }
      }

      ExecutorResourceInfo(execId, last.host, last.numCores, last.gpuAddresses, last.gpuSource,
        samples.headOption.map(_.timeMs).getOrElse(0L), samples.lastOption.map(_.timeMs).getOrElse(0L), samples.size) ->
        downsample(execId, cores, samples, maxPointsPerExecutor)
    }

    val stages = stageAccs.toList.sortBy(_._1).map { case ((stageId, attempt), a) =>
      StageResourceUsage(stageId, attempt, a.seconds, a.executors.size, a.activeTasks.avg,
        a.cpuCores.avg, a.cpuUtil.avg, a.hostCpu.avg, a.gc.avg, a.diskRead.avg, a.diskWrite.avg, a.rchar.avg,
        a.gpuUtil.avg, a.gpuUtil.max, a.sm.avg, a.smOcc.avg, a.dram.avg, a.gpuMem.avg, a.gpuMem.max,
        a.pcieRx.avg, a.pcieTx.avg)
    }

    val gpuSource = executors.map(_._1.gpuSource).find(_ != "none").getOrElse("none")
    ResourceMetricsSummary(
      enabled = true,
      gpuSource = gpuSource,
      executors = executors.map(_._1).sortBy(_.executorId),
      stages = stages,
      timeline = executors.flatMap(_._2).sortBy(p => (p.timeMs, p.executorId)))
  }

  private def downsample(execId: String, cores: Int, samples: Seq[ResourceSample], maxPoints: Int): Seq[ResourceTimelinePoint] = {
    if (samples.isEmpty) return Seq.empty
    val bucketSize = math.max(1, math.ceil(samples.size.toDouble / math.max(1, maxPoints)).toInt)
    samples.grouped(bucketSize).toList.map { bucket =>
      def a(f: ResourceSample => Double): Double = avgValid(bucket.map(f))
      val cpuCores = a(_.processCpuCores)
      ResourceTimelinePoint(
        timeMs = bucket.map(_.timeMs).sum / bucket.size,
        executorId = execId,
        activeTasks = a(_.activeTasks.toDouble),
        processCpuCores = cpuCores,
        cpuUtilPct = if (cpuCores >= 0) math.min(100.0, cpuCores / cores * 100.0) else -1.0,
        hostCpuPct = a(_.hostCpuPct),
        diskReadMBps = a(s => if (s.diskReadBytesPerSec >= 0) s.diskReadBytesPerSec / MB else -1),
        diskWriteMBps = a(s => if (s.diskWriteBytesPerSec >= 0) s.diskWriteBytesPerSec / MB else -1),
        gpuUtilPct = a(s => gpuAvg(s, g => g.gpuUtilPct)),
        smActivityPct = a(s => gpuAvg(s, g => g.smActivityPct)),
        smOccupancyPct = a(s => gpuAvg(s, g => g.smOccupancyPct)),
        dramActivityPct = a(s => gpuAvg(s, g => g.dramActivityPct)),
        gpuMemUsedMiB = a(s => maxValid(s.gpus.map(g => g.memUsedMiB))),
        pcieRxMiBps = a(s => gpuAvg(s, g => g.pcieRxMiBps)),
        pcieTxMiBps = a(s => gpuAvg(s, g => g.pcieTxMiBps)))
    }
  }
}
