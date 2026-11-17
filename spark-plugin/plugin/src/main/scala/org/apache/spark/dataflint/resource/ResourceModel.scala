package org.apache.spark.dataflint.resource

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.kvstore.KVIndex

/**
 * Data model for executor resource sampling (CPU / IO / GPU).
 *
 * These classes travel three ways, so they are kept deliberately simple (no Option of
 * primitives, no Map with primitive keys — both lose their type through Jackson erasure):
 *  1. executor -> driver via the plugin RPC channel (java serialization),
 *  2. driver -> event log as a custom SparkListenerEvent (Jackson JSON), so that the
 *     history server can replay them,
 *  3. listener -> KVStore (Jackson again).
 *
 * Missing numeric values are encoded as -1.
 */

/** Number of tasks of one stage attempt that were running on the executor at sample time. */
case class StageTaskCount(stageId: Int, stageAttemptId: Int, numTasks: Int)

/** One GPU device sample, mostly from `nvidia-smi dmon` (GPM fields need Hopper or newer). */
case class GpuDeviceSample(
    index: Int,
    // nvidia-smi "sm" / utilization.gpu: % of time at least one kernel was running.
    // Over-estimates real SM usage, kept because it exists on every GPU generation.
    gpuUtilPct: Double,
    // nvidia-smi "mem": % of time device memory was being read/written.
    memBwUtilPct: Double,
    // GPM "SM Activity": % of SMs active averaged over the interval (true SM utilization).
    smActivityPct: Double,
    // GPM "SM Occupancy": resident warps / max warps.
    smOccupancyPct: Double,
    // GPM "DRAM Activity".
    dramActivityPct: Double,
    memUsedMiB: Double,
    powerW: Double,
    pcieTxMiBps: Double,
    pcieRxMiBps: Double)

case class ResourceSample(
    timeMs: Long,
    intervalMs: Long,
    // CPU time consumed by the executor JVM during the interval, in cores (1.0 == one core busy).
    processCpuCores: Double,
    // Whole host CPU utilization 0..100 (other executors on the host included).
    hostCpuPct: Double,
    gcTimeMs: Long,
    // /proc/self/io: bytes that really hit the storage layer.
    diskReadBytesPerSec: Double,
    diskWriteBytesPerSec: Double,
    // /proc/self/io: rchar/wchar, includes page cache hits and sockets (shuffle fetch).
    readCharsPerSec: Double,
    writeCharsPerSec: Double,
    activeTasks: Int,
    stageTasks: Seq[StageTaskCount],
    gpus: Seq[GpuDeviceSample])

case class ResourceSampleBatch(
    executorId: String,
    host: String,
    numCores: Int,
    gpuAddresses: Seq[String],
    gpuSource: String,
    samples: Seq[ResourceSample]) extends Serializable

/** Posted on the driver's listener bus; ends up in the event log. */
case class DataflintResourceSamplesEvent(batch: ResourceSampleBatch) extends SparkListenerEvent

class ResourceSampleBatchWrapper(val info: ResourceSampleBatch) {
  @KVIndex
  @JsonIgnore
  def id: String = {
    val first = info.samples.headOption.map(_.timeMs).getOrElse(0L)
    s"${info.executorId}_$first"
  }
}
