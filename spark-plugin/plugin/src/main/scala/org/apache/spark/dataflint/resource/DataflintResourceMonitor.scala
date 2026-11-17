package org.apache.spark.dataflint.resource

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, TaskContext, TaskFailedReason}
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Configuration of the executor resource monitor.
 *
 * spark.dataflint.resourceMonitor.enabled      default: auto (on when the RAPIDS SQLPlugin is
 *                                              configured in spark.plugins), "true"/"false" force it
 * spark.dataflint.resourceMonitor.intervalMs   sampling period, default 1000 (GPU sampling
 *                                              cannot go below 1s)
 * spark.dataflint.resourceMonitor.reportIntervalMs  how often a batch is sent to the driver
 *                                              (and so written to the event log), default 5000
 * spark.dataflint.resourceMonitor.gpu.enabled  default true, needs nvidia-smi on the executor
 * spark.dataflint.resourceMonitor.gpu.gpm      default true, request GPM SM-activity metrics
 * spark.dataflint.resourceMonitor.gpu.nvidiaSmiPath  explicit nvidia-smi location
 * spark.dataflint.resourceMonitor.gpu.devices  comma separated device indices to sample when
 *                                              the executor has no "gpu" resource (local mode)
 */
object DataflintResourceMonitor extends Logging {
  val PREFIX = "spark.dataflint.resourceMonitor"
  val ENABLED = s"$PREFIX.enabled"
  val INTERVAL_MS = s"$PREFIX.intervalMs"
  val REPORT_INTERVAL_MS = s"$PREFIX.reportIntervalMs"
  val GPU_ENABLED = s"$PREFIX.gpu.enabled"
  val GPU_GPM = s"$PREFIX.gpu.gpm"
  val GPU_NVIDIA_SMI = s"$PREFIX.gpu.nvidiaSmiPath"
  val GPU_DEVICES = s"$PREFIX.gpu.devices"

  private val RAPIDS_PLUGIN = "com.nvidia.spark.SQLPlugin"

  def isEnabled(conf: SparkConf): Boolean = {
    conf.getOption(ENABLED).map(_.trim.toLowerCase) match {
      case Some("true") => true
      case Some("false") => false
      case _ => conf.get("spark.plugins", "").split(",").map(_.trim).contains(RAPIDS_PLUGIN)
    }
  }

  /**
   * Host name of the executor. PluginContext.hostname() reads the RpcEnv's listen address,
   * which is null on executors in cluster mode (their RpcEnv is client only), so it throws a
   * NullPointerException there; it only works in local mode.
   */
  private[resource] def executorHost(ctx: PluginContext): String = {
    def nonEmpty(s: => String): Option[String] =
      scala.util.Try(s).toOption.flatMap(Option(_)).filter(_.nonEmpty)
    nonEmpty(SparkEnv.get.blockManager.blockManagerId.host)
      .orElse(nonEmpty(ctx.hostname()))
      .orElse(nonEmpty(java.net.InetAddress.getLocalHost.getHostName))
      .getOrElse("unknown")
  }

  /** Driver side: called from the driver plugin's receive(). */
  def onDriverReceive(sc: SparkContext, message: Any): AnyRef = {
    message match {
      case batch: ResourceSampleBatch =>
        try {
          sc.listenerBus.post(DataflintResourceSamplesEvent(batch))
        } catch {
          case e: Throwable => logWarning("Could not post resource samples", e)
        }
      case _ =>
    }
    // also the reply of ask() used by the final flush
    java.lang.Boolean.TRUE
  }
}

/**
 * Executor plugin that samples process CPU, GC, IO and GPU utilization and tags every sample
 * with the stages whose tasks were running at that moment, so the UI can attribute utilization
 * to stages (and through the SQL plan, to operators).
 */
class DataflintExecutorResourcePlugin extends ExecutorPlugin with Logging {
  import DataflintResourceMonitor.{GPU_DEVICES, GPU_ENABLED, GPU_GPM, GPU_NVIDIA_SMI, INTERVAL_MS, REPORT_INTERVAL_MS, isEnabled}

  private var ctx: PluginContext = _
  private var scheduler: Option[ScheduledExecutorService] = None
  private var gpuSampler: Option[GpuSampler] = None
  private var hostSampler: HostSampler = _
  private val runningByStage = new ConcurrentHashMap[(Int, Int), AtomicInteger]()
  // onTaskSucceeded / onTaskFailed run on the task thread after Spark has already cleared the
  // TaskContext, so the stage of the task is remembered per thread at onTaskStart
  private val currentTaskStage = new ThreadLocal[(Int, Int)]()
  private var lastActiveTasks = 0
  private var lastStages: Set[(Int, Int)] = Set.empty
  private val pending = ArrayBuffer[ResourceSample]()
  private var lastReport = System.currentTimeMillis()
  private var reportIntervalMs = 5000L
  private var gpuAddresses: Seq[String] = Seq.empty
  private var numCores = 0
  private var executorId = ""
  private var host = ""
  // the first failure of each kind is logged as a warning, later ones at debug level
  private val warnedSampling = new AtomicBoolean(false)
  private val warnedSend = new AtomicBoolean(false)

  private def warnOnce(flag: AtomicBoolean, message: String, e: Throwable): Unit = {
    if (flag.compareAndSet(false, true)) logWarning(s"$message (further failures are logged at debug level)", e)
    else logDebug(message, e)
  }

  override def init(ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    val conf = ctx.conf()
    if (!isEnabled(conf)) return
    this.ctx = ctx
    try {
      executorId = ctx.executorID()
      host = DataflintResourceMonitor.executorHost(ctx)
      val intervalMs = math.max(1000L, conf.getLong(INTERVAL_MS, 1000L))
      reportIntervalMs = math.max(intervalMs, conf.getLong(REPORT_INTERVAL_MS, 5000L))
      numCores = conf.getInt("spark.executor.cores", Runtime.getRuntime.availableProcessors())
      gpuAddresses = Option(ctx.resources()).map(_.asScala.get("gpu").map(_.addresses.toSeq).getOrElse(Seq.empty))
        .getOrElse(Seq.empty)
      hostSampler = new HostSampler()

      if (conf.getBoolean(GPU_ENABLED, defaultValue = true)) {
        GpuSampler.findNvidiaSmi(conf.getOption(GPU_NVIDIA_SMI)) match {
          case Some(smi) =>
            val devices = {
              val fromResources = GpuSampler.parseIndices(gpuAddresses)
              if (fromResources.nonEmpty) fromResources
              else conf.getOption(GPU_DEVICES).map(s => GpuSampler.parseIndices(s.split(",").toSeq)).getOrElse(Seq.empty)
            }
            val sampler = new GpuSampler(smi, devices, conf.getBoolean(GPU_GPM, defaultValue = true))
            sampler.start()
            gpuSampler = Some(sampler)
          case None => logInfo("nvidia-smi not found, GPU sampling disabled")
        }
      }

      val exec = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "dataflint-resource-monitor")
          t.setDaemon(true)
          t
        }
      })
      exec.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = tick()
      }, intervalMs, intervalMs, TimeUnit.MILLISECONDS)
      scheduler = Some(exec)
      logInfo(s"DataFlint resource monitor started (interval ${intervalMs}ms, gpus ${gpuAddresses.mkString(",")})")
    } catch {
      case e: Throwable => logWarning("Could not start DataFlint resource monitor", e)
    }
  }

  private def tick(): Unit = {
    try {
      val d = hostSampler.sample()
      val stages = runningByStage.asScala.toList
        .map { case ((stageId, attempt), n) => StageTaskCount(stageId, attempt, n.get()) }
        .filter(_.numTasks > 0)
        .sortBy(_.stageId)
      val sample = ResourceSample(
        timeMs = System.currentTimeMillis(),
        intervalMs = d.intervalMs,
        processCpuCores = d.cpuCores,
        hostCpuPct = d.hostCpuPct,
        gcTimeMs = d.gcMs,
        diskReadBytesPerSec = d.diskRead,
        diskWriteBytesPerSec = d.diskWrite,
        readCharsPerSec = d.rchar,
        writeCharsPerSec = d.wchar,
        activeTasks = stages.map(_.numTasks).sum,
        stageTasks = stages,
        gpus = gpuSampler.map(_.snapshot()).getOrElse(Seq.empty))
      pending += sample
      // Flush right away when the executor becomes idle or the set of running stages changes, so
      // the samples of a finished stage reach the event log before the application stops. Only
      // the samples of the very last stage depend on the (best effort) flush at shutdown.
      val stageSet = stages.map(st => (st.stageId, st.stageAttemptId)).toSet
      val becameIdle = sample.activeTasks == 0 && lastActiveTasks > 0
      val stagesChanged = stageSet.nonEmpty && lastStages.nonEmpty && stageSet != lastStages
      lastActiveTasks = sample.activeTasks
      if (stageSet.nonEmpty) lastStages = stageSet
      if (becameIdle || stagesChanged || sample.timeMs - lastReport >= reportIntervalMs) flush()
    } catch {
      case e: Throwable => warnOnce(warnedSampling, "DataFlint resource sampling failed", e)
    }
  }

  private def flush(synchronous: Boolean = false): Unit = {
    if (pending.isEmpty || ctx == null) return
    val batch = ResourceSampleBatch(
      executorId = executorId,
      host = host,
      numCores = numCores,
      gpuAddresses = gpuAddresses,
      gpuSource = gpuSampler.map(_.source).getOrElse("none"),
      samples = pending.toList)
    pending.clear()
    lastReport = System.currentTimeMillis()
    try {
      if (synchronous) {
        // At shutdown the driver stops its listener bus right after the executors, so wait (a
        // bounded time) for the driver to have posted the batch. ask() has no timeout of its own.
        val t = new Thread(new Runnable {
          override def run(): Unit = try ctx.ask(batch) catch { case _: Throwable => }
        }, "dataflint-resource-final-flush")
        t.setDaemon(true)
        t.start()
        t.join(2000)
      } else {
        ctx.send(batch)
      }
    } catch {
      case e: Throwable => warnOnce(warnedSend, "Could not send DataFlint resource samples to the driver", e)
    }
  }

  private def stageKey(): Option[(Int, Int)] =
    Option(TaskContext.get()).map(tc => (tc.stageId(), tc.stageAttemptNumber()))

  override def onTaskStart(): Unit = {
    if (scheduler.isEmpty) return
    stageKey().foreach { k =>
      currentTaskStage.set(k)
      val existing = runningByStage.get(k)
      val counter = if (existing != null) existing else {
        val created = new AtomicInteger()
        val prev = runningByStage.putIfAbsent(k, created)
        if (prev != null) prev else created
      }
      counter.incrementAndGet()
    }
  }

  private def taskEnded(): Unit = {
    if (scheduler.isEmpty) return
    // Counters are never removed (that would race with a concurrent onTaskStart of the same
    // stage); zero entries are filtered out when sampling.
    val k = currentTaskStage.get()
    if (k != null) {
      currentTaskStage.remove()
      val counter = runningByStage.get(k)
      if (counter != null) counter.decrementAndGet()
    }
  }

  override def onTaskSucceeded(): Unit = taskEnded()

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = taskEnded()

  override def shutdown(): Unit = {
    scheduler.foreach { s =>
      s.shutdown()
      try s.awaitTermination(2, TimeUnit.SECONDS) catch { case _: InterruptedException => }
    }
    try tick() catch { case _: Throwable => }
    try flush(synchronous = true) catch { case _: Throwable => }
    gpuSampler.foreach(_.stop())
  }
}
