package org.apache.spark.dataflint.resource

import org.apache.spark.internal.Logging

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

/**
 * Samples GPU utilization by streaming `nvidia-smi dmon` in CSV mode.
 *
 * One long-lived child process per executor (1s resolution, which is the dmon minimum) is far
 * cheaper than forking nvidia-smi on every tick and needs no NVML/JNI binding. On Hopper and
 * newer GPUs we also ask for GPM metrics, which give the real "SM Activity"/"SM Occupancy";
 * plain `sm` (utilization.gpu) only says "a kernel was running" and over-states usage (measured
 * on this kind of workload: 74% utilization.gpu vs 50% SM activity).
 *
 * The reader thread keeps only the latest value per device; the resource monitor picks it up
 * on its own tick.
 */
class GpuSampler(nvidiaSmiPath: String, deviceIndices: Seq[Int], useGpm: Boolean) extends Logging {

  private val latest = new AtomicReference[Map[Int, GpuDeviceSample]](Map.empty)
  @volatile private var process: Option[Process] = None
  @volatile private var stopped = false
  @volatile private var gpmActive = useGpm
  private var readerThread: Option[Thread] = None

  def source: String = if (gpmActive) "nvidia-smi-dmon+gpm" else "nvidia-smi-dmon"

  def start(): Unit = {
    val t = new Thread(new Runnable {
      override def run(): Unit = runLoop()
    }, "dataflint-gpu-sampler")
    t.setDaemon(true)
    t.start()
    readerThread = Some(t)
  }

  def stop(): Unit = {
    stopped = true
    process.foreach(p => p.destroy())
    readerThread.foreach(_.interrupt())
  }

  /** Latest sample per device, filtered to the devices this executor owns. */
  def snapshot(): Seq[GpuDeviceSample] = {
    val all = latest.get()
    val selected = if (deviceIndices.isEmpty) all.values.toSeq else deviceIndices.flatMap(all.get)
    selected.sortBy(_.index)
  }

  private def command(withGpm: Boolean): Seq[String] = {
    val base = Seq(nvidiaSmiPath, "dmon", "-d", "1", "-s", "pum", "--format", "csv,nounit")
    val devices = if (deviceIndices.nonEmpty) Seq("-i", deviceIndices.mkString(",")) else Seq.empty
    // 2 = SM Activity, 3 = SM Occupancy, 10 = DRAM Activity, 20/21 = PCIe TX/RX
    val gpm = if (withGpm) Seq("--gpm-metrics", "2,3,10,20,21") else Seq.empty
    base ++ devices ++ gpm
  }

  private def runLoop(): Unit = {
    var failures = 0
    while (!stopped && failures < 5) {
      val startedAt = System.currentTimeMillis()
      val linesParsed = runOnce(command(gpmActive))
      if (stopped) return
      if (linesParsed == 0 && gpmActive) {
        // GPM is not supported on this GPU / driver: fall back to the basic counters.
        logInfo("nvidia-smi dmon with GPM metrics produced no data, falling back to basic counters")
        gpmActive = false
      } else {
        failures += 1
      }
      // Back off, the process should normally run until the executor stops.
      if (System.currentTimeMillis() - startedAt < 10000) Thread.sleep(2000L * failures)
    }
    if (!stopped) logWarning("GPU sampling disabled after repeated nvidia-smi failures")
  }

  /** Runs nvidia-smi until it exits; returns the number of data lines parsed. */
  private def runOnce(cmd: Seq[String]): Int = {
    var parsed = 0
    try {
      val pb = new ProcessBuilder(cmd: _*)
      pb.redirectErrorStream(true)
      val p = pb.start()
      process = Some(p)
      val reader = new BufferedReader(new InputStreamReader(p.getInputStream, StandardCharsets.UTF_8))
      var header: Option[Array[String]] = None
      var line = reader.readLine()
      while (line != null && !stopped) {
        val trimmed = line.trim
        if (trimmed.startsWith("#")) {
          header = Some(GpuSampler.splitCsv(trimmed.stripPrefix("#")))
        } else if (trimmed.nonEmpty && header.isDefined) {
          GpuSampler.parseLine(header.get, trimmed).foreach { s =>
            parsed += 1
            latest.set(latest.get() + (s.index -> s)) // single writer thread
          }
        }
        line = reader.readLine()
      }
      p.waitFor()
    } catch {
      case _: InterruptedException => // stopping
      case e: Throwable => logWarning(s"nvidia-smi sampling failed: ${e.getMessage}")
    }
    parsed
  }
}

object GpuSampler extends Logging {

  private[resource] def splitCsv(line: String): Array[String] = line.split(",").map(_.trim)

  private def num(fields: Map[String, String], key: String): Double = {
    fields.get(key) match {
      case Some(v) if v.nonEmpty && v != "-" && !v.startsWith("N/A") =>
        try v.toDouble catch { case _: NumberFormatException => -1.0 }
      case _ => -1.0
    }
  }

  /** Parses one dmon CSV data line against the header columns. */
  private[resource] def parseLine(header: Array[String], line: String): Option[GpuDeviceSample] = {
    val values = splitCsv(line)
    if (values.length < header.length) return None
    val fields = header.zip(values).toMap
    val idx = num(fields, "gpu")
    if (idx < 0) return None
    Some(GpuDeviceSample(
      index = idx.toInt,
      gpuUtilPct = num(fields, "sm"),
      memBwUtilPct = num(fields, "mem"),
      smActivityPct = num(fields, "smutil"),
      smOccupancyPct = num(fields, "smocc"),
      dramActivityPct = num(fields, "dram"),
      memUsedMiB = num(fields, "fb"),
      powerW = num(fields, "pwr"),
      pcieTxMiBps = num(fields, "pcitx"),
      pcieRxMiBps = num(fields, "pcirx")))
  }

  /** Resolves nvidia-smi on PATH (or an explicit path); None when there is no NVIDIA driver. */
  def findNvidiaSmi(configured: Option[String]): Option[String] = {
    configured.filter(p => new java.io.File(p).canExecute).orElse {
      val pathDirs = Option(System.getenv("PATH")).map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Seq.empty)
      (pathDirs ++ Seq("/usr/bin", "/usr/local/bin", "/usr/local/nvidia/bin"))
        .map(d => new java.io.File(d, "nvidia-smi"))
        .find(_.canExecute)
        .map(_.getAbsolutePath)
    }
  }

  /** Executor GPU addresses are normally nvidia-smi indices ("0", "1"); ignore UUID-style ones. */
  def parseIndices(addresses: Seq[String]): Seq[Int] = {
    val parsed = addresses.flatMap(a => scala.util.Try(a.trim.toInt).toOption)
    if (parsed.size == addresses.size) parsed.distinct.sorted else Seq.empty
  }
}

/** Process CPU / GC / IO sampler based on JMX and /proc. All values are deltas per interval. */
class HostSampler {
  private val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
  private val gcBeans = {
    import scala.collection.JavaConverters._
    java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.asScala.toSeq
  }
  private var lastCpuNs: Long = processCpuNs()
  private var lastGcMs: Long = gcMs()
  private var lastIo: Map[String, Long] = HostSampler.readProcIo()
  private var lastTimeMs: Long = System.currentTimeMillis()

  private def processCpuNs(): Long = osBean match {
    case b: com.sun.management.OperatingSystemMXBean => b.getProcessCpuTime
    case _ => -1L
  }

  private def hostCpuPct(): Double = osBean match {
    case b: com.sun.management.OperatingSystemMXBean =>
      val v = b.getSystemCpuLoad
      if (v < 0) -1.0 else v * 100.0
    case _ => -1.0
  }

  private def gcMs(): Long = gcBeans.map(b => math.max(0L, b.getCollectionTime)).sum

  case class Delta(intervalMs: Long, cpuCores: Double, hostCpuPct: Double, gcMs: Long,
                   diskRead: Double, diskWrite: Double, rchar: Double, wchar: Double)

  def sample(): Delta = {
    val now = System.currentTimeMillis()
    val cpu = processCpuNs()
    val gc = gcMs()
    val io = HostSampler.readProcIo()
    val intervalMs = math.max(1L, now - lastTimeMs)
    val seconds = intervalMs / 1000.0
    def rate(key: String): Double =
      (for (cur <- io.get(key); prev <- lastIo.get(key)) yield math.max(0L, cur - prev) / seconds).getOrElse(-1.0)
    val cores = if (cpu >= 0 && lastCpuNs >= 0) (cpu - lastCpuNs).toDouble / (intervalMs * 1e6) else -1.0
    val d = Delta(intervalMs, cores, hostCpuPct(), math.max(0L, gc - lastGcMs),
      rate("read_bytes"), rate("write_bytes"), rate("rchar"), rate("wchar"))
    lastTimeMs = now
    lastCpuNs = cpu
    lastGcMs = gc
    lastIo = io
    d
  }
}

object HostSampler {
  private val procIo = new java.io.File("/proc/self/io")

  def readProcIo(): Map[String, Long] = {
    if (!procIo.canRead) return Map.empty
    try {
      val src = scala.io.Source.fromFile(procIo, "UTF-8")
      try {
        val m = mutable.Map[String, Long]()
        src.getLines().foreach { l =>
          val parts = l.split(":")
          if (parts.length == 2) scala.util.Try(parts(1).trim.toLong).foreach(v => m(parts(0).trim) = v)
        }
        m.toMap
      } finally src.close()
    } catch {
      case _: Throwable => Map.empty
    }
  }
}
