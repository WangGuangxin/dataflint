package org.apache.spark.dataflint.resource

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ResourceMetricsAggregatorSpec extends AnyFunSuite with Matchers {

  private val header = GpuSampler.splitCsv(
    "gpu, pwr, gtemp, mtemp, sm, mem, enc, dec, jpg, ofa, fb, bar1, ccpm, smutil, smocc, dram, pcitx, pcirx")

  test("parse nvidia-smi dmon csv line with GPM metrics") {
    val s = GpuSampler.parseLine(header, " 1, 250, 40, -, 87, 35, 0, 0, 0, 0, 40960, 5, 0, 52, 21, 30, 1200, 9000").get
    s.index shouldBe 1
    s.gpuUtilPct shouldBe 87.0
    s.smActivityPct shouldBe 52.0
    s.smOccupancyPct shouldBe 21.0
    s.memUsedMiB shouldBe 40960.0
    s.pcieRxMiBps shouldBe 9000.0
  }

  test("GPM warm-up values '-' are reported as missing") {
    val s = GpuSampler.parseLine(header, " 0, 36, 29, -, 0, 0, 0, 0, 0, 0, 0, 0, 0, -, -, -, -, -").get
    s.smActivityPct shouldBe -1.0
    s.gpuUtilPct shouldBe 0.0
  }

  test("GPU addresses that are not indices are ignored") {
    GpuSampler.parseIndices(Seq("0", "1")) shouldBe Seq(0, 1)
    GpuSampler.parseIndices(Seq("GPU-1234")) shouldBe Seq.empty
  }

  private def gpu(util: Double, sm: Double) = GpuDeviceSample(0, util, 10, sm, 20, 30, 1000, 200, 100, 200)

  private def sample(t: Long, cpu: Double, stages: Seq[StageTaskCount], g: Seq[GpuDeviceSample]) =
    ResourceSample(t, 1000, cpu, 50, 0, 1024 * 1024, 0, 0, 0, stages.map(_.numTasks).sum, stages, g)

  test("stage usage is weighted by the stage's share of running tasks") {
    val batch = ResourceSampleBatch("1", "host", 8, Seq("0"), "nvidia-smi-dmon+gpm", Seq(
      sample(1000, 4.0, Seq(StageTaskCount(1, 0, 4)), Seq(gpu(80, 40))),
      // stage 1 and 2 overlap: stage 1 gets weight 0.25, stage 2 gets 0.75
      sample(2000, 8.0, Seq(StageTaskCount(1, 0, 1), StageTaskCount(2, 0, 3)), Seq(gpu(20, 10))),
      // idle sample: not attributed to any stage
      sample(3000, 0.1, Seq.empty, Seq(gpu(0, 0)))))

    val summary = ResourceMetricsAggregator.aggregate(Seq(batch))
    summary.enabled shouldBe true
    summary.executors.map(_.numSamples) shouldBe Seq(3)
    val s1 = summary.stages.find(_.stageId == 1).get
    val s2 = summary.stages.find(_.stageId == 2).get
    s1.sampledSeconds shouldBe 1.25 +- 1e-9
    s2.sampledSeconds shouldBe 0.75 +- 1e-9
    // (80 * 1 + 20 * 0.25) / 1.25
    s1.avgGpuUtilPct shouldBe 68.0 +- 1e-9
    s1.maxGpuUtilPct shouldBe 80.0
    s2.avgGpuUtilPct shouldBe 20.0 +- 1e-9
    s2.avgSmActivityPct shouldBe 10.0 +- 1e-9
    // 8 cores busy on an 8 core executor
    s2.avgCpuUtilPct shouldBe 100.0 +- 1e-9
    s1.avgDiskReadMBps shouldBe 1.0 +- 1e-9
    summary.timeline.size shouldBe 3
  }

  test("missing GPU data stays -1 instead of becoming 0") {
    val batch = ResourceSampleBatch("1", "host", 8, Seq.empty, "none",
      Seq(sample(1000, 2.0, Seq(StageTaskCount(5, 0, 2)), Seq.empty)))
    val s = ResourceMetricsAggregator.aggregate(Seq(batch)).stages.head
    s.avgGpuUtilPct shouldBe -1.0
    s.avgCpuUtilPct shouldBe 25.0 +- 1e-9
  }

  test("timeline is down-sampled per executor") {
    val samples = (0 until 100).map(i => sample(i * 1000L, 1.0, Seq.empty, Seq(gpu(i, i))))
    val summary = ResourceMetricsAggregator.aggregate(Seq(ResourceSampleBatch("1", "h", 4, Seq("0"), "x", samples)), 10)
    summary.timeline.size shouldBe 10
    summary.timeline.head.gpuUtilPct shouldBe 4.5 +- 1e-9
  }

  test("no batches means disabled") {
    ResourceMetricsAggregator.aggregate(Seq.empty).enabled shouldBe false
  }
}
