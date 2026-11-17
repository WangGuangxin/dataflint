package org.apache.spark.dataflint.resource

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.serializer.JavaSerializer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class DataflintExecutorResourcePluginSpec extends AnyFunSuite with Matchers {

  /** Behaves like the PluginContext of an executor in cluster mode. */
  private class ExecutorLikeContext extends PluginContext {
    val received = new ConcurrentLinkedQueue[AnyRef]()
    override def metricRegistry(): MetricRegistry = new MetricRegistry()
    override def conf(): SparkConf = new SparkConf(false)
      .set(DataflintResourceMonitor.ENABLED, "true")
      .set(DataflintResourceMonitor.GPU_ENABLED, "false")
      .set("spark.executor.cores", "4")
    override def executorID(): String = "7"
    // an executor's RpcEnv has no listen address, Spark's implementation throws here
    override def hostname(): String = throw new NullPointerException("RpcEnv.address() is null")
    override def resources(): java.util.Map[String, ResourceInformation] =
      Map("gpu" -> new ResourceInformation("gpu", Array("1"))).asJava
    override def send(message: AnyRef): Unit = received.add(message)
    override def ask(message: AnyRef): AnyRef = { received.add(message); java.lang.Boolean.TRUE }
  }

  // the serializer the RPC layer uses for executor -> driver plugin messages
  private def javaRoundTrip[T: scala.reflect.ClassTag](value: T): T = {
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    ser.deserialize[T](ser.serialize(value), getClass.getClassLoader)
  }

  test("sends samples when PluginContext.hostname() fails, as on cluster-mode executors") {
    val ctx = new ExecutorLikeContext
    val plugin = new DataflintExecutorResourcePlugin()
    plugin.init(ctx, new java.util.HashMap[String, String]())
    plugin.shutdown() // takes a last sample and flushes it synchronously

    val batches = ctx.received.asScala.toList.collect { case b: ResourceSampleBatch => b }
    batches should have size 1
    val batch = batches.head
    batch.executorId shouldBe "7"
    batch.host should not be empty
    batch.gpuAddresses shouldBe Seq("1")
    batch.samples should not be empty

    // executor -> driver messages are java serialized in cluster mode (not in local mode)
    javaRoundTrip(batch) shouldBe batch
  }
}
