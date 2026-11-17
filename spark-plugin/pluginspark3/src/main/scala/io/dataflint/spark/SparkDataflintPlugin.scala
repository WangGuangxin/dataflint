package io.dataflint.spark

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.dataflint.{DataflintSparkUICommonLoader, DataflintSparkUILoader}
import org.apache.spark.dataflint.resource.{DataflintExecutorResourcePlugin, DataflintResourceMonitor}
import org.apache.spark.internal.Logging

import java.util
import scala.collection.JavaConverters.mapAsJavaMapConverter

class SparkDataflintPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new SparkDataflintDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = new DataflintExecutorResourcePlugin()
}

class SparkDataflintDriverPlugin extends DriverPlugin with Logging {
  var sc: SparkContext = null

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    this.sc = sc
    DataflintSparkUICommonLoader.registerInstrumentationExtension(sc)
    Map[String, String]().asJava
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    var webUrl = DataflintSparkUILoader.install(sc)
    logInfo(s"spark dataflint url is $webUrl/dataflint")
    super.registerMetrics(appId, pluginContext)
  }

  // Executor resource samples (CPU / IO / GPU) are forwarded to the listener bus so they are
  // shown live and written to the event log for the history server.
  override def receive(message: Any): AnyRef = DataflintResourceMonitor.onDriverReceive(sc, message)
}
