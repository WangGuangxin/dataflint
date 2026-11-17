package org.apache.spark.dataflint.api

import org.apache.spark.dataflint.listener.DataflintStore
import org.apache.spark.dataflint.resource.ResourceMetricsAggregator
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUIPage}
import org.json4s.{Extraction, JObject}

import jakarta.servlet.http.HttpServletRequest
import scala.xml.Node

/**
 * Executor CPU / IO / GPU utilization collected by the DataFlint executor plugin, aggregated
 * per stage plus a down-sampled per-executor timeline.
 */
class DataflintResourceMetricsPage(ui: SparkUI, dataflintStore: DataflintStore)
  extends WebUIPage("resourcemetrics") with Logging {
  override def renderJson(request: HttpServletRequest) = {
    try {
      val maxPoints = Option(request.getParameter("maxPoints")).flatMap(p => scala.util.Try(p.toInt).toOption).getOrElse(600)
      val summary = ResourceMetricsAggregator.aggregate(dataflintStore.resourceSampleBatches(), maxPoints)
      Extraction.decompose(summary)(org.json4s.DefaultFormats)
    }
    catch {
      case e: Throwable => {
        logError("failed to serve dataflint resource metrics", e)
        JObject()
      }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = Seq[Node]()
}
