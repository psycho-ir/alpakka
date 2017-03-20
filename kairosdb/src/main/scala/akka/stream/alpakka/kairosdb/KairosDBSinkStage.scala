/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb

import akka.Done
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, StageLogging}
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

import scala.concurrent.{Future, Promise}

final case class KairosDBSinkSettings() {

}

class KairosDBSinkStage(baseUrl: String, settings: KairosDBSinkSettings, kairosClient: HttpClient)
  extends GraphStageWithMaterializedValue[SinkShape[MetricBuilder], Future[Done]] {

  val in: Inlet[MetricBuilder] = Inlet("KairosSink.in")

  override val shape: SinkShape[MetricBuilder] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = ???

    (logic, promise.future)
  }
}

private[kairosdb] class KairosSinkStageLogic(
      baseUrl: String,
      shape: SinkShape[MetricBuilder],
      promise: Promise[Done]
) extends GraphStageLogic(shape)
    with StageLogging {

}