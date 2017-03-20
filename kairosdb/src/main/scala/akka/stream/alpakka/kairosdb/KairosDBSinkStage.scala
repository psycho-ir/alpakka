/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb

import akka.Done
import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, StageLogging}
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

import scala.concurrent.{Future, Promise}
import scala.util.Try

final case class KairosDBSinkSettings() {

}

class KairosDBSinkStage(settings: KairosDBSinkSettings, kairosClient: HttpClient)
  extends GraphStageWithMaterializedValue[SinkShape[MetricBuilder], Future[Done]] {

  val in: Inlet[MetricBuilder] = Inlet("KairosSink.in")

  override val shape: SinkShape[MetricBuilder] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]()
    val logic = new KairosSinkStageLogic(in, shape, kairosClient, promise)

    (logic, promise.future)
  }
}


private[kairosdb] class KairosSinkStageLogic(
                                              in: Inlet[MetricBuilder],
                                              shape: SinkShape[MetricBuilder],
                                              kairosClient: HttpClient,
                                              promise: Promise[Done]
                                            ) extends GraphStageLogic(shape)
  with StageLogging {

  setHandler(in,
    new InHandler {
      override def onPush() = {
        try {
          kairosClient.pushMetrics(grab(in))
          pull(in)
        }
        catch {
          case exception: Exception => {
            promise.tryFailure(exception)
            failStage(exception)
          }
        }
      }

      override def onUpstreamFinish(): Unit = {
        super.onUpstreamFinish()
        promise.trySuccess(Done)
      }
    }

  )

  override def preStart(): Unit = {
    pull(in)
  }
}