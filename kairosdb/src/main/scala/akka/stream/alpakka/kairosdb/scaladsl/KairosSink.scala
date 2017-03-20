/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb.scaladsl

import akka.Done
import akka.stream.alpakka.kairosdb.KairosDBSinkStage
import akka.stream.scaladsl.Sink
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

import scala.concurrent.Future

object KairosSink {
  def apply(kairosClient: HttpClient): Sink[MetricBuilder, Future[Done]] = Sink.fromGraph(new KairosDBSinkStage(null, kairosClient))
}
