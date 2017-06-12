/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb.javadsl

import akka.Done
import akka.stream.alpakka.kairosdb.{KairosDBSinkStage, KairosSinkSettings}
import akka.stream.javadsl.Sink
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by SOROOSH on 3/22/17.
 */
object KairosSink {
  /**
   * Java API
   */
  def create(settings: KairosSinkSettings, kairosClient: HttpClient, executionContext: ExecutionContext): Sink[MetricBuilder, Future[Done]] =
    Sink.fromGraph(new KairosDBSinkStage(settings, kairosClient)(executionContext))

  /**
   * Java API
   */
  def create(kairosClient: HttpClient, executionContext: ExecutionContext): Sink[MetricBuilder, Future[Done]] =
    create(KairosSinkSettings.Defaults, kairosClient, executionContext)
}
