package akka.stream.alpakka.kairosdb.scaladsl

import java.io.IOException

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import org.kairosdb.client.HttpClient
import org.kairosdb.client.builder.MetricBuilder
import org.kairosdb.client.response.Response
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar.mock

import scala.concurrent.duration._
import scala.concurrent.Await



/**
 * Created by SOROOSH on 3/20/17.
 */
class KairosSinkSpec extends FlatSpec with Matchers {

  //#init-mat
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  //#init-mat

  it should "send a metric via http client" in {
    val client = mock[HttpClient]
    when(client.pushMetrics(any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = new Response(200)
      }
    )
    val (probe, future) = TestSource.probe[MetricBuilder].toMat(KairosSink(client))(Keep.both).run()
    val builder = MetricBuilder.getInstance()
    probe.sendNext(builder).sendComplete()
    Await.result(future, 1 second) shouldBe Done

    verify(client,times(1)).pushMetrics(any())
  }

  it should "fail stage on http client error" in {
    val client = mock[HttpClient]
    when(client.pushMetrics(any())).thenAnswer(
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock) = throw new IOException("Fake IO error")
      }
    )

    val (probe, future) = TestSource.probe[MetricBuilder].toMat(KairosSink(client))(Keep.both).run()
    val builder = MetricBuilder.getInstance()

    probe.sendNext(builder).sendComplete()
    an[IOException] should be thrownBy {
      Await.result(future, 1 second)
    }

    verify(client,times(1)).pushMetrics(any())
  }


}
