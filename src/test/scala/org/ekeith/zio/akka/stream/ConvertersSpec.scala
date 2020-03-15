package org.ekeith.zio.akka.stream

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source => AkkaSource }
import zio.Task
import zio.test.{ assert, suite, testM, DefaultRunnableSpec, Spec, TestFailure, TestSuccess, ZSpec }
import zio.test.Assertion.equalTo
import zio.test.environment.TestEnvironment

import scala.concurrent.Future

object ConvertersSpec extends DefaultRunnableSpec {

  def spec: ZSpec[TestEnvironment, Any] = suite("All Tests")(GraphConverterSuite, SourceConverterSuite)
  import Converters._

  val GraphConverterSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("GraphConvertersSpec")(
    testM("Converted graph that sums a list of integers materialises the correct result") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output      <- runnableGraphAsTask(runnable).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(55))
    },
    testM("graph can be converted and evaluated independently twice with correct result both times") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output1     <- runnableGraphAsTask(runnable).provide(mat)
        output2     <- runnableGraphAsTask(runnable).provide(mat)
        output      = (output1, output2)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo((55, 55)))
    },
    testM("A converted graph evaluates basic side effects when ran") {
      import scala.collection.mutable.{ Map => MMap }

      // expected updated mutable map to test against
      val targetMap = MMap(
        1 -> "record for key 1",
        2 -> "record for key 2",
        3 -> "record for key 3"
      )

      // set up the mutable map to update and a helper map update function
      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      // graph that updates the mutable map and returns the last key value updated
      val sideEffectGraph: RunnableGraph[Future[Int]] =
        AkkaSource(List(1, 2, 3))
          .map(updateState(_, effectState))
          .toMat(Sink.last)(Keep.right)

      // run the side-effecting graph and test the materialised output, and the updated mutable map
      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output      <- runnableGraphAsTask(sideEffectGraph).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(3)) &&
        assert(effectState)(equalTo(targetMap))
    }
  )

  val SourceConverterSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("SourceConvertersSpec")(
    testM("Converted Akka source can be properly folded over as a ZIO Stream") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         <- Task(Materializer(actorSystem))
        zioStream   = akkaSourceAsZioStream(testSource).provide(mat)
        output      <- zioStream.fold(0)(_ + _)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(55))
    },
    testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         <- Task(Materializer(actorSystem))
        zioStream   = akkaSourceAsZioStream(testSource).provide(mat)
        output1     <- zioStream.fold(0)(_ + _)
        output2     <- zioStream.fold(10)(_ + _)
        output      = (output1, output2)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo((55, 65)))
    }
  )
}
