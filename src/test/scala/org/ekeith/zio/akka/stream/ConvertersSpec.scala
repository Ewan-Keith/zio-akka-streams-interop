package org.ekeith.zio.akka.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source => AkkaSource }
import zio.Task
import zio.test.{ assert, suite, testM, DefaultRunnableSpec, Spec, TestFailure, TestSuccess, ZSpec }
import zio.test.Assertion.{ anything, equalTo, hasMessage, isLeft, isSubtype }
import zio.test.environment.TestEnvironment

import scala.concurrent.Future

object ConvertersSpec extends DefaultRunnableSpec {

  import Converters._

  def spec: ZSpec[TestEnvironment, Any] = suite("All Tests")(
    RunnableGraphAsTaskSuite,
    AkkaSourceAsZioStreamSuite
  )

  val RunnableGraphAsTaskSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("runnableGraphAsTaskSpec")(
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
    testM("graph that throws an exception should be caught correctly by ZIO") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(-1 to 1).map(5 / _).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output      <- runnableGraphAsTask(runnable).provide(mat).either
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      )
    },
    testM("A converted graph evaluates basic side effects when ran") {
      import scala.collection.mutable.{ Map => MMap }

      val targetState = MMap(
        1 -> "record for key 1",
        2 -> "record for key 2",
        3 -> "record for key 3"
      )

      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      val sideEffectingGraph: RunnableGraph[Future[Int]] =
        AkkaSource(List(1, 2, 3))
          .map(updateState(_, effectState))
          .toMat(Sink.last)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output      <- runnableGraphAsTask(sideEffectingGraph).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(3)) &&
        assert(effectState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("akkaSourceAsZioStreamSpec")(
    testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         <- Task(Materializer(actorSystem))
        zioStream   = akkaSourceAsZioStream(testSource).provide(mat)
        output      <- zioStream.map(_ * 2).fold(0)(_ + _)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(110))
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
    },
    testM("Source that throws an exception should be caught correctly by ZIO") {

      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(-1 to 1).map(5 / _))
        mat         <- Task(Materializer(actorSystem))
        zioStream   = akkaSourceAsZioStream(testSource).provide(mat)
        output      <- zioStream.fold(0)(_ + _).either
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      )
    },
    testM("A converted source evaluates basic side effects when ran") {
      import scala.collection.mutable.{ Map => MMap }

      val targetState = MMap(
        1 -> "record for key 1",
        2 -> "record for key 2",
        3 -> "record for key 3"
      )

      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      val sideEffectingSource: AkkaSource[Int, NotUsed] =
        AkkaSource(List(1, 2, 3))
          .map(updateState(_, effectState))

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        zioStream   = akkaSourceAsZioStream(sideEffectingSource).provide(mat)
        output      <- zioStream.fold(0)(_ + _)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(6)) &&
        assert(effectState)(equalTo(targetState))
    }
  )
}
