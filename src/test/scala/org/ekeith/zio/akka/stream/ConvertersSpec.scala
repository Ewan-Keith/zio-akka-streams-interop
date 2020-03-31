package org.ekeith.zio.akka.stream

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source => AkkaSource }
import zio.{ Task, ZIO, ZLayer }
import zio.test.{ assert, suite, testM, DefaultRunnableSpec, Spec, TestFailure, TestSuccess, ZSpec }
import zio.test.Assertion.{ anything, equalTo, hasMessage, isLeft, isSubtype, isTrue, isUnit, matchesRegex }
import zio.test.environment.{ Live, TestEnvironment }

import scala.collection.mutable.{ Map => MMap }
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.duration._

import scala.concurrent.Future

object ConvertersSpec extends DefaultRunnableSpec {

  import Converters._

  def spec: ZSpec[TestEnvironment, Any] =
    suite("All Tests")(
      RunnableGraphAsTaskSuite,
      AkkaSourceAsZioStreamSuite,
      AkkaSourceAsZioStreamMatSuite,
      akkaSinkAsZioSinkSuite
    ) @@ timed

  /** set up a mutable map used to test side effecting stream stages */
  case class TestState() {
    private val state = MMap[Int, String]()

    def updateState(key: Int): Int  = { this.state += (key -> s"record for key $key"); key }
    def getState: MMap[Int, String] = this.state
  }

  val RunnableGraphAsTaskSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("runnableGraphAsTaskSpec")(
    testM("Converted graph that sums a list of integers materialises the correct result") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        output      <- runnableGraphAsZioEffect(runnable).provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(55))
    },
    testM("graph can be converted and evaluated independently twice with correct result both times") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        output1     <- runnableGraphAsZioEffect(runnable).provideLayer(mat)
        output2     <- runnableGraphAsZioEffect(runnable).provideLayer(mat)
        output      = (output1, output2)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo((55, 55)))
    },
    testM("graph that throws an exception should be caught correctly by ZIO") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(-1 to 1).map(5 / _).toMat(sink)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        output      <- runnableGraphAsZioEffect(runnable).provideLayer(mat).either
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      )
    },
    testM("A converted graph evaluates basic side effects when ran") {

      val targetState = MMap(
        1 -> "record for key 1",
        2 -> "record for key 2",
        3 -> "record for key 3"
      )

      val testState = TestState()

      val sideEffectingGraph: RunnableGraph[Future[Int]] =
        AkkaSource(List(1, 2, 3))
          .map(testState.updateState)
          .toMat(Sink.last)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        output      <- runnableGraphAsZioEffect(sideEffectingGraph).provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(3)) &&
        assert(testState.getState)(equalTo(targetState))
    },
    testM("A converted graph  with side effects should not evaluate any effects past a thrown exception") {

      val targetState = MMap(-5 -> "record for key -5")

      val testState = TestState()

      val sideEffectingGraph: RunnableGraph[Future[Int]] =
        AkkaSource(-1 to 1)
          .map(5 / _)
          .map(testState.updateState)
          .toMat(Sink.last)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        output      <- runnableGraphAsZioEffect(sideEffectingGraph).either.provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      ) && assert(testState.getState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("akkaSourceAsZioStreamSpec")(
    testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output      <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(110))
    },
    testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output1     <- zioStream.fold(0)(_ + _).provideLayer(mat)
        output2     <- zioStream.fold(10)(_ + _).provideLayer(mat)
        output      = (output1, output2)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo((55, 65)))
    },
    testM("Source that throws an exception should be caught correctly by ZIO") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(-1 to 1).map(5 / _))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output      <- zioStream.fold(0)(_ + _).either.provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      )
    },
    testM("A converted source evaluates basic side effects when ran") {

      val targetState = MMap(
        1 -> "record for key 1",
        2 -> "record for key 2",
        3 -> "record for key 3"
      )

      val testState = TestState()

      val sideEffectingSource: AkkaSource[Int, NotUsed] =
        AkkaSource(List(1, 2, 3))
          .map(testState.updateState)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        zioStream   <- akkaSourceAsZioStream(sideEffectingSource)
        output      <- zioStream.fold(0)(_ + _).provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(6)) &&
        assert(testState.getState)(equalTo(targetState))
    },
    testM("A converted source with side effects should not evaluate any effects past a thrown exception") {

      val targetState = MMap(-5 -> "record for key -5")

      val testState = TestState()

      val sideEffectingSource: AkkaSource[Int, NotUsed] =
        AkkaSource(-1 to 1)
          .map(5 / _)
          .map(testState.updateState)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        zioStream   <- akkaSourceAsZioStream(sideEffectingSource)
        output      <- zioStream.fold(0)(_ + _).either.provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      ) && assert(testState.getState)(equalTo(targetState))
    },
    testM("A converted source doesn't evaluate its side effects if the ZStream is not completed (e.g. folded over)") {

      val targetState = MMap(
        2 -> "record for key 2",
        4 -> "record for key 4",
        6 -> "record for key 6"
      )

      val testState = TestState()

      val sideEffectingSource1 =
        AkkaSource(List(1, 2, 3))
          .map(testState.updateState)

      val sideEffectingSource2 =
        AkkaSource(List(2, 4, 6))
          .map(testState.updateState)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         = ZLayer.fromEffect(Task(Materializer(actorSystem)))
        _           <- akkaSourceAsZioStream(sideEffectingSource1)
        zioStream2  <- akkaSourceAsZioStream(sideEffectingSource2)
        _           <- zioStream2.fold(0)(_ + _).provideLayer(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(testState.getState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamMatSuite: Spec[Live, TestFailure[Throwable], TestSuccess] =
    suite("akkaSourceAsZioStreamMatSpec")(
      testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {
        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(1 to 10))
          mat            = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output         <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110))
      },
      testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {
        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(1 to 10))
          mat            = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output1        <- zioStream.fold(0)(_ + _).provideLayer(mat)
          output2        <- zioStream.fold(10)(_ + _).provideLayer(mat)
          output         = (output1, output2)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo((55, 65)))
      },
      testM("Source that throws an exception should be caught correctly by ZIO") {

        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(-1 to 1).map(5 / _))
          mat            = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output         <- zioStream.fold(0)(_ + _).either.provideLayer(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(equalTo("/ by zero")))
        )
      },
      testM("A converted source evaluates basic side effects when ran") {

        val targetState = MMap(
          1 -> "record for key 1",
          2 -> "record for key 2",
          3 -> "record for key 3"
        )

        val testState = TestState()

        val sideEffectingSource: AkkaSource[Int, NotUsed] =
          AkkaSource(List(1, 2, 3))
            .map(testState.updateState)

        for {
          actorSystem    <- Task(ActorSystem("Test"))
          mat            = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, _) <- akkaSourceAsZioStreamMat(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).provideLayer(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(6)) &&
          assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted source with side effects should not evaluate any effects past a thrown exception") {

        val targetState = MMap(-5 -> "record for key -5")

        val testState = TestState()

        val sideEffectingSource: AkkaSource[Int, NotUsed] =
          AkkaSource(-1 to 1)
            .map(5 / _)
            .map(testState.updateState)

        for {
          actorSystem    <- Task(ActorSystem("Test"))
          mat            = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, _) <- akkaSourceAsZioStreamMat(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).either.provideLayer(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(equalTo("/ by zero")))
        ) && assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted source doesn't evaluate its side effects if the ZStream is not completed (e.g. folded over)") {

        val targetState = MMap(
          2 -> "record for key 2",
          4 -> "record for key 4",
          6 -> "record for key 6"
        )

        val testState = TestState()

        val sideEffectingSource1 =
          AkkaSource(List(1, 2, 3))
            .map(testState.updateState)

        val sideEffectingSource2 =
          AkkaSource(List(2, 4, 6))
            .map(testState.updateState)

        for {
          actorSystem     <- Task(ActorSystem("Test"))
          mat             = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (_, _)          <- akkaSourceAsZioStreamMat(sideEffectingSource1)
          (zioStream2, _) <- akkaSourceAsZioStreamMat(sideEffectingSource2)
          _               <- zioStream2.fold(0)(_ + _).provideLayer(mat)
          _               <- Task(actorSystem.terminate())
        } yield assert(testState.getState)(equalTo(targetState))
      },
      testM("Materialised value is produced after stream is ran") {
        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          mat               = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(mat)
          materialisedValue <- m
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is produced even if stream evaluation begins after materialised value evaluation") {
        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          mat               = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          fibre             <- m.fork
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(mat)
          materialisedValue <- fibre.join
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is never produced if the matching stream is not evaluated") {
        for {
          actorSystem <- Task(ActorSystem("Test"))
          testSource  <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          (_, m)      <- akkaSourceAsZioStreamMat(testSource)
          _           <- m
          _           <- Task(actorSystem.terminate())
        } yield assert(false)(isTrue) // test will fail if completes
      } @@ forked @@ nonTermination(5.seconds),
      testM("Materialised source value will be successfully evaluated even if the stream fails with an exception") {

        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(-1 to 1).map(5 / _).mapMaterializedValue(_ => 5))
          mat               = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          output            <- zioStream.fold(0)(_ + _).either.provideLayer(mat)
          materialisedValue <- m
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(equalTo("/ by zero")))
        ) && assert(materialisedValue)(equalTo(5))
      }
    )

  val akkaSinkAsZioSinkSuite: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("akkaSinkAsZioSinkSuiteSpec")(
      testM("A converted Sink evaluates basic side effects when ran using ZStream.run") {

        val targetState = MMap(
          1 -> "record for key 1",
          2 -> "record for key 2",
          3 -> "record for key 3"
        )

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        val testStream = ZStream.fromIterable(List(1, 2, 3))

        for {
          actorSystem       <- Task(ActorSystem("Test"))
          mat               = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink              <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue <- testStream.run(sink).provideLayer(mat)
          _                 <- Task(actorSystem.terminate())
        } yield assert(materialisedValue)(isUnit) &&
          assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink can be evaluated against multiple streams independently using ZStream.run") {

        val targetState = MMap(
          1 -> "record for key 1",
          2 -> "record for key 2",
          3 -> "record for key 3",
          7 -> "record for key 7",
          8 -> "record for key 8",
          9 -> "record for key 9"
        )

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        val testStream1 = ZStream.fromIterable(List(1, 2, 3))
        val testStream2 = ZStream.fromIterable(List(7, 8, 9))

        for {
          actorSystem        <- Task(ActorSystem("Test"))
          mat                = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink               <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue1 <- testStream1.run(sink).provideLayer(mat)
          materialisedValue2 <- testStream2.run(sink).provideLayer(mat)
          _                  <- Task(actorSystem.terminate())
        } yield assert(materialisedValue1)(isUnit) &&
          assert(materialisedValue2)(isUnit) &&
          assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink should raise the appropriate exception if thrown by the underlying Akka Streams sink") {

        val targetState = MMap(-5 -> "record for key -5")

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(5 / i); () })

        val testStream1 = ZStream.fromIterable(-1 to 1)

        // Unfortunately the pre-materialisation of the sink means no more informative an error message is surfaced
        val SinkFailurePattern = "Stage with GraphStageLogic .* stopped before async invocation was processed"

        for {
          actorSystem       <- Task(ActorSystem("Test"))
          mat               = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink              <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue <- testStream1.run(sink).either.provideLayer(mat)
          _                 <- Task(actorSystem.terminate())
        } yield assert(materialisedValue)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(matchesRegex(SinkFailurePattern)))
        ) && assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink does not evaluate its side effects if never ran with a ZStream") {

        val targetState = MMap[Int, String]()

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        for {
          actorSystem <- Task(ActorSystem("Test"))
          _           <- Task(Materializer(actorSystem))
          _           <- akkaSinkAsZioSink(sideEffectingSink)
          _           <- Task(actorSystem.terminate())
        } yield assert(testState.getState)(equalTo(targetState))
      }
    )

  val akkaSinkAsZioSinkMatSuite: Spec[Any, TestFailure[Throwable], TestSuccess] =
    suite("akkaSinkAsZioSinkMatSuiteSpec")(
      testM("A converted Sink evaluates basic side effects when ran using ZStream.run") {

        val targetState = MMap(
          1 -> "record for key 1",
          2 -> "record for key 2",
          3 -> "record for key 3"
        )

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        val testStream = ZStream.fromIterable(List(1, 2, 3))

        for {
          actorSystem        <- Task(ActorSystem("Test"))
          mat                = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink               <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture <- testStream.run(sink).provideLayer(mat)
          materialisedValue  <- ZIO.fromFuture(_ => materialisedFuture)
          _                  <- Task(actorSystem.terminate())
        } yield assert(materialisedValue)(equalTo(Done)) &&
          assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink can be evaluated against multiple streams independently using ZStream.run") {

        val targetState = MMap(
          1 -> "record for key 1",
          2 -> "record for key 2",
          3 -> "record for key 3",
          7 -> "record for key 7",
          8 -> "record for key 8",
          9 -> "record for key 9"
        )

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        val testStream1 = ZStream.fromIterable(List(1, 2, 3))
        val testStream2 = ZStream.fromIterable(List(7, 8, 9))

        for {
          actorSystem         <- Task(ActorSystem("Test"))
          mat                 = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink                <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture1 <- testStream1.run(sink).provideLayer(mat)
          materialisedFuture2 <- testStream2.run(sink).provideLayer(mat)
          materialisedValue1  <- ZIO.fromFuture(_ => materialisedFuture1)
          materialisedValue2  <- ZIO.fromFuture(_ => materialisedFuture2)
          _                   <- Task(actorSystem.terminate())
        } yield assert(materialisedValue1)(equalTo(Done)) &&
          assert(materialisedValue2)(equalTo(Done)) &&
          assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink should raise the appropriate exception if thrown by the underlying Akka Streams sink") {

        val targetState = MMap(-5 -> "record for key -5")

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(5 / i); () })

        val testStream1 = ZStream.fromIterable(-1 to 1)

        // Unfortunately the pre-materialisation of the sink means no more informative an error message is surfaced
        val SinkFailurePattern = "Stage with GraphStageLogic .* stopped before async invocation was processed"

        for {
          actorSystem        <- Task(ActorSystem("Test"))
          mat                = ZLayer.fromEffect(Task(Materializer(actorSystem)))
          sink               <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture <- testStream1.run(sink).provideLayer(mat)
          materialisedValue  <- ZIO.fromFuture(_ => materialisedFuture).either
          _                  <- Task(actorSystem.terminate())
        } yield assert(materialisedValue)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(matchesRegex(SinkFailurePattern)))
        ) && assert(testState.getState)(equalTo(targetState))
      },
      testM("A converted Sink does not evaluate its side effects if never ran with a ZStream") {

        val targetState = MMap[Int, String]()

        val testState = TestState()

        val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })

        for {
          actorSystem <- Task(ActorSystem("Test"))
          _           <- Task(Materializer(actorSystem))
          _           <- akkaSinkAsZioSinkMat(sideEffectingSink)
          _           <- Task(actorSystem.terminate())
        } yield assert(testState.getState)(equalTo(targetState))
      }
    )

}
