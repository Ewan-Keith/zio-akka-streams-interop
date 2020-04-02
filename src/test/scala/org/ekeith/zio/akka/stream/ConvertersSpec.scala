package org.ekeith.zio.akka.stream

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source => AkkaSource }
import zio.{ Has, Layer, Managed, Task, ZIO, ZLayer }
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

  /** Helper method to build a Layer which has an Akka Streams Materializer */
  def buildMaterializerLayer(): Layer[Throwable, Has[Materializer]] = {
    val actorSystem: Layer[Throwable, Has[ActorSystem]] =
      ZLayer.fromManaged(Managed.make(Task(ActorSystem("Test")))(sys => Task.fromFuture(_ => sys.terminate()).either))

    actorSystem >>> ZLayer.fromFunction(as => Materializer(as.get))
  }

  val RunnableGraphAsTaskSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("runnableGraphAsTaskSpec")(
    testM("Converted graph that sums a list of integers materialises the correct result") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        materialisedValue <- runnableGraphAsZioEffect(runnable).provideLayer(materializerLayer)
      } yield assert(materialisedValue)(equalTo(55))
    },
    testM("graph can be converted and evaluated independently twice with correct result both times") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(1 to 10).toMat(sink)(Keep.right)

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        materialisedValue1 <- runnableGraphAsZioEffect(runnable).provideLayer(materializerLayer)
        materialisedValue2 <- runnableGraphAsZioEffect(runnable).provideLayer(materializerLayer)
        materialisedValues = (materialisedValue1, materialisedValue2)
      } yield assert(materialisedValues)(equalTo((55, 55)))
    },
    testM("graph that throws an exception should be caught correctly by ZIO") {
      val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
      val runnable: RunnableGraph[Future[Int]] = AkkaSource(-1 to 1).map(5 / _).toMat(sink)(Keep.right)

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        materialisedValue <- runnableGraphAsZioEffect(runnable).provideLayer(materializerLayer).either
      } yield assert(materialisedValue)(
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

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        materialisedValue <- runnableGraphAsZioEffect(sideEffectingGraph).provideLayer(materializerLayer)
      } yield assert(materialisedValue)(equalTo(3)) &&
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

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        materialisedValue <- runnableGraphAsZioEffect(sideEffectingGraph).either.provideLayer(materializerLayer)
      } yield assert(materialisedValue)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      ) && assert(testState.getState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("akkaSourceAsZioStreamSpec")(
    testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        testSource <- Task(AkkaSource(1 to 10))
        zioStream  <- akkaSourceAsZioStream(testSource)
        output     <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(materializerLayer)
      } yield assert(output)(equalTo(110))
    },
    testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        testSource <- Task(AkkaSource(1 to 10))
        zioStream  <- akkaSourceAsZioStream(testSource)
        output1    <- zioStream.fold(0)(_ + _).provideLayer(materializerLayer)
        output2    <- zioStream.fold(10)(_ + _).provideLayer(materializerLayer)
        output     = (output1, output2)
      } yield assert(output)(equalTo((55, 65)))
    },
    testM("Source that throws an exception should be caught correctly by ZIO") {

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        testSource <- Task(AkkaSource(-1 to 1).map(5 / _))
        zioStream  <- akkaSourceAsZioStream(testSource)
        output     <- zioStream.fold(0)(_ + _).either.provideLayer(materializerLayer)
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

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        zioStream <- akkaSourceAsZioStream(sideEffectingSource)
        output    <- zioStream.fold(0)(_ + _).provideLayer(materializerLayer)
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

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        zioStream <- akkaSourceAsZioStream(sideEffectingSource)
        output    <- zioStream.fold(0)(_ + _).either.provideLayer(materializerLayer)
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

      val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

      for {
        _          <- akkaSourceAsZioStream(sideEffectingSource1)
        zioStream2 <- akkaSourceAsZioStream(sideEffectingSource2)
        _          <- zioStream2.fold(0)(_ + _).provideLayer(materializerLayer)
      } yield assert(testState.getState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamMatSuite: Spec[Live, TestFailure[Throwable], TestSuccess] =
    suite("akkaSourceAsZioStreamMatSpec")(
      testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource     <- Task(AkkaSource(1 to 10))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output         <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(materializerLayer)
        } yield assert(output)(equalTo(110))
      },
      testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource     <- Task(AkkaSource(1 to 10))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output1        <- zioStream.fold(0)(_ + _).provideLayer(materializerLayer)
          output2        <- zioStream.fold(10)(_ + _).provideLayer(materializerLayer)
          output         = (output1, output2)
        } yield assert(output)(equalTo((55, 65)))
      },
      testM("Source that throws an exception should be caught correctly by ZIO") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource     <- Task(AkkaSource(-1 to 1).map(5 / _))
          (zioStream, _) <- akkaSourceAsZioStreamMat(testSource)
          output         <- zioStream.fold(0)(_ + _).either.provideLayer(materializerLayer)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          (zioStream, _) <- akkaSourceAsZioStreamMat(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).provideLayer(materializerLayer)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          (zioStream, _) <- akkaSourceAsZioStreamMat(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).either.provideLayer(materializerLayer)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          (_, _)          <- akkaSourceAsZioStreamMat(sideEffectingSource1)
          (zioStream2, _) <- akkaSourceAsZioStreamMat(sideEffectingSource2)
          _               <- zioStream2.fold(0)(_ + _).provideLayer(materializerLayer)
        } yield assert(testState.getState)(equalTo(targetState))
      },
      testM("Materialised value is produced after stream is ran") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(materializerLayer)
          materialisedValue <- m
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is produced even if stream evaluation begins after materialised value evaluation") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          fibre             <- m.fork
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(materializerLayer)
          materialisedValue <- fibre.join
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is never produced if the matching stream is not evaluated") {
        for {
          testSource <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          (_, m)     <- akkaSourceAsZioStreamMat(testSource)
          _          <- m
        } yield assert(false)(isTrue) // test will fail if completes
      } @@ forked @@ nonTermination(5.seconds),
      testM("Materialised source value will be successfully evaluated even if the stream fails with an exception") {

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          testSource        <- Task(AkkaSource(-1 to 1).map(5 / _).mapMaterializedValue(_ => 5))
          (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
          output            <- zioStream.fold(0)(_ + _).either.provideLayer(materializerLayer)
          materialisedValue <- m
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink              <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue <- testStream.run(sink).provideLayer(materializerLayer)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink               <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue1 <- testStream1.run(sink).provideLayer(materializerLayer)
          materialisedValue2 <- testStream2.run(sink).provideLayer(materializerLayer)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink              <- akkaSinkAsZioSink(sideEffectingSink)
          materialisedValue <- testStream1.run(sink).either.provideLayer(materializerLayer)
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
          _ <- akkaSinkAsZioSink(sideEffectingSink)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink               <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture <- testStream.run(sink).provideLayer(materializerLayer)
          materialisedValue  <- ZIO.fromFuture(_ => materialisedFuture)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink                <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture1 <- testStream1.run(sink).provideLayer(materializerLayer)
          materialisedFuture2 <- testStream2.run(sink).provideLayer(materializerLayer)
          materialisedValue1  <- ZIO.fromFuture(_ => materialisedFuture1)
          materialisedValue2  <- ZIO.fromFuture(_ => materialisedFuture2)
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

        val materializerLayer: Layer[Throwable, Has[Materializer]] = buildMaterializerLayer()

        for {
          sink               <- akkaSinkAsZioSinkMat(sideEffectingSink)
          materialisedFuture <- testStream1.run(sink).provideLayer(materializerLayer)
          materialisedValue  <- ZIO.fromFuture(_ => materialisedFuture).either
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
          _ <- akkaSinkAsZioSinkMat(sideEffectingSink)
        } yield assert(testState.getState)(equalTo(targetState))
      }
    )

}
