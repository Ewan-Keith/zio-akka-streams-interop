package org.ekeith.zio.akka.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source => AkkaSource }
import zio.Task
import zio.test.{ assert, suite, testM, DefaultRunnableSpec, Spec, TestFailure, TestSuccess, ZSpec }
import zio.test.Assertion.{ anything, equalTo, hasMessage, isLeft, isSubtype, isTrue }
import zio.test.environment.{ Live, TestEnvironment }
import zio.test.TestAspect._
import zio.duration._

import scala.concurrent.Future

object ConvertersSpec extends DefaultRunnableSpec {

  import Converters._

  def spec: ZSpec[TestEnvironment, Any] =
    suite("All Tests")(
      RunnableGraphAsTaskSuite,
      AkkaSourceAsZioStreamSuite,
      AkkaSourceAsZioStreamMSuite
    ) @@ timed

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
    },
    testM("A converted graph  with side effects should not evaluate any effects past a thrown exception") {
      import scala.collection.mutable.{ Map => MMap }

      val targetState = MMap(-5 -> "record for key -5")

      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      val sideEffectingGraph: RunnableGraph[Future[Int]] =
        AkkaSource(-1 to 1)
          .map(5 / _)
          .map(updateState(_, effectState))
          .toMat(Sink.last)(Keep.right)

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        output      <- runnableGraphAsTask(sideEffectingGraph).either.provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      ) && assert(effectState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamSuite: Spec[Any, TestFailure[Throwable], TestSuccess] = suite("akkaSourceAsZioStreamSpec")(
    testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         <- Task(Materializer(actorSystem))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output      <- zioStream.map(_ * 2).fold(0)(_ + _).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(110))
    },
    testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {
      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(1 to 10))
        mat         <- Task(Materializer(actorSystem))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output1     <- zioStream.fold(0)(_ + _).provide(mat)
        output2     <- zioStream.fold(10)(_ + _).provide(mat)
        output      = (output1, output2)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo((55, 65)))
    },
    testM("Source that throws an exception should be caught correctly by ZIO") {

      for {
        actorSystem <- Task(ActorSystem("Test"))
        testSource  <- Task(AkkaSource(-1 to 1).map(5 / _))
        mat         <- Task(Materializer(actorSystem))
        zioStream   <- akkaSourceAsZioStream(testSource)
        output      <- zioStream.fold(0)(_ + _).either.provide(mat)
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
        zioStream   <- akkaSourceAsZioStream(sideEffectingSource)
        output      <- zioStream.fold(0)(_ + _).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(equalTo(6)) &&
        assert(effectState)(equalTo(targetState))
    },
    testM("A converted source with side effects should not evaluate any effects past a thrown exception") {
      import scala.collection.mutable.{ Map => MMap }

      val targetState = MMap(-5 -> "record for key -5")

      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      val sideEffectingSource: AkkaSource[Int, NotUsed] =
        AkkaSource(-1 to 1)
          .map(5 / _)
          .map(updateState(_, effectState))

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        zioStream   <- akkaSourceAsZioStream(sideEffectingSource)
        output      <- zioStream.fold(0)(_ + _).either.provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(output)(
        isSubtype[Either[ArithmeticException, Int]](anything) &&
          isLeft(hasMessage(equalTo("/ by zero")))
      ) && assert(effectState)(equalTo(targetState))
    },
    testM("A converted source doesn't evaluate its side effects if the ZStream is not completed (e.g. folded over)") {
      import scala.collection.mutable.{ Map => MMap }

      val targetState = MMap(
        2 -> "record for key 2",
        4 -> "record for key 4",
        6 -> "record for key 6"
      )

      val effectState = MMap[Int, String]()
      val updateState: (Int, MMap[Int, String]) => Int =
        (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

      val sideEffectingSource1 =
        AkkaSource(List(1, 2, 3))
          .map(updateState(_, effectState))

      val sideEffectingSource2 =
        AkkaSource(List(2, 4, 6))
          .map(updateState(_, effectState))

      for {
        actorSystem <- Task(ActorSystem("Test"))
        mat         <- Task(Materializer(actorSystem))
        _           <- akkaSourceAsZioStream(sideEffectingSource1)
        zioStream2  <- akkaSourceAsZioStream(sideEffectingSource2)
        _           <- zioStream2.fold(0)(_ + _).provide(mat)
        _           <- Task(actorSystem.terminate())
      } yield assert(effectState)(equalTo(targetState))
    }
  )

  val AkkaSourceAsZioStreamMSuite: Spec[Live, TestFailure[Throwable], TestSuccess] =
    suite("akkaSourceAsZioStreamMSpec")(
      testM("Converted Akka source can be properly mapped and folded over as a ZIO Stream") {
        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(1 to 10))
          mat            <- Task(Materializer(actorSystem))
          (zioStream, _) <- akkaSourceAsZioStreamM(testSource)
          output         <- zioStream.map(_ * 2).fold(0)(_ + _).provide(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110))
      },
      testM("Converted Akka source should be able to be evaluated as a ZStream multiple times") {
        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(1 to 10))
          mat            <- Task(Materializer(actorSystem))
          (zioStream, _) <- akkaSourceAsZioStreamM(testSource)
          output1        <- zioStream.fold(0)(_ + _).provide(mat)
          output2        <- zioStream.fold(10)(_ + _).provide(mat)
          output         = (output1, output2)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo((55, 65)))
      },
      testM("Source that throws an exception should be caught correctly by ZIO") {

        for {
          actorSystem    <- Task(ActorSystem("Test"))
          testSource     <- Task(AkkaSource(-1 to 1).map(5 / _))
          mat            <- Task(Materializer(actorSystem))
          (zioStream, _) <- akkaSourceAsZioStreamM(testSource)
          output         <- zioStream.fold(0)(_ + _).either.provide(mat)
          _              <- Task(actorSystem.terminate())
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
          actorSystem    <- Task(ActorSystem("Test"))
          mat            <- Task(Materializer(actorSystem))
          (zioStream, _) <- akkaSourceAsZioStreamM(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).provide(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(6)) &&
          assert(effectState)(equalTo(targetState))
      },
      testM("A converted source with side effects should not evaluate any effects past a thrown exception") {
        import scala.collection.mutable.{ Map => MMap }

        val targetState = MMap(-5 -> "record for key -5")

        val effectState = MMap[Int, String]()
        val updateState: (Int, MMap[Int, String]) => Int =
          (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

        val sideEffectingSource: AkkaSource[Int, NotUsed] =
          AkkaSource(-1 to 1)
            .map(5 / _)
            .map(updateState(_, effectState))

        for {
          actorSystem    <- Task(ActorSystem("Test"))
          mat            <- Task(Materializer(actorSystem))
          (zioStream, _) <- akkaSourceAsZioStreamM(sideEffectingSource)
          output         <- zioStream.fold(0)(_ + _).either.provide(mat)
          _              <- Task(actorSystem.terminate())
        } yield assert(output)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(equalTo("/ by zero")))
        ) && assert(effectState)(equalTo(targetState))
      },
      testM("A converted source doesn't evaluate its side effects if the ZStream is not completed (e.g. folded over)") {
        import scala.collection.mutable.{ Map => MMap }

        val targetState = MMap(
          2 -> "record for key 2",
          4 -> "record for key 4",
          6 -> "record for key 6"
        )

        val effectState = MMap[Int, String]()
        val updateState: (Int, MMap[Int, String]) => Int =
          (key: Int, state: MMap[Int, String]) => { state += (key -> s"record for key $key"); key }

        val sideEffectingSource1 =
          AkkaSource(List(1, 2, 3))
            .map(updateState(_, effectState))

        val sideEffectingSource2 =
          AkkaSource(List(2, 4, 6))
            .map(updateState(_, effectState))

        for {
          actorSystem     <- Task(ActorSystem("Test"))
          mat             <- Task(Materializer(actorSystem))
          (_, _)          <- akkaSourceAsZioStreamM(sideEffectingSource1)
          (zioStream2, _) <- akkaSourceAsZioStreamM(sideEffectingSource2)
          _               <- zioStream2.fold(0)(_ + _).provide(mat)
          _               <- Task(actorSystem.terminate())
        } yield assert(effectState)(equalTo(targetState))
      },
      testM("Materialised value is produced after stream is ran") {
        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          mat               <- Task(Materializer(actorSystem))
          (zioStream, m)    <- akkaSourceAsZioStreamM(testSource)
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provide(mat)
          materialisedValue <- m
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is produced even if stream evaluation begins after materialised value evaluation") {
        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          mat               <- Task(Materializer(actorSystem))
          (zioStream, m)    <- akkaSourceAsZioStreamM(testSource)
          fibre             <- m.fork
          output            <- zioStream.map(_ * 2).fold(0)(_ + _).provide(mat)
          materialisedValue <- fibre.join
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(equalTo(110)) &&
          assert(materialisedValue)(equalTo(5))
      },
      testM("Materialised value is never produced if the matching stream is not evaluated") {
        for {
          actorSystem <- Task(ActorSystem("Test"))
          testSource  <- Task(AkkaSource(1 to 10).mapMaterializedValue(_ => 5))
          (_, m)      <- akkaSourceAsZioStreamM(testSource)
          _           <- m
          _           <- Task(actorSystem.terminate())
        } yield assert(false)(isTrue) // test will fail if completes
      } @@ forked @@ nonTermination(5.seconds),
      testM("Materialsied source value will be successfully evaluated even if the stream fails with an exception") {

        for {
          actorSystem       <- Task(ActorSystem("Test"))
          testSource        <- Task(AkkaSource(-1 to 1).map(5 / _).mapMaterializedValue(_ => 5))
          mat               <- Task(Materializer(actorSystem))
          (zioStream, m)    <- akkaSourceAsZioStreamM(testSource)
          output            <- zioStream.fold(0)(_ + _).either.provide(mat)
          materialisedValue <- m
          _                 <- Task(actorSystem.terminate())
        } yield assert(output)(
          isSubtype[Either[ArithmeticException, Int]](anything) &&
            isLeft(hasMessage(equalTo("/ by zero")))
        ) && assert(materialisedValue)(equalTo(5))
      }
    )
}
