# ZIO Wrappers for Akka Streams

[![CircleCI](https://circleci.com/gh/Ewan-Keith/zio-akka-streams-interop/tree/master.svg?style=svg)](https://circleci.com/gh/Ewan-Keith/zio-akka-streams-interop/tree/master)

This library is a [ZIO](https://github.com/zio/zio) wrapper for [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html).
It provides helper methods for describing basic Akka Streams stages/graphs as ZIO effects.

This table gives the supported conversions from Akka Streams components to ZIO effects.

| Akka Streams               | ZIO (with Materialized value)                              | ZIO (without Materialized value)                             |
|----------------------------|------------------------------------------------------------|--------------------------------------------------------------|
| `RunnableGraph[Future[M]]` | `ZIO[Has[Materializer], Throwable, M]`                     | `N/A`                                                        |
| `Graph[SourceShape[A], M]` | `UIO[(ZStream[Has[Materializer], Throwable, A], Task[M])]` | `UIO[(ZStream[Has[Materializer], Throwable, A], Task[M])]`   |
| `Graph[SinkShape[A], M]`   | `UIO[ZSink[Has[Materializer], Throwable, Nothing, A, M]]`  | `UIO[ZSink[Has[Materializer], Throwable, Nothing, A, Unit]]` |


## Add the dependency

To use `zio-akka-streams-interop`, add the following line in your `build.sbt` file:

```
libraryDependencies += "com.github.ewan-keith" %% "zio-akka-streams-interop" % "0.1.0"
```

## How to use

In order to evaluate an Akka Stream as a ZIO effect, you need to provide an explicit `Materializer`. See the [Akka Documentation](https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html#stream-materialization) or the examples below for help.

All methods described below can be imported as follows:

```scala
import com.github.ekeith.zio.akkastream.Converters._
```

### RunnableGraph

The simplest way to run an Akka Streams component in a ZIO application is to first construct a full Akka Streams `RunnableGraph[Future[M]]`,
and then to evaluate the full Graph as a ZIO effect. It's important that the `Future[M]` value is materialised by the Graph as this is the
only way for the graph to communicate its completion (successful or unsuccessful) to ZIO.

A Graph of this type can be wrapped as `ZIO[Has[Materializer], Throwable, M]` with the method imported below:

```scala
import com.github.ekeith.zio.akkastream.Converters.runnableGraphAsZioEffect
```

The Graph will then be evaluated when the ZIO effect is.

**Example:**

```scala
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{ Keep, RunnableGraph, Sink, Source }
import com.github.ekeith.zio.akkastream.Converters.runnableGraphAsZioEffect
import scala.concurrent.Future
import zio.{ Has, Layer, Managed, Task, ZLayer }

val actorSystem: Layer[Throwable, Has[ActorSystem]] =
  ZLayer.fromManaged(Managed.make(Task(ActorSystem("Test")))(sys => Task.fromFuture(_ => sys.terminate()).either))

val materializerLayer: Layer[Throwable, Has[Materializer]] =
  actorSystem >>> ZLayer.fromFunction(as => Materializer(as.get))

val sink: Sink[Int, Future[Int]]         = Sink.fold[Int, Int](0)(_ + _)
val runnable: RunnableGraph[Future[Int]] = Source(1 to 10).toMat(sink)(Keep.right)

for {
  materialisedValue <- runnableGraphAsZioEffect(runnable).provideLayer(materializerLayer)
} yield materialisedValue   // 55
```

### Source

An Akka Streams `Source[A, M]` can be expressed as a ZIO effect in 2 ways, depending on whether the `Source` materialised value
should be surfaced from the effect or if it can be discarded. Either the `akkaSourceAsZioStream` or `akkaSourceAsZioStreamMat`
methods can be used depending on whether the materialised value should be surfaced or not. Both methods return a `ZStream`
outputting the output values from the Akka Streams `Source`.

See the table at the top of this page for the exact return signatures for each converter method.

**Example**

```scala
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.github.ekeith.zio.akkastream.Converters.akkaSourceAsZioStreamMat
import zio.{ Has, Layer, Managed, Task, ZLayer }

val actorSystem: Layer[Throwable, Has[ActorSystem]] =
  ZLayer.fromManaged(Managed.make(Task(ActorSystem("Test")))(sys => Task.fromFuture(_ => sys.terminate()).either))

val materializerLayer: Layer[Throwable, Has[Materializer]] =
  actorSystem >>> ZLayer.fromFunction(as => Materializer(as.get))

for {
  testSource        <- Task(Source(1 to 10).mapMaterializedValue(_ => 5))
  (zioStream, m)    <- akkaSourceAsZioStreamMat(testSource)
  output            <- zioStream.map(_ * 2).fold(0)(_ + _).provideLayer(materializerLayer)
  materialisedValue <- m
} yield (output, materialisedValue)   // (110, 5)
```

### Sink

An Akka Streams `Sink[A, M]` can be expressed as a ZIO effect in 2 ways, depending on whether the `Sink` materialised value
should be surfaced from the effect or if it can be discarded. Either the `akkaSinkAsZioSink` or `akkaSinkAsZioSinkMat`
methods can be used depending on whether the materialised value should be surfaced or not. Both methods return a `ZSink`
that accepts values and provides them to the underlying Akka STreams `Sink`.

See the table at the top of this page for the exact return signatures for each converter method.

**Example**
```scala
import akka.actor.ActorSystem
import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.github.ekeith.zio.akkastream.Converters.akkaSinkAsZioSinkMat
import scala.concurrent.Future
import zio.{ Has, Layer, Managed, Task, ZLayer, ZIO }
import zio.stream.ZStream
import scala.collection.mutable.{ Map => MMap }

val actorSystem: Layer[Throwable, Has[ActorSystem]] =
  ZLayer.fromManaged(Managed.make(Task(ActorSystem("Test")))(sys => Task.fromFuture(_ => sys.terminate()).either))

val materializerLayer: Layer[Throwable, Has[Materializer]] =
  actorSystem >>> ZLayer.fromFunction(as => Materializer(as.get))

/** mutable map used to test side effecting stream stages */
case class TestState() {
  private val state = MMap[Int, String]()

  def updateState(key: Int): Int  = { this.state += (key -> s"record for key $key"); key }
  def getState: MMap[Int, String] = this.state
}

val testState = TestState()

val sideEffectingSink: Sink[Int, Future[Done]] = Sink.foreach({ i => testState.updateState(i); () })
val testStream = ZStream.fromIterable(List(1, 2, 3))

for {
  sink               <- akkaSinkAsZioSinkMat(sideEffectingSink)
  materialisedFuture <- testStream.run(sink).provideLayer(materializerLayer)
  materialisedValue  <- ZIO.fromFuture(_ => materialisedFuture)
} yield (testState.getState, materialisedValue) 
// (MMap(1 -> "record for key 1", 2 -> "record for key 2", 3 -> "record for key 3"), Done)
```

## Shout-out

When putting this library together I've used the great [zio-akka-cluster](https://github.com/zio/zio-akka-cluster) library as a template for documentation, CI and deployment.