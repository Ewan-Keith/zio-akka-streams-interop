package org.ekeith.zio.akka.stream

import akka.stream.Materializer
import akka.stream.scaladsl.RunnableGraph
import zio.{Task, ZIO}

import scala.concurrent.Future

object Converters {

  /**
   * Convert a RunnableGraph to a ZIO Task dependent on an Akka Streams Materializer.
   *
   * @param graph An akka-streams RunnableGraph.
   *
   * @tparam M The type of the materialized value returned when the graph completes running.
   * @return A ZIO Task, dependent on an Akka Streams Materializer that runs the provided graph.
   */
  def runnableGraphAsTask[M](graph: RunnableGraph[Future[M]]): ZIO[Materializer, Throwable, M] = {
    for {
      mat <- ZIO.environment[Materializer]
      materialisedFuture <- Task(graph.run()(mat))
      materialisedValue <- ZIO.fromFuture(implicit ec => materialisedFuture)
    } yield materialisedValue
  }

}
