package com.ringcentral.cassandra4io

import cats.effect.{ Async, Resource }
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metrics.Metrics
import com.ringcentral.cassandra4io.utils.JavaConcurrentToCats.fromJavaAsync
import fs2.{ Chunk, Pull, Stream }
import simulacrum.typeclass

import scala.jdk.CollectionConverters._

@typeclass
trait CassandraSession[F[_]] {
  def prepare(stmt: String): F[PreparedStatement]
  def execute(stmt: Statement[_]): F[AsyncResultSet]
  def execute(query: String): F[AsyncResultSet]
  def select(stmt: Statement[_]): Stream[F, Row]

  // short-cuts
  def selectFirst(stmt: Statement[_]): F[Option[Row]]

  // metrics
  def metrics: Option[Metrics]
}

object CassandraSession {

  private class Live[F[_]: Async](
    underlying: CqlSession
  ) extends CassandraSession[F] {

    def metrics: Option[Metrics] = underlying.getMetrics.asScala

    override def prepare(stmt: String): F[PreparedStatement] =
      fromJavaAsync(underlying.prepareAsync(stmt))

    override def execute(stmt: Statement[_]): F[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(stmt))

    override def select(stmt: Statement[_]): Stream[F, Row] = {
      def go(current: F[AsyncResultSet]): Pull[F, Row, Unit] =
        Pull
          .eval(current)
          .flatMap { rs =>
            val chunk = Chunk.iterable(rs.currentPage().asScala)

            if (rs.hasMorePages)
              Pull.output(chunk) >> go(fromJavaAsync(rs.fetchNextPage()))
            else Pull.output(chunk) >> Pull.done
          }
      go(execute(stmt)).stream
    }

    override def execute(query: String): F[AsyncResultSet] =
      fromJavaAsync(underlying.executeAsync(query))

    // short-cuts
    def selectFirst(stmt: Statement[_]): F[Option[Row]] =
      execute(stmt).map(rs => Option(rs.one()))
  }

  /**
   * Create CassandraSession from prepared CqlSessionBuilder
   *
   * @param builder prepared CqlSessionBuilder
   * @tparam F rabbit hole
   * @return Resource with CassandraSession, use it wisely
   */
  def connect[F[_]: Async](
    builder: CqlSessionBuilder
  ): Resource[F, CassandraSession[F]] =
    Resource
      .make[F, CqlSession](fromJavaAsync(builder.buildAsync()))(session => fromJavaAsync(session.closeAsync()).void)
      .map(cqlSession => new Live[F](cqlSession))
}
