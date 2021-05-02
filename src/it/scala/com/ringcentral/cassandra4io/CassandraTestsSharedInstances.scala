package com.ringcentral.cassandra4io

import java.net.InetSocketAddress

import cats.effect.{ IO, Resource }
import cats.implicits.catsSyntaxApplicative
import com.dimafeng.testcontainers.CassandraContainer
import weaver.IOSuite
import cats.syntax.foldable._

import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{ CqlSession, CqlSessionBuilder }
import com.ringcentral.cassandra4io.utils.JavaConcurrentToCats.fromJavaAsync

import scala.io.BufferedSource

trait CassandraTestsSharedInstances { self: IOSuite =>

  val keyspace  = "cassandra4io"
  val container = CassandraContainer("cassandra:3.11.8")

  def migrateSession(session: CassandraSession[IO]): IO[Unit] = {
    val migrationSource = IO.blocking(scala.io.Source.fromResource("migration/1__test_tables.cql"))
    for {
      _         <- session.execute(s"use $keyspace")
      source    <- migrationSource
      migrations = splitToMigrations(source)
      _         <- migrations.toList.traverse_ { migration =>
                     val st = SimpleStatement.newInstance(migration)
                     session.execute(st)
                   }
    } yield ()
  }

  def ensureKeyspaceExists(builder: CqlSessionBuilder): IO[Unit] =
    for {
      session <- fromJavaAsync(builder.withKeyspace(Option.empty[String].orNull).buildAsync())
      _       <-
        fromJavaAsync(
          session.executeAsync(
            s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
          )
        ).unlessA(session.getMetadata.getKeyspace(keyspace).isPresent)
      _       <- fromJavaAsync(session.closeAsync())
    } yield ()

  override type Res = CassandraSession[IO]
  override def sharedResource: Resource[IO, Res] =
    Resource
      .make(IO.blocking {
        container.start()
      })(_ => IO.blocking(container.stop()))
      .flatMap { _ =>
        val builder = CqlSession
          .builder()
          .addContactPoint(InetSocketAddress.createUnresolved(container.host, container.mappedPort(9042)))
          .withLocalDatacenter("datacenter1")
          .withKeyspace(keyspace)
        Resource.liftK(ensureKeyspaceExists(builder)).flatMap(_ => CassandraSession.connect[IO](builder))
      }
      .evalTap(migrateSession)

  private def splitToMigrations(source: BufferedSource): Seq[String] = {
    val s1 = source
      .getLines()
      .toList
      .filterNot { line =>
        val l = line.stripLeading()
        l.startsWith("//") || l.startsWith("--")
      }
      .mkString("")
    s1.split(';').toList.map(_.strip())
  }
}
