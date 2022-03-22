package com.ringcentral.cassandra4io

import java.net.InetSocketAddress

import cats.effect._
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException
import com.datastax.oss.driver.api.core.CqlSession
import weaver._

trait CassandraSessionSuite { self: IOSuite with CassandraTestsSharedInstances =>

  implicit def toStatement(s: String): SimpleStatement = SimpleStatement.newInstance(s)

  test("CassandraSession.connect be referentially transparent") { _ =>
    val testSession = CassandraSession.connect[IO](
      CqlSession
        .builder()
        .addContactPoint(InetSocketAddress.createUnresolved(container.host, container.mappedPort(9042)))
        .withLocalDatacenter("datacenter1")
    )
    val st          = SimpleStatement.newInstance(s"select cluster_name from system.local")
    for {
      r1 <- testSession.use(session => session.selectFirst(st).map(_.map(_.getString(0))))
      r2 <- testSession.use(session => session.selectFirst(st).map(_.map(_.getString(0))))
    } yield expect(r1 == r2)
  }

  test("prepare should return PreparedStatement") { session =>
    for {
      st <- session.prepare(s"select data FROM $keyspace.test_data WHERE id = :id")
    } yield expect(st.getQuery == s"select data FROM $keyspace.test_data WHERE id = :id")
  }

  test("prepare should return error on invalid request") { session =>
    for {
      result <- session.prepare(s"select column404 FROM $keyspace.test_data WHERE id = :id").attempt
      error   = getError(result)
    } yield expect(error.getMessage == "Undefined column name column404") &&
      expect(error.isInstanceOf[InvalidQueryException])
  }

  test("select should return prepared data") { session =>
    for {
      results <- session
                   .select(s"select data FROM $keyspace.test_data WHERE id IN (1,2,3)")
                   .map(_.getString(0))
                   .compile
                   .toList
    } yield expect(results == List("one", "two", "three"))
  }

  test("select should be pure stream") { session =>
    val selectStream = session
      .select(s"select data FROM $keyspace.test_data WHERE id IN (1,2,3)")
      .map(_.getString(0))
      .compile
      .toList
    for {
      _       <- selectStream
      results <- selectStream
    } yield expect(results == List("one", "two", "three"))
  }

  test("selectOne should return None on empty result") { session =>
    for {
      result <- session
                  .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 404")
                  .map(_.map(_.getString(0)))
    } yield expect(result.isEmpty)
  }

  test("selectOne should return Some for one") { session =>
    for {
      result <- session
                  .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 1")
                  .map(_.map(_.getString(0)))
    } yield expect(result.contains("one"))
  }

  test("selectOne should return Some(null) for null") { session =>
    for {
      result <- session
                  .selectFirst(s"select data FROM $keyspace.test_data WHERE id = 0")
                  .map(_.map(_.getString(0)))
    } yield expect(result.contains(null))
  }

  test("select will emit in chunks sized equal to statement pageSize") { session =>
    val st = SimpleStatement.newInstance(s"select data from $keyspace.test_data").setPageSize(2)
    for {
      result <- session.select(st).map(_.getString(0)).chunks.compile.toList
    } yield expect(result.nonEmpty && result.map(_.size).forall(_ == 2))
  }

}
