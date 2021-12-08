package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.ringcentral.cassandra4io.CassandraTestsSharedInstances
import fs2.Stream
import weaver._

import java.time.{ Duration, LocalDate, LocalTime }
import java.util.UUID

trait CqlSuite { self: IOSuite with CassandraTestsSharedInstances =>

  case class Data(id: Long, data: String)

  case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])
  object BasicInfo {
    implicit val cqlReads: Reads[BasicInfo]   = FromUdtValue.deriveReads[BasicInfo]
    implicit val cqlBinder: Binder[BasicInfo] = ToUdtValue.deriveBinder[BasicInfo]
  }

  case class PersonAttribute(personId: Int, info: BasicInfo)

  case class CollectionTestRow(id: Int, maptest: Map[String, UUID], settest: Set[Int], listtest: List[LocalDate])

  case class ExampleType(x: Long, y: Long, date: LocalDate, time: LocalTime)

  case class ExampleNestedType(a: Int, b: String, c: ExampleType)

  case class ExampleCollectionNestedUdtType(a: Int, b: Map[Int, Set[Set[Set[Set[ExampleNestedType]]]]])
  object ExampleCollectionNestedUdtType {
    implicit val binderExampleCollectionNestedUdtType: Binder[ExampleCollectionNestedUdtType] =
      ToUdtValue.deriveBinder[ExampleCollectionNestedUdtType]

    implicit val readsExampleCollectionNestedUdtType: Reads[ExampleCollectionNestedUdtType] =
      FromUdtValue.deriveReads[ExampleCollectionNestedUdtType]
  }

  case class ExampleNestedPrimitiveType(a: Int, b: Map[Int, Set[Set[Set[Set[Int]]]]])
  object ExampleNestedPrimitiveType {
    implicit val binderExampleNestedPrimitiveType: Binder[ExampleNestedPrimitiveType] =
      ToUdtValue.deriveBinder[ExampleNestedPrimitiveType]

    implicit val readsExampleNestedPrimitiveType: Reads[ExampleNestedPrimitiveType] =
      FromUdtValue.deriveReads[ExampleNestedPrimitiveType]
  }

  case class TableContainingExampleCollectionNestedUdtType(id: Int, data: ExampleCollectionNestedUdtType)

  case class TableContainingExampleNestedPrimitiveType(id: Int, data: ExampleNestedPrimitiveType)

  test("interpolated select template should return data from migration") { session =>
    for {
      prepared <- cqlt"select data FROM cassandra4io.test_data WHERE id in ${Put[List[Long]]}"
                    .as[String]
                    .config(_.setTimeout(Duration.ofSeconds(1)))
                    .prepare(session)
      query     = prepared(List[Long](1, 2, 3))
      results  <- query.select.compile.toList
    } yield expect(results == Seq("one", "two", "three"))
  }

  test("interpolated select template should return tuples from migration") { session =>
    for {
      prepared <- cqlt"select id, data FROM cassandra4io.test_data WHERE id in ${Put[List[Long]]}"
                    .as[(Long, String)]
                    .prepare(session)
      query     = prepared(List[Long](1, 2, 3))
      results  <- query.select.compile.toList
    } yield expect(results == Seq((1, "one"), (2, "two"), (3, "three")))
  }

  test("interpolated select template should return tuples from migration with multiple binding") { session =>
    for {
      query   <-
        cqlt"select data FROM cassandra4io.test_data_multiple_keys WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}"
          .as[String]
          .prepare(session)
      results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.compile.toList
    } yield expect(results == Seq("one-two"))
  }

  test("interpolated select template should return tuples from migration with multiple binding and margin stripped") {
    session =>
      for {
        query   <- cqlt"""select data FROM cassandra4io.test_data_multiple_keys
                       |WHERE id1 = ${Put[Long]} and id2 = ${Put[Int]}""".stripMargin.as[String].prepare(session)
        results <- query(1L, 2).config(_.setExecutionProfileName("default")).select.compile.toList
      } yield expect(results == Seq("one-two"))
  }

  test("interpolated select template should return data case class from migration") { session =>
    for {
      prepared <-
        cqlt"select id, data FROM cassandra4io.test_data WHERE id in ${Put[List[Long]]}".as[Data].prepare(session)
      query     = prepared(List[Long](1, 2, 3))
      results  <- query.select.compile.toList
    } yield expect(results == Seq(Data(1, "one"), Data(2, "two"), Data(3, "three")))
  }

  test("interpolated select template should be reusable") { session =>
    for {
      query  <- cqlt"select data FROM cassandra4io.test_data WHERE id = ${Put[Long]}".as[String].prepare(session)
      result <- Stream.emits(Seq(1L, 2L, 3L)).flatMap(i => query(i).select).compile.toList
    } yield expect(result == Seq("one", "two", "three"))
  }

  test("interpolated select should return data from migration") { session =>
    def getDataByIds(ids: List[Long]) =
      cql"select data FROM cassandra4io.test_data WHERE id in $ids"
        .as[String]
        .config(_.setConsistencyLevel(ConsistencyLevel.ALL))
    for {
      results <- getDataByIds(List(1, 2, 3)).select(session).compile.toList
    } yield expect(results == Seq("one", "two", "three"))
  }

  test("interpolated select should return tuples from migration") { session =>
    def getAllByIds(ids: List[Long]) =
      cql"select id, data FROM cassandra4io.test_data WHERE id in $ids".as[(Long, String)]
    for {
      results <- getAllByIds(List(1, 2, 3)).config(_.setQueryTimestamp(0L)).select(session).compile.toList
    } yield expect(results == Seq((1, "one"), (2, "two"), (3, "three")))
  }

  test("interpolated select should return tuples from migration with multiple binding") { session =>
    def getAllByIds(id1: Long, id2: Int) =
      cql"select data FROM cassandra4io.test_data_multiple_keys WHERE id1 = $id1 and id2 = $id2".as[String]
    for {
      results <- getAllByIds(1, 2).select(session).compile.toList
    } yield expect(results == Seq("one-two"))
  }

  test("interpolated select should return tuples from migration with multiple binding and margin stripped") { session =>
    def getAllByIds(id1: Long, id2: Int) =
      cql"""select data FROM cassandra4io.test_data_multiple_keys
           |WHERE id1 = $id1 and id2 = $id2""".stripMargin.as[String]
    for {
      results <- getAllByIds(1, 2).select(session).compile.toList
    } yield expect(results == Seq("one-two"))
  }

  test("interpolated select should return data case class from migration") { session =>
    def getIds(ids: List[Long]) =
      cql"select id, data FROM cassandra4io.test_data WHERE id in $ids".as[Data]
    for {
      results <- getIds(List(1, 2, 3)).select(session).compile.toList
    } yield expect(results == Seq(Data(1, "one"), Data(2, "two"), Data(3, "three")))
  }

  test(
    "interpolated inserts and selects should produce UDTs and return data case classes when nested case classes are used"
  ) { session =>
    val data   = PersonAttribute(1, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
    val insert =
      cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
        .execute(session)

    val retrieve = cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
      .as[PersonAttribute]
      .select(session)
      .compile
      .toList

    for {
      _      <- insert
      result <- retrieve
    } yield expect(result.length == 1 && result.head == data)
  }

  test("interpolated inserts and selects should handle cassandra collections") { session =>
    val data = CollectionTestRow(1, Map("2" -> UUID.randomUUID()), Set(1, 2, 3), List(LocalDate.now()))

    val insert =
      cql"INSERT INTO cassandra4io.test_collection (id, maptest, settest, listtest) VALUES (${data.id}, ${data.maptest}, ${data.settest}, ${data.listtest})"
        .execute(session)

    val retrieve =
      cql"SELECT id, maptest, settest, listtest FROM cassandra4io.test_collection WHERE id = ${data.id}"
        .as[CollectionTestRow]
        .select(session)
        .compile
        .toList

    for {
      _      <- insert
      result <- retrieve
    } yield expect(result.length == 1 && result.head == data)
  }

  test("interpolated inserts and selects should handle nested UDTs in heavily nested collections") { session =>
    val row    = TableContainingExampleCollectionNestedUdtType(
      id = 1,
      data = ExampleCollectionNestedUdtType(
        a = 2,
        b = Map(
          1 -> Set(
            Set(
              Set(
                Set(
                  ExampleNestedType(
                    a = 3,
                    b = "4",
                    c = ExampleType(x = 5L, y = 6L, date = LocalDate.now(), time = LocalTime.now())
                  )
                )
              )
            )
          ),
          2 -> Set(
            Set(
              Set(
                Set(
                  ExampleNestedType(
                    a = 10,
                    b = "100",
                    c = ExampleType(x = 105L, y = 106L, date = LocalDate.now(), time = LocalTime.now())
                  )
                )
              )
            )
          )
        )
      )
    )
    val insert =
      cql"INSERT INTO cassandra4io.heavily_nested_udt_table (id, data) VALUES (${row.id}, ${row.data})".execute(session)

    val retrieve = cql"SELECT id, data FROM cassandra4io.heavily_nested_udt_table WHERE id = ${row.id}"
      .as[TableContainingExampleCollectionNestedUdtType]
      .select(session)
      .compile
      .toList

    for {
      _      <- insert
      actual <- retrieve
    } yield expect(actual.length == 1 && actual.head == row)
  }

  test("interpolated inserts and selects should handle UDTs and primitives in heavily nested collections") { session =>
    val row    = TableContainingExampleNestedPrimitiveType(
      id = 1,
      data = ExampleNestedPrimitiveType(
        a = 1,
        b = Map(
          1 -> Set(Set(Set(Set(2, 3), Set(4, 5)))),
          2 -> Set(Set(Set(Set(7, 8))))
        )
      )
    )
    val insert =
      cql"INSERT INTO cassandra4io.heavily_nested_prim_table (id, data) VALUES (${row.id}, ${row.data})".execute(
        session
      )

    val retrieve = cql"SELECT id, data FROM cassandra4io.heavily_nested_prim_table WHERE id = ${row.id}"
      .as[TableContainingExampleNestedPrimitiveType]
      .select(session)
      .compile
      .toList

    for {
      _      <- insert
      actual <- retrieve
    } yield expect(actual.length == 1 && actual.head == row)
  }

  test("interpolated select should bind constants") { session =>
    val query = cql"select data FROM cassandra4io.test_data WHERE id = ${1L}".as[String]
    for {
      result <- query.select(session).compile.toList
    } yield expect(result == Seq("one"))
  }

  test("cqlConst/cqlConst0 should allow you to interpolate on what is usually not possible with cql strings") {
    session =>
      val data         = PersonAttribute(2, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
      val keyspaceName = "cassandra4io"
      val tableName    = "person_attributes"
      val selectFrom   = cql"SELECT person_id, info FROM "
      val keyspace     = cqlConst0(s"$keyspaceName.")
      val table        = cqlConst(s"$tableName")

      def where(personId: Int) =
        cql"WHERE person_id = $personId"

      def insert(personAttribute: PersonAttribute) =
        (cql"INSERT INTO " ++ keyspace ++ table ++ cql"(person_id, info) VALUES (${data.personId}, ${data.info})")
          .execute(session)

      for {
        _      <- insert(data)
        result <- (selectFrom ++ keyspace ++ table ++ where(data.personId)).as[PersonAttribute].selectFirst(session)
      } yield expect(result.isDefined && result.get == data)
  }
}
