package com.ringcentral.cassandra4io.cql

import cats.effect.IO
import cats.syntax.parallel._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.ringcentral.cassandra4io.CassandraTestsSharedInstances
import fs2.Stream
import weaver._

import java.time.{ Duration, LocalDate, LocalTime }
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

trait CqlSuite {
  self: IOSuite with CassandraTestsSharedInstances =>

  case class Data(id: Long, data: String)

  case class OptData(id: Long, data: Option[String])

  case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])

  object BasicInfo {
    implicit val cqlReads: Reads[BasicInfo]   = FromUdtValue.deriveReads[BasicInfo]
    implicit val cqlBinder: Binder[BasicInfo] = ToUdtValue.deriveBinder[BasicInfo]
  }

  case class PersonAttribute(personId: Int, info: BasicInfo)

  object PersonAttribute {
    val idxCounter = new AtomicInteger(0)
  }

  case class PersonAttributeOpt(personId: Int, info: Option[BasicInfo])

  case class OptBasicInfo(weight: Option[Double], height: Option[String], datapoints: Option[Set[Int]])

  object OptBasicInfo {
    implicit val cqlReads: Reads[OptBasicInfo]   = FromUdtValue.deriveReads[OptBasicInfo]
    implicit val cqlBinder: Binder[OptBasicInfo] = ToUdtValue.deriveBinder[OptBasicInfo]
  }

  case class PersonAttributeUdtOpt(personId: Int, info: OptBasicInfo)

  case class CollectionTestRow(
    id: Int,
    maptest: Map[String, UUID],
    settest: Set[Int],
    listtest: Option[List[LocalDate]]
  )

  case class ExampleType(x: Long, y: Long, date: LocalDate, time: Option[LocalTime])

  case class ExampleNestedType(a: Int, b: String, c: Option[ExampleType])

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
      prepared <- cqlt"select id, data, dataset FROM cassandra4io.test_data WHERE id in ${Put[List[Long]]}"
                    .as[(Long, String, Option[Set[Int]])]
                    .prepare(session)
      query     = prepared(List[Long](1, 2, 3))
      results  <- query.select.compile.toList
    } yield expect(results == Seq((1, "one", Some(Set.empty)), (2, "two", Some(Set(201))), (3, "three", None)))
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
    val data   =
      PersonAttribute(PersonAttribute.idxCounter.incrementAndGet(), BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
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
    val dataRow1 = CollectionTestRow(1, Map("2" -> UUID.randomUUID()), Set(1, 2, 3), Option(List(LocalDate.now())))
    val dataRow2 = CollectionTestRow(2, Map("3" -> UUID.randomUUID()), Set(4, 5, 6), None)

    def insert(data: CollectionTestRow): IO[Boolean] =
      cql"INSERT INTO cassandra4io.test_collection (id, maptest, settest, listtest) VALUES (${data.id}, ${data.maptest}, ${data.settest}, ${data.listtest})"
        .execute(session)

    def retrieve(id: Int, ids: Int*): IO[List[CollectionTestRow]] = {
      val allIds = id :: ids.toList
      cql"SELECT id, maptest, settest, listtest FROM cassandra4io.test_collection WHERE id IN $allIds"
        .as[CollectionTestRow]
        .select(session)
        .compile
        .toList
    }

    for {
      _    <- List(dataRow1, dataRow2).parTraverse(insert)
      res1 <- retrieve(dataRow1.id)
      res2 <- retrieve(dataRow2.id)
    } yield expect(res1.length == 1 && res1.head == dataRow1) and expect(res2.length == 1 && res2.head == dataRow2)
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
                    c = Option(ExampleType(x = 5L, y = 6L, date = LocalDate.now(), time = Option(LocalTime.now())))
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
                    c = Option(ExampleType(x = 105L, y = 106L, date = LocalDate.now(), time = None))
                  )
                )
              )
            )
          ),
          3 -> Set(
            Set(
              Set(
                Set(
                  ExampleNestedType(
                    a = 24,
                    b = "101",
                    c = None
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

  test("cqlConst allows you to interpolate on what is usually not possible with cql strings") { session =>
    val data         =
      PersonAttribute(PersonAttribute.idxCounter.incrementAndGet(), BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
    val keyspaceName = "cassandra4io"
    val tableName    = "person_attributes"
    val selectFrom   = cql"SELECT person_id, info FROM "
    val keyspace     = cqlConst"$keyspaceName."
    val table        = cqlConst"$tableName"

    def where(personId: Int) =
      cql" WHERE person_id = $personId"

    def insert(data: PersonAttribute) =
      (cql"INSERT INTO " ++ keyspace ++ table ++ cql" (person_id, info) VALUES (${data.personId}, ${data.info})")
        .execute(session)

    for {
      _      <- insert(data)
      result <- (selectFrom ++ keyspace ++ table ++ where(data.personId)).as[PersonAttribute].selectFirst(session)
    } yield expect(result.isDefined && result.get == data)
  }

  // handle NULL values
  test("decoding from null should return None for Option[String]") { session =>
    for {
      result <- cql"select data FROM cassandra4io.test_data WHERE id = 0".as[Option[String]].selectFirst(session)
    } yield expect(result.isDefined && result.get.isEmpty)
  }

  test("decoding from null should raise error for String(non-primitive)") { session =>
    for {
      result <-
        cql"select data FROM cassandra4io.test_data WHERE id = 0".as[String].selectFirst(session).attempt
    } yield expect(result.isLeft) && expect(
      getError(result).isInstanceOf[UnexpectedNullValue]
    )
  }

  test("decoding from null should raise error for Int(primitive)") { session =>
    for {
      result <-
        cql"select count FROM cassandra4io.test_data WHERE id = 0".as[String].selectFirst(session).attempt
    } yield expect(result.isLeft) && expect(
      getError(result).isInstanceOf[UnexpectedNullValue]
    )
  }

  test("decoding from null should raise error for Set(collection)") { session =>
    for {
      result <-
        cql"select dataset FROM cassandra4io.test_data WHERE id = 0".as[Set[Int]].selectFirst(session).attempt
    } yield expect(result.isLeft) && expect(
      getError(result).isInstanceOf[UnexpectedNullValue]
    )
  }

  test("decoding from null should return None for Option[String] field in case class") { session =>
    for {
      row <- cql"select id, data FROM cassandra4io.test_data WHERE id = 0".as[OptData].selectFirst(session)
    } yield expect(row.isDefined && row.get.data.isEmpty)
  }

  test("decoding from null should return None for optional case class first parameter") { session =>
    case class OptDataReverse(data: Option[String], id: Long)
    for {
      row <- cql"select data, id FROM cassandra4io.test_data WHERE id = 0".as[OptDataReverse].selectFirst(session)
    } yield expect(row.isDefined && row.get.data.isEmpty)
  }

  test("decoding from null should raise error String field in case class") { session =>
    for {
      result <- cql"select id, data FROM cassandra4io.test_data WHERE id = 0".as[Data].selectFirst(session).attempt
    } yield expect(result.isLeft) && expect(getError(result).isInstanceOf[UnexpectedNullValue])
  }

  test("nullable field should be correctly encoded in inserts") { session =>
    val id                   = 5L
    val data: Option[String] = None
    for {
      result <- cql"insert into cassandra4io.test_data (id, data) values ($id, $data)".execute(session).attempt
    } yield expect(result.isRight) && expect(result.contains(true))
  }

  test("nullable fields should be correctly set with 'usingUnset'") { session =>
    case class TestData(id: Long, data: Option[String], count: Option[Int])
    val id    = 111L
    val data1 = TestData(id, Some("test"), Some(15))
    val data2 = TestData(id, None, None)

    // This test looks a bit awkward.
    // It's because there is no easy way to differentiate between a null and empty field.
    for {
      insertResult1 <-
        cql"insert into cassandra4io.test_data (id, data, count) values (${data1.id}, ${data1.data}, ${data1.count})"
          .execute(session)
          .attempt
      selectResult1 <-
        cql"select id, data, count from cassandra4io.test_data where id = $id".as[TestData].selectFirst(session)
      insertResult2 <-
        cql"insert into cassandra4io.test_data (id, data, count) values (${data2.id}, ${data2.data}, ${data2.count.usingUnset})"
          .execute(session)
          .attempt
      selectResult2 <-
        cql"select id, data, count from cassandra4io.test_data where id = $id".as[TestData].selectFirst(session)
    } yield expect(insertResult1.contains(true)) &&
      expect(insertResult2.contains(true)) &&
      expect(selectResult1.contains(data1)) &&
      expect(selectResult2.contains(data2.copy(count = data1.count)))
  }

  // handle NULL values for udt columns

  test("decoding from null at udt column should return None for Option type") { session =>
    val data = PersonAttributeOpt(PersonAttribute.idxCounter.incrementAndGet(), None)

    for {
      _      <- cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
                  .execute(session)
      result <- cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
                  .as[PersonAttributeOpt]
                  .select(session)
                  .compile
                  .toList
    } yield expect(result.length == 1 && result.head == data)
  }

  test("decoding from null at udt column should raise Error for non Option type") { session =>
    val data = PersonAttributeOpt(PersonAttribute.idxCounter.incrementAndGet(), None)

    for {
      _      <- cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
                  .execute(session)
      result <- cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
                  .as[PersonAttribute]
                  .selectFirst(session)
                  .attempt
    } yield expect(result.isLeft) && expect(getError(result).isInstanceOf[UnexpectedNullValue])
  }

  // handle NULL inside udt

  test("decoding from null at udt field should return None for Option type") { session =>
    val data = PersonAttributeUdtOpt(
      PersonAttribute.idxCounter.incrementAndGet(),
      OptBasicInfo(None, None, None)
    )

    for {
      _      <- cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
                  .execute(session)
      result <- cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
                  .as[PersonAttributeUdtOpt]
                  .selectFirst(session)
    } yield expect(result.contains(data))
  }

  test("decoding from null at udt field should raise error for String(non-primitive)") { session =>
    val data =
      PersonAttributeUdtOpt(
        PersonAttribute.idxCounter.incrementAndGet(),
        OptBasicInfo(Some(160.0), None, Some(Set(1)))
      )
    for {
      _      <-
        cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
          .execute(session)
      result <-
        cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
          .as[PersonAttribute]
          .selectFirst(session)
          .attempt
    } yield expect(result.isLeft) && expect(getError(result).isInstanceOf[UnexpectedNullValue])
  }

  test("decoding from null at udt field should raise error for Double(primitive)") { session =>
    val data =
      PersonAttributeUdtOpt(
        PersonAttribute.idxCounter.incrementAndGet(),
        OptBasicInfo(None, Some("tall"), Some(Set(1)))
      )
    for {
      _      <-
        cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
          .execute(session)
      result <-
        cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
          .as[PersonAttribute]
          .selectFirst(session)
          .attempt
    } yield expect(result.isLeft) && expect(getError(result).isInstanceOf[UnexpectedNullValue])
  }

  test("decoding from null at udt field should raise error for Set(collection)") { session =>
    val data =
      PersonAttributeUdtOpt(
        PersonAttribute.idxCounter.incrementAndGet(),
        OptBasicInfo(Some(180.0), Some("tall"), None)
      )
    for {
      _      <-
        cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
          .execute(session)
      result <-
        cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
          .as[PersonAttribute]
          .selectFirst(session)
          .attempt
    } yield expect(result.isLeft) && expect(getError(result).isInstanceOf[UnexpectedNullValue])
  }

}
