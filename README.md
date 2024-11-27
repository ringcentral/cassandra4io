# Cassandra 4 io

![CI](https://github.com/ringcentral/cassandra4io/workflows/CI/badge.svg?branch=main)
![Maven Central](https://img.shields.io/maven-central/v/com.ringcentral/cassandra4io_2.13)

This is lightweight cats-effect and fs2 IO wrapper for latest datastax 4.x driver.

Why 4.x?
 
4.x was re-written in immutable first design, within async first API, 
optimizations, fewer allocations, metrics improvements, and fully compatible with cassandra 3.x

## Goals
- Be safe, type-safe.
- Be fast 
    - use minimal allocations
    - minimize resources and abstractions overhead over original datastax driver, which is good


## How to use
Cassandra4io is currently available for Scala 2.13 and 2.12.

### Add a dependency to your project
```scala
libraryDependencies += ("com.ringcentral" %% "cassandra4io" % "0.1.14")
```

### Create a connection to Cassandra
```scala
import com.ringcentral.cassandra4io.CassandraSession

import com.datastax.oss.driver.api.core.CqlSession
import cats.effect._

import java.net.InetSocketAddress

val builder = CqlSession
      .builder()
      .addContactPoint(InetSocketAddress.createUnresolved("localhost", 9042))
      .withLocalDatacenter("datacenter1")
      .withKeyspace("awesome") 

def makeSession[F[_]: Async]: Resource[F, CassandraSession[F]] =
  CassandraSession.connect(builder)
```

### Write some requests

package `com.ringcentral.cassandra4io.cql` introduces typed way to deal with cql queries

### Simple syntax

```scala
import cats.effect.Sync
import cats.syntax.all._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._

case class Model(id: Int, data: String)

trait Dao[F[_]] {
  def put(value: Model): F[Unit]
  def get(id: Int): F[Option[Model]]
}

class DaoImpl[F[_]: Async](session: CassandraSession[F]) extends Dao[F] {

  private def insertQuery(value: Model) =
    cql"insert into table (id, data) values (${value.id}, ${value.data})"
      .config(_.setConsistencyLevel(ConsistencyLevel.ALL))

  private def selectQuery(id: Int) =
    cql"select id, data from table where id = $id".as[Model]
  
  override def put(value: Model) = insertQuery(value).execute(session).void
  override def get(id: Int) = selectQuery(id).select(session).head.compile.last
}
```

this syntax reuse implicit driver prepared statements cache
  
### Templated syntax

```scala
import cats.effect._
import scala.concurrent.duration._
import cats.syntax.all._
import scala.jdk.DurationConverters._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._

case class Model(pk: Long, ck: String, data: String, metaData: String)
case class Key(pk: Long, ck: String)
case class Data(data: String, metaData: String)

trait Dao[F[_]] {
  def put(value: Model): F[Unit]
  def update(key: Key, data: Data): F[Unit]
  def get(key: Key): F[Option[Model]]
}

object Dao {

  private val tableName = "table"
  private val insertQuery =
    cqlt"insert into ${Const(tableName)} (pk, ck, data, meta_data) values (${Put[Long]}, ${Put[String]}, ${Put[String]}, ${Put[String]})"
      .config(_.setTimeout(1.second.toJava))
  private val insertQueryAlternative =
    cqlt"insert into ${Const(tableName)} (${Columns[Model]}) values (${Values[Model]})"
  private val updateQuery = cqlt"update ${Const(tableName)} set ${Assignment[Data]} where ${EqualsTo[Key]}"
  private val selectQuery = cqlt"select ${Columns[Model]} from ${Const(tableName)} where ${EqualsTo[Key]}".as[Model]

  def apply[F[_] : Async](session: CassandraSession[F]) = for {
    insert <- insertQuery.prepare(session)
    update <- updateQuery.prepare(session)
    updateAlternative <- insertQueryAlternative.prepare(session)
    select <- selectQuery.prepare(session)
  } yield new Dao[F] {
    override def put(value: Model) = insert(
      value.pk,
      value.ck,
      value.data,
      value.metaData
    ).execute.void // updateAlternative(value).execute.void
    override def update(key: Key, data: Data): F[Unit] = updateQuery(data, key).execute.void
    override def get(key: Key) = select(key).config(_.setExecutionProfileName("default")).select.head.compile.last
  }
}
```
As you can see `${Columns[Model]}` expands to `pk, ck, data, meta_data`, `${Values[Model]}` to `?, ?, ?, ?`, `${Assignment[Data]}` to `pk = ?, ck = ?, data = ?, meta_data = ?` and `${EqualsTo[Key]}` expands to `pk = ? and ck = ? and data = ? and meta_data = ?`.
Latter three types adjust query type as well for being able to bind corresponding values 

### Handling optional fields (`null`)

By default, cassandra4io encodes `Option` as a `null` value. Which is ok for most cases. But in Cassandra, there is a difference between a `null` value and an empty value. In java driver this difference is represented by `BoundStatement#setToNull` (default behavior) and `BoundStatement#unset` (setting an empty field). The main advantage of using `unset` instead of `setToNull` is that tombstone will not be created for an empty field.

To use the `unset` instead of the `setToNull` for your optional value in a `cql` interpolators you could add `.usingUnset` to your optional value. Like in the following example:
```scala
import com.ringcentral.cassandra4io.cql._

cql"insert into entities(foo, bar, baz) values (${e.foo}, ${e.bar}, ${e.baz.usingUnset}"
```

## User Defined Type (UDT) support

Cassandra4IO provides support for Cassandra's User Defined Type (UDT) values. 
For example, given the following Cassandra schema:

```cql
create type basic_info(
    weight double,
    height text,
    datapoints frozen<set<int>>
);

create table person_attributes(
    person_id int,
    info frozen<basic_info>,
    PRIMARY KEY (person_id)
);
```

**Note:** `frozen` means immutable

Here is how to insert and select data from the `person_attributes` table:

```scala
final case class BasicInfo(weight: Double, height: String, datapoints: Set[Int])
object BasicInfo {
  implicit val cqlReads: Reads[BasicInfo]   = FromUdtValue.deriveReads[BasicInfo]
  implicit val cqlBinder: Binder[BasicInfo] = ToUdtValue.deriveBinder[BasicInfo]
}

final case class PersonAttribute(personId: Int, info: BasicInfo)
```

We provide a set of typeclasses (`FromUdtValue` and `ToUDtValue`) under the hood that automatically convert your Scala 
types into types that Cassandra can understand without having to manually convert your data-types into Datastax Java 
driver's `UdtValue`s. 

```scala
class UDTUsageExample[F[_]: Async](session: CassandraSession[F]) {
  val data = PersonAttribute(1, BasicInfo(180.0, "tall", Set(1, 2, 3, 4, 5)))
  val insert: F[Boolean] =
    cql"INSERT INTO cassandra4io.person_attributes (person_id, info) VALUES (${data.personId}, ${data.info})"
            .execute(session)

  val retrieve: fs2.Stream[F, PersonAttribute] = 
    cql"SELECT person_id, info FROM cassandra4io.person_attributes WHERE person_id = ${data.personId}"
            .as[PersonAttribute]
            .select(session)
}
```

### More control over the transformation process of `UdtValue`s

If you wanted to have additional control into how you map data-types to and from Cassandra rather than using `FromUdtValue`
& `ToUdtValue`, we expose the Datastax Java driver API to you for full control. Here is an example using `BasicInfo`:

```scala
object BasicInfo {
  implicit val cqlReads: Reads[BasicInfo] = Reads[UdtValue].map { udtValue =>
    BasicInfo(
      weight = udtValue.getDouble("weight"),
      height = udtValue.getString("height"),
      datapoints = udtValue
        .getSet[java.lang.Integer]("datapoints", classOf[java.lang.Integer])
        .asScala
        .toSet
        .map { int: java.lang.Integer => Int.unbox(int) }
    )
  }

  implicit val cqlBinder: Binder[BasicInfo] = Binder[UdtValue].contramapUDT { (info, constructor) =>
    constructor
      .newValue()
      .setDouble("weight", info.weight)
      .setString("height", info.height)
      .setSet("datapoints", info.datapoints.map(Int.box).asJava, classOf[java.lang.Integer])
  }
}
```

Please note that we recommend using `FromUdtValue` and `ToUdtValue` to automatically derive this hand-written (and error-prone) 
code. 

## Interpolating on CQL parameters

Cassandra4IO Allows you to interpolate (i.e. using string interpolation) on values that are not valid CQL parameters using 
`++` or `concat` to build out your CQL query. For example, you can interpolate on the keyspace and table name using 
the `cqlConst` interpolator like so:

```scala
val session: CassandraSession[IO] = ???
val keyspaceName = "cassandra4io"
val tableName    = "person_attributes"
val keyspace     = cqlConst"$keyspaceName."
val table        = cqlConst"$tableName"

def insert(data: PersonAttribute) = 
  (cql"INSERT INTO " ++ keyspace ++ table ++ cql" (person_id, info) VALUES (${data.personId}, ${data.info})")
    .execute(session)
```

This allows you (the author of the application) to feed in parameters like the table name and keyspace through 
configuration. Please be aware that you should not be taking your user's input and feeding this into `cqlConst` as 
this will pose an injection risk.

## References
- [Datastax Java driver](https://docs.datastax.com/en/developer/java-driver/4.9)

## License
Cassandra4io is released under the [Apache License 2.0](https://opensource.org/licenses/Apache-2.0).
