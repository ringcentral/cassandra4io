# Cassandra 4 io

![CI](https://github.com/ringcentral/cassandra4io/workflows/CI/badge.svg?branch=main)

This is lightweight cats-effect and fs2 IO wrapper for latest datastax 4.x driver.

Why 4.x?
 
4.x was re-written in immutable first design, within async first API, 
optimizations, less allocations, metrics improvements, and fully compatible with cassandra 3.x

## Goals
- Be safe, type-safe.
- Be fast 
    - use minimal allocations
    - minimize resources and abstractions overhead over original datastax driver, which is good


## How to use
Cassandra4io is currently available for Scala 2.13 and 2.12.

### Add a dependency to your project
```scala
libraryDependencies += ("com.ringcentral" %% "cassandra4io" % "0.1.5")
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

def makeSession[F[_]: Async: ContextShift]: Resource[F, CassandraSession[F]] =
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

class DaoImpl[F[_]: Sync](session: CassandraSession[F]) extends Dao[F] {

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
import cats.effect.Sync
import scala.concurrent.duration._
import cats.syntax.all._
import scala.jdk.DurationConverters._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.ringcentral.cassandra4io.CassandraSession
import com.ringcentral.cassandra4io.cql._
    
case class Model(id: Int, data: String)
  
trait Dao[F[_]] {
  def put(value: Model): F[Unit]
  def get(id: Int): F[Option[Model]]
}
    
object Dao {
  
  private val insertQuery = cqlt"insert into table (id, data) values (${Put[Int]}, ${Put[String]})"
    .config(_.setTimeout(1.second.toJava))
  private val selectQuery = cqlt"select id, data from table where id = ${Put[Int]}".as[Model]

  def apply[F[_]: Sync](session: CassandraSession[F]) = for {
    insert <- insertQuery.prepare(session)
    select <- selectQuery.prepare(session)      
  } yield new Dao[F] {
    override def put(value: Model) = insert(value.id, value.data).execute.void
    override def get(id: Int) = select(id).config(_.setExecutionProfileName("default")).select.head.compile.last
  } 
} 
```


## References
- java driver https://docs.datastax.com/en/developer/java-driver/4.9/

## License
Cassandra4io is released under the [Apache License 2.0](https://opensource.org/licenses/Apache-2.0).
