package com.ringcentral.cassandra4io

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate }

import cats.data.OptionT
import cats.{ Functor, Monad }
import cats.syntax.flatMap._
import cats.syntax.functor._
import com.datastax.oss.driver.api.core.cql.{ BatchStatementBuilder, BatchType, BoundStatement, PreparedStatement, Row }
import fs2.Stream
import shapeless._

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

package object cql {

  case class QueryTemplate[V <: HList: Binder, R: Reads] private[cql] (
    query: String,
    config: BoundStatement => BoundStatement
  ) {
    def +(that: String): QueryTemplate[V, R]                                            = QueryTemplate[V, R](this.query + that, config)
    def as[R1: Reads]: QueryTemplate[V, R1]                                             = QueryTemplate[V, R1](query, config)
    def prepare[F[_]: Functor](session: CassandraSession[F]): F[PreparedQuery[F, V, R]] =
      session.prepare(query).map(new PreparedQuery(session, _, config))
    def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R]           =
      QueryTemplate[V, R](this.query, this.config andThen config)
    def stripMargin: QueryTemplate[V, R]                                                = QueryTemplate[V, R](this.query.stripMargin, this.config)
  }

  case class ParameterizedQuery[V <: HList: Binder, R: Reads] private (template: QueryTemplate[V, R], values: V) {
    def +(that: String): ParameterizedQuery[V, R]                                  = ParameterizedQuery[V, R](this.template + that, this.values)
    def as[R1: Reads]: ParameterizedQuery[V, R1]                                   = ParameterizedQuery[V, R1](template.as[R1], values)
    def select[F[_]: Functor](session: CassandraSession[F]): Stream[F, R]          =
      Stream.force(template.prepare(session).map(_.applyProduct(values).select))
    def selectFirst[F[_]: Monad](session: CassandraSession[F]): F[Option[R]]       =
      template.prepare(session).flatMap(_.applyProduct(values).selectFirst)
    def execute[F[_]: Monad](session: CassandraSession[F]): F[Boolean]             =
      template.prepare(session).map(_.applyProduct(values)).flatMap(_.execute)
    def config(config: BoundStatement => BoundStatement): ParameterizedQuery[V, R] =
      ParameterizedQuery[V, R](template.config(config), values)
    def stripMargin: ParameterizedQuery[V, R]                                      = ParameterizedQuery[V, R](this.template.stripMargin, values)
  }

  class PreparedQuery[F[_]: Functor, V <: HList: Binder, R: Reads] private[cql] (
    session: CassandraSession[F],
    statement: PreparedStatement,
    config: BoundStatement => BoundStatement
  ) extends ProductArgs {
    def applyProduct(values: V) = new Query[F, R](session, Binder[V].bind(config(statement.bind()), 0, values)._1)
  }

  class Query[F[_]: Functor, R: Reads] private[cql] (
    session: CassandraSession[F],
    private[cql] val statement: BoundStatement
  ) {
    def config(statement: BoundStatement => BoundStatement) = new Query[F, R](session, statement(this.statement))
    def select: Stream[F, R]                                = session.select(statement).map(Reads[R].read(_, 0)._1)
    def selectFirst: F[Option[R]]                           = OptionT(session.selectFirst(statement)).map(Reads[R].read(_, 0)._1).value
    def execute: F[Boolean]                                 = session.execute(statement).map(_.wasApplied)
  }

  class Batch[F[_]: Functor](batchStatementBuilder: BatchStatementBuilder) {
    def add(queries: Seq[Query[F, _]])                                           = new Batch[F](batchStatementBuilder.addStatements(queries.map(_.statement): _*))
    def execute(session: CassandraSession[F]): F[Boolean]                        =
      session.execute(batchStatementBuilder.build()).map(_.wasApplied)
    def config(config: BatchStatementBuilder => BatchStatementBuilder): Batch[F] =
      new Batch[F](config(batchStatementBuilder))
  }

  object Batch {
    def logged[F[_]: Functor]   = new Batch[F](new BatchStatementBuilder(BatchType.LOGGED))
    def unlogged[F[_]: Functor] = new Batch[F](new BatchStatementBuilder(BatchType.UNLOGGED))
  }

  class CqlTemplateStringInterpolator(ctx: StringContext) extends ProductArgs {
    import CqlTemplateStringInterpolator._
    def applyProduct[P <: HList, V <: HList](params: P)(implicit
      bb: BindableBuilder.Aux[P, V]
    ): QueryTemplate[V, Row] = {
      implicit val binder: Binder[V] = bb.binder
      QueryTemplate[V, Row](ctx.parts.mkString("?"), identity)
    }
  }

  object CqlTemplateStringInterpolator {

    trait BindableBuilder[P] {
      type Repr <: HList
      def binder: Binder[Repr]
    }

    private object BindableBuilder {
      type Aux[P, Repr0] = BindableBuilder[P] { type Repr = Repr0 }
      def apply[P](implicit builder: BindableBuilder[P]): BindableBuilder.Aux[P, builder.Repr] = builder
      implicit def hNilBindableBuilder: BindableBuilder.Aux[HNil, HNil]                        = new BindableBuilder[HNil] {
        override type Repr = HNil
        override def binder: Binder[HNil] = Binder[HNil]
      }
      implicit def hConsBindableBuilder[PH <: Put[_], T: Binder, PT <: HList, RT <: HList](implicit
                                                                                           f: BindableBuilder.Aux[PT, RT]
                                                                                          ): BindableBuilder.Aux[Put[T] :: PT, T :: RT]                                            = new BindableBuilder[Put[T] :: PT] {
        override type Repr = T :: RT
        override def binder: Binder[T :: RT] = {
          implicit val tBinder: Binder[RT] = f.binder
          Binder[T :: RT]
        }
      }
    }
  }

  class CqlStringInterpolator(ctx: StringContext) extends ProductArgs {
    def applyProduct[V <: HList: Binder](values: V): ParameterizedQuery[V, Row] =
      ParameterizedQuery[V, Row](QueryTemplate[V, Row](ctx.parts.mkString("?"), identity), values)
  }

  implicit class CqlStringContext(ctx: StringContext) {
    val cqlt = new CqlTemplateStringInterpolator(ctx)
    val cql  = new CqlStringInterpolator(ctx)
  }

  @implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
  trait Binder[T] {
    def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int)
  }

  trait Put[T]
  object Put {
    def apply[T: Binder]: Put[T] = new Put[T] {}
  }

  object Binder {

    def apply[T](implicit binder: Binder[T]): Binder[T] = binder

    implicit val stringBinder: Binder[String] = new Binder[String] {
      override def bind(statement: BoundStatement, index: Int, value: String): (BoundStatement, Int) =
        (statement.setString(index, value), index + 1)
    }

    implicit val doubleBinder: Binder[Double] = new Binder[Double] {
      override def bind(statement: BoundStatement, index: Int, value: Double): (BoundStatement, Int) =
        (statement.setDouble(index, value), index + 1)
    }

    implicit val intBinder: Binder[Int] = new Binder[Int] {
      override def bind(statement: BoundStatement, index: Int, value: Int): (BoundStatement, Int) =
        (statement.setInt(index, value), index + 1)
    }

    implicit val longBinder: Binder[Long] = new Binder[Long] {
      override def bind(statement: BoundStatement, index: Int, value: Long): (BoundStatement, Int) =
        (statement.setLong(index, value), index + 1)
    }

    implicit val byteBufferBinder: Binder[ByteBuffer] = new Binder[ByteBuffer] {
      override def bind(statement: BoundStatement, index: Int, value: ByteBuffer): (BoundStatement, Int) =
        (statement.setByteBuffer(index, value), index + 1)
    }

    implicit val localDateBinder: Binder[LocalDate] = new Binder[LocalDate] {
      override def bind(statement: BoundStatement, index: Int, value: LocalDate): (BoundStatement, Int) =
        (statement.setLocalDate(index, value), index + 1)
    }

    implicit val instantBinder: Binder[Instant] = new Binder[Instant] {
      override def bind(statement: BoundStatement, index: Int, value: Instant): (BoundStatement, Int) =
        (statement.setInstant(index, value), index + 1)
    }

    implicit val booleanBinder: Binder[Boolean] = new Binder[Boolean] {
      override def bind(statement: BoundStatement, index: Int, value: Boolean): (BoundStatement, Int) =
        (statement.setBoolean(index, value), index + 1)
    }

    implicit val uuidBinder: Binder[UUID] = new Binder[UUID] {
      override def bind(statement: BoundStatement, index: Int, value: UUID): (BoundStatement, Int) =
        (statement.setUuid(index, value), index + 1)
    }

    implicit def optionBinder[T: Binder]: Binder[Option[T]] = new Binder[Option[T]] {
      override def bind(statement: BoundStatement, index: Int, value: Option[T]): (BoundStatement, Int) = value match {
        case Some(x) => Binder[T].bind(statement, index, x)
        case None    => (statement.setToNull(index), index + 1)
      }
    }

    implicit val longListBinder: Binder[List[Long]] = new Binder[List[Long]] {
      override def bind(statement: BoundStatement, index: Int, value: List[Long]): (BoundStatement, Int) =
        (statement.setList(index, value.map(Long.box).asJava, classOf[java.lang.Long]), index + 1)
    }

    implicit def listBinder[T: Binder: ClassTag]: Binder[List[T]] = new Binder[List[T]] {
      override def bind(statement: BoundStatement, index: Int, value: List[T]): (BoundStatement, Int) =
        (statement.setList(index, value.asJava, implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]), index + 1)
    }

    implicit val hNilBinder: Binder[HNil]                                   = new Binder[HNil] {
      override def bind(statement: BoundStatement, index: Int, value: HNil): (BoundStatement, Int) = (statement, index)
    }
    implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
      override def bind(statement: BoundStatement, index: Int, value: H :: T): (BoundStatement, Int) = {
        val (applied, nextIndex) = Binder[H].bind(statement, index, value.head)
        Binder[T].bind(applied, nextIndex, value.tail)
      }
    }

    implicit def widenBinder[T: Binder, X <: T](implicit wd: Widen.Aux[X, T]): Binder[X] = new Binder[X] {
      override def bind(statement: BoundStatement, index: Int, value: X): (BoundStatement, Int) =
        Binder[T].bind(statement, index, wd.apply(value))
    }
  }

  trait Reads[T] {
    def read(row: Row, index: Int): (T, Int)
  }
  object Reads   {
    def apply[T](implicit r: Reads[T]): Reads[T] = r

    implicit val rowReads: Reads[Row] = (row: Row, i: Int) => (row, i)

    implicit val stringReads: Reads[String]         = (row: Row, index: Int) => (row.getString(index), index + 1)
    implicit val doubleReads: Reads[Double]         = (row: Row, index: Int) => (row.getDouble(index), index + 1)
    implicit val intReads: Reads[Int]               = (row: Row, index: Int) => (row.getInt(index), index + 1)
    implicit val longReads: Reads[Long]             = (row: Row, index: Int) => (row.getLong(index), index + 1)
    implicit val byteBufferReads: Reads[ByteBuffer] = (row: Row, index: Int) => (row.getByteBuffer(index), index + 1)
    implicit val localDateReads: Reads[LocalDate]   = (row: Row, index: Int) => (row.getLocalDate(index), index + 1)
    implicit val instantReads: Reads[Instant]       = (row: Row, index: Int) => (row.getInstant(index), index + 1)
    implicit val booleanReads: Reads[Boolean]       = (row: Row, index: Int) => (row.getBoolean(index), index + 1)

    implicit def optionReads[T: Reads]: Reads[Option[T]] =
      (row: Row, index: Int) =>
        if (row.isNull(index)) (None, index + 1)
        else {
          val (t, i) = Reads[T].read(row, index)
          Some(t) -> i
        }

    implicit def arrayReads[T](implicit ct: ClassTag[T]): Reads[List[T]] =
      (row: Row, index: Int) => (row.getList(index, ct.runtimeClass.asInstanceOf[Class[T]]).asScala.toList, index + 1)

    implicit val hNilParser: Reads[HNil] = (_: Row, index: Int) => (HNil, index)

    implicit def hConsParser[H: Reads, T <: HList: Reads]: Reads[H :: T] = (row: Row, index: Int) => {
      val (h, nextIndex) = Reads[H].read(row, index)
      val (t, lastIndex) = Reads[T].read(row, nextIndex)
      (h :: t, lastIndex)
    }

    implicit def caseClassParser[A, R <: HList](implicit
      gen: Generic[A] { type Repr = R },
      reprParser: Reads[R]
    ): Reads[A] = (row: Row, index: Int) => {
      val (rep, nextIndex) = reprParser.read(row, index)
      gen.from(rep) -> nextIndex
    }
  }
}
