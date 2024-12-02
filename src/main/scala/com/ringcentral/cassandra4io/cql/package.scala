package com.ringcentral.cassandra4io

import cats.data.OptionT
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{ Functor, Monad }
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.data.UdtValue
import fs2.Stream
import shapeless._
import shapeless.labelled.FieldType
import shapeless.ops.hlist.Prepend

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate }
import java.util.UUID
import scala.annotation.{ implicitNotFound, tailrec }

package object cql {

  case class QueryTemplate[V <: HList: Binder, R: Reads] private[cql] (
    query: String,
    config: BoundStatement => BoundStatement
  ) {
    def +(that: String): QueryTemplate[V, R] = QueryTemplate[V, R](this.query + that, config)

    def ++[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): QueryTemplate[Out, R] = concat(that)

    def concat[W <: HList, Out <: HList](that: QueryTemplate[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): QueryTemplate[Out, R] = QueryTemplate[Out, R](
      this.query + that.query,
      statement => (this.config andThen that.config)(statement)
    )

    def as[R1: Reads]: QueryTemplate[V, R1] = QueryTemplate[V, R1](query, config)

    def prepare[F[_]: Functor](session: CassandraSession[F]): F[PreparedQuery[F, V, R]] =
      session.prepare(query).map(new PreparedQuery(session, _, config))

    def config(config: BoundStatement => BoundStatement): QueryTemplate[V, R] =
      QueryTemplate[V, R](this.query, this.config andThen config)

    def stripMargin: QueryTemplate[V, R] = QueryTemplate[V, R](this.query.stripMargin, this.config)
  }

  type SimpleQuery[Output] = ParameterizedQuery[HNil, Output]

  case class ParameterizedQuery[V <: HList: Binder, R: Reads] private (template: QueryTemplate[V, R], values: V) {
    def +(that: String): ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template + that, this.values)

    def ++[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): ParameterizedQuery[Out, R] = concat(that)

    def concat[W <: HList, Out <: HList](that: ParameterizedQuery[W, R])(implicit
      prepend: Prepend.Aux[V, W, Out],
      binderForW: Binder[W],
      binderForOut: Binder[Out]
    ): ParameterizedQuery[Out, R] =
      ParameterizedQuery[Out, R](this.template ++ that.template, prepend(this.values, that.values))

    def as[R1: Reads]: ParameterizedQuery[V, R1] = ParameterizedQuery[V, R1](template.as[R1], values)

    def select[F[_]: Functor](session: CassandraSession[F]): Stream[F, R] =
      Stream.force(template.prepare(session).map(_.applyProduct(values).select))

    def selectFirst[F[_]: Monad](session: CassandraSession[F]): F[Option[R]] =
      template.prepare(session).flatMap(_.applyProduct(values).selectFirst)

    def execute[F[_]: Monad](session: CassandraSession[F]): F[Boolean] =
      template.prepare(session).map(_.applyProduct(values)).flatMap(_.execute)

    def config(config: BoundStatement => BoundStatement): ParameterizedQuery[V, R] =
      ParameterizedQuery[V, R](template.config(config), values)

    def stripMargin: ParameterizedQuery[V, R] = ParameterizedQuery[V, R](this.template.stripMargin, values)
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
    def select: Stream[F, R]                                = session.select(statement).map(Reads[R].read(_, 0))
    def selectFirst: F[Option[R]]                           = OptionT(session.selectFirst(statement)).map(Reads[R].read(_, 0)).value
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
      QueryTemplate[V, Row](
        ctx.parts
          .foldLeft[(HList, StringBuilder)]((params, new StringBuilder())) {
            case ((Const(const) :: tail, builder), part)                => (tail, builder.appendAll(part).appendAll(const))
            case (((restriction: EqualsTo[_]) :: tail, builder), part)  =>
              (tail, builder.appendAll(part).appendAll(restriction.keys.map(key => s"${key} = ?").mkString(" AND ")))
            case (((assignment: Assignment[_]) :: tail, builder), part) =>
              (tail, builder.appendAll(part).appendAll(assignment.keys.map(key => s"${key} = ?").mkString(", ")))
            case (((columns: Columns[_]) :: tail, builder), part)       =>
              (tail, builder.appendAll(part).appendAll(columns.keys.mkString(", ")))
            case (((values: Values[_]) :: tail, builder), part)         =>
              (tail, builder.appendAll(part).appendAll(List.fill(values.size)("?").mkString(", ")))
            case ((_ :: tail, builder), part)                           => (tail, builder.appendAll(part).appendAll("?"))
            case ((HNil, builder), part)                                => (HNil, builder.appendAll(part))
          }
          ._2
          .toString(),
        identity
      )
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
      implicit def hConsBindableBuilder[T: Binder, PT <: HList, RT <: HList](implicit
        f: BindableBuilder.Aux[PT, RT]
      ): BindableBuilder.Aux[Put[T] :: PT, T :: RT]                                            = new BindableBuilder[Put[T] :: PT] {
        override type Repr = T :: RT
        override def binder: Binder[T :: RT] = {
          implicit val tBinder: Binder[RT] = f.binder
          Binder[T :: RT]
        }
      }
      implicit def hConsBindableConstBuilder[PT <: HList, RT <: HList](implicit
        f: BindableBuilder.Aux[PT, RT]
      ): BindableBuilder.Aux[Const :: PT, RT]                                                  =
        new BindableBuilder[Const :: PT] {
          override type Repr = RT
          override def binder: Binder[RT] = f.binder
        }

      implicit def hConsBindableColumnsBuilder[T, PT <: HList, RT <: HList](implicit
        f: BindableBuilder.Aux[PT, RT]
      ): BindableBuilder.Aux[Columns[T] :: PT, RT] =
        new BindableBuilder[Columns[T] :: PT] {
          override type Repr = RT
          override def binder: Binder[RT] = f.binder
        }

      implicit def hConsBindableValuesBuilder[V[_] <: Values[_], T: ColumnsValues, PT <: HList, RT <: HList](implicit
        f: BindableBuilder.Aux[PT, RT]
      ): BindableBuilder.Aux[V[T] :: PT, T :: RT] = new BindableBuilder[V[T] :: PT] {
        override type Repr = T :: RT
        override def binder: Binder[T :: RT] = {
          implicit val hBinder: Binder[T]  = Values[T].binder
          implicit val tBinder: Binder[RT] = f.binder
          Binder[T :: RT]
        }
      }
    }
  }

  /**
   * BoundValue is used to capture the value inside the cql interpolated string along with evidence of its Binder so that
   * a ParameterizedQuery can be built and the values can be bound to the BoundStatement internally
   */
  final case class BoundValue[A](value: A, ev: Binder[A])
  object BoundValue {
    // This implicit conversion automatically captures the value and evidence of the Binder in a cql interpolated string
    implicit def aToBoundValue[A](a: A)(implicit ev: Binder[A]): BoundValue[A] =
      BoundValue(a, ev)
  }

  class CqlStringInterpolator(ctx: StringContext) {
    @tailrec
    private def replaceValuesWithQuestionMark(
      strings: Iterator[String],
      expressions: Iterator[BoundValue[_]],
      acc: String
    ): String =
      if (strings.hasNext && expressions.hasNext) {
        val str = strings.next()
        val _   = expressions.next()
        replaceValuesWithQuestionMark(
          strings = strings,
          expressions = expressions,
          acc = acc + s"$str?"
        )
      } else if (strings.hasNext && !expressions.hasNext) {
        val str = strings.next()
        replaceValuesWithQuestionMark(
          strings = strings,
          expressions = expressions,
          acc + str
        )
      } else acc

    def apply(values: BoundValue[_]*): SimpleQuery[Row] = {
      val queryWithQuestionMark = replaceValuesWithQuestionMark(ctx.parts.iterator, values.iterator, "")
      val assignValuesToStatement: BoundStatement => BoundStatement = { in: BoundStatement =>
        val (configuredBoundStatement, _) =
          values.foldLeft((in, 0)) { case ((current, index), bv: BoundValue[a]) =>
            val binder: Binder[a] = bv.ev
            val value: a          = bv.value
            binder.bind(current, index, value)
          }
        configuredBoundStatement
      }
      ParameterizedQuery(QueryTemplate[HNil, Row](queryWithQuestionMark, assignValuesToStatement), HNil)
    }
  }

  /**
   * Provides a way to lift arbitrary strings into CQL so you can parameterize on values that are not valid CQL parameters
   * Please note that this is not escaped so do not use this with user-supplied input for your application (only use
   * cqlConst for input that you as the application author control)
   */
  class CqlConstInterpolator(ctx: StringContext) {
    def apply(args: Any*): ParameterizedQuery[HNil, Row] =
      ParameterizedQuery(QueryTemplate(ctx.s(args: _*), identity), HNil)
  }

  implicit class CqlStringContext(val ctx: StringContext) extends AnyVal {
    def cqlt     = new CqlTemplateStringInterpolator(ctx)
    def cql      = new CqlStringInterpolator(ctx)
    def cqlConst = new CqlConstInterpolator(ctx)
  }

  implicit class UnsetOptionValueOps[A](val self: Option[A]) extends AnyVal {
    def usingUnset(implicit aBinder: Binder[A]): BoundValue[Option[A]] =
      BoundValue(self, Binder.optionUsingUnsetBinder[A])
  }

  @implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
  trait Binder[T] { self =>
    def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int)

    def contramap[U](f: U => T): Binder[U] = new Binder[U] {
      override def bind(statement: BoundStatement, index: Int, value: U): (BoundStatement, Int) =
        self.bind(statement, index, f(value))
    }
  }

  trait Put[T]
  object Put {
    def apply[T: Binder]: Put[T] = new Put[T] {}
  }

  case class Const(fragment: String)
  trait Columns[T] {
    def keys: List[String]
  }
  object Columns   {
    def apply[T: ColumnsValues]: Columns[T] = new Columns[T] {
      override def keys: List[String] = ColumnsValues[T].keys
    }
  }
  trait Values[T]  {
    def size: Int
    def binder: Binder[T]
  }
  object Values    {
    def apply[T: ColumnsValues]: Values[T] = new Values[T] {
      override def size: Int         = ColumnsValues[T].size
      override def binder: Binder[T] = ColumnsValues[T].binder
    }
  }
  trait EqualsTo[T] extends Columns[T] with Values[T]
  object EqualsTo  {
    def apply[T: ColumnsValues]: EqualsTo[T] = new EqualsTo[T] {
      override def keys: List[String] = ColumnsValues[T].keys
      override def size: Int          = ColumnsValues[T].size
      override def binder: Binder[T]  = ColumnsValues[T].binder
    }
  }

  trait Assignment[T] extends Columns[T] with Values[T]
  object Assignment {
    def apply[T: ColumnsValues]: Assignment[T] = new Assignment[T] {
      override def keys: List[String] = ColumnsValues[T].keys
      override def size: Int          = ColumnsValues[T].size
      override def binder: Binder[T]  = ColumnsValues[T].binder
    }
  }

  private trait ColumnsValues[T] extends Columns[T] with Values[T]
  private object ColumnsValues {
    def apply[T](implicit ev: ColumnsValues[T]): ColumnsValues[T] = ev

    implicit val hNilColumnsValues: ColumnsValues[HNil] = new ColumnsValues[HNil] {
      override def keys: List[String]   = List.empty
      override def size: Int            = 0
      override def binder: Binder[HNil] = Binder.hNilBinder
    }

    private def camel2snake(text: String) =
      text.tail.foldLeft(text.headOption.fold("")(_.toLower.toString)) {
        case (acc, c) if c.isUpper => acc + "_" + c.toLower
        case (acc, c)              => acc + c
      }

    implicit def hListColumnsValues[K, V, T <: HList](implicit
      witness: Witness.Aux[K],
      tColumnsValues: ColumnsValues[T],
      vBinder: Binder[V]
    ): ColumnsValues[FieldType[K, V] :: T] =
      new ColumnsValues[FieldType[K, V] :: T] {
        override def keys: List[String] = {
          val key = witness.value match {
            case Symbol(key) => camel2snake(key)
            case _           => witness.value.toString
          }
          key :: tColumnsValues.keys
        }
        override def size: Int = tColumnsValues.size + 1
        override def binder: Binder[FieldType[K, V] :: T] = {
          implicit val hBinder: Binder[FieldType[K, V]] = new Binder[FieldType[K, V]] {
            override def bind(statement: BoundStatement, index: Int, value: FieldType[K, V]): (BoundStatement, Int) =
              vBinder.bind(statement, index, value)
          }
          implicit val tBinder: Binder[T]               = tColumnsValues.binder
          Binder[FieldType[K, V] :: T]
        }
      }
    implicit def genColumnValues[T, TRepr](implicit
      gen: LabelledGeneric.Aux[T, TRepr],
      columnsValues: ColumnsValues[TRepr]
    ): ColumnsValues[T]                    = new ColumnsValues[T] {
      override def keys: List[String] = columnsValues.keys
      override def size: Int          = columnsValues.size
      override def binder: Binder[T]  = new Binder[T] {
        override def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int) =
          columnsValues.binder.bind(statement, index, gen.to(value))
      }
    }
  }

  object Binder extends BinderLowerPriority with BinderLowestPriority {

    def apply[T](implicit binder: Binder[T]): Binder[T] = binder

    implicit val stringBinder: Binder[String] = new Binder[String] {
      override def bind(statement: BoundStatement, index: Int, value: String): (BoundStatement, Int) =
        (statement.setString(index, value), index + 1)
    }

    implicit val doubleBinder: Binder[Double] = new Binder[Double] {
      override def bind(statement: BoundStatement, index: Int, value: Double): (BoundStatement, Int) =
        (statement.setDouble(index, value), index + 1)
    }

    implicit val floatBinder: Binder[Float] = new Binder[Float] {
      override def bind(statement: BoundStatement, index: Int, value: Float): (BoundStatement, Int) =
        (statement.setFloat(index, value), index + 1)
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

    implicit val bigIntBinder: Binder[BigInt] = new Binder[BigInt] {
      override def bind(statement: BoundStatement, index: Int, value: BigInt): (BoundStatement, Int) =
        (statement.setBigInteger(index, value.bigInteger), index + 1)
    }

    implicit val bigDecimalBinder: Binder[BigDecimal] = new Binder[BigDecimal] {
      override def bind(statement: BoundStatement, index: Int, value: BigDecimal): (BoundStatement, Int) =
        (statement.setBigDecimal(index, value.bigDecimal), index + 1)
    }

    implicit val shortBinder: Binder[Short] = new Binder[Short] {
      override def bind(statement: BoundStatement, index: Int, value: Short): (BoundStatement, Int) =
        (statement.setShort(index, value), index + 1)
    }

    implicit val userDefinedTypeValueBinder: Binder[UdtValue] =
      (statement: BoundStatement, index: Int, value: UdtValue) => (statement.setUdtValue(index, value), index + 1)

    private def commonOptionBinder[T: Binder](
      bindNone: (BoundStatement, Int) => BoundStatement
    ): Binder[Option[T]] = new Binder[Option[T]] {
      override def bind(statement: BoundStatement, index: Int, value: Option[T]): (BoundStatement, Int) = value match {
        case Some(x) => Binder[T].bind(statement, index, x)
        case None    => (bindNone(statement, index), index + 1)
      }
    }

    implicit def optionBinder[T: Binder]: Binder[Option[T]] = commonOptionBinder[T] { (statement, index) =>
      statement.setToNull(index)
    }

    def optionUsingUnsetBinder[T: Binder]: Binder[Option[T]] = commonOptionBinder[T] { (statement, index) =>
      statement.unset(index)
    }

    implicit def widenBinder[T: Binder, X <: T](implicit wd: Widen.Aux[X, T]): Binder[X] = new Binder[X] {
      override def bind(statement: BoundStatement, index: Int, value: X): (BoundStatement, Int) =
        Binder[T].bind(statement, index, wd.apply(value))
    }

    implicit class UdtValueBinderOps(udtBinder: Binder[UdtValue]) {

      /**
       * This is necessary for UDT values as you are not allowed to safely create a UDT value, instead you use the
       * prepared statement's variable definitions to retrieve a UserDefinedType that can be used as a constructor
       * for a UdtValue
       *
       * @param f is a function that accepts the input value A along with a constructor that you use to build the
       *          UdtValue that gets sent to Cassandra
       * @tparam A
       * @return
       */
      def contramapUDT[A](f: (A, UserDefinedType) => UdtValue): Binder[A] = new Binder[A] {
        override def bind(statement: BoundStatement, index: Int, value: A): (BoundStatement, Int) = {
          val udtValue = f(
            value,
            statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
          )
          udtBinder.bind(statement, index, udtValue)
        }
      }
    }
  }

  trait BinderLowerPriority {

    /**
     * This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
     * @param ev is evidence that a typeclass instance of CassandraTypeMapper exists for A
     * @tparam A is the Scala datatype that needs to be written to Cassandra
     * @return
     */
    implicit def deriveBinderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Binder[A] =
      (statement: BoundStatement, index: Int, value: A) => {
        val datatype  = statement.getType(index)
        val cassandra = ev.toCassandra(value, datatype)
        (statement.set(index, cassandra, ev.classType), index + 1)
      }
  }

  trait BinderLowestPriority {
    implicit val hNilBinder: Binder[HNil]                                   = new Binder[HNil] {
      override def bind(statement: BoundStatement, index: Int, value: HNil): (BoundStatement, Int) = (statement, index)
    }
    implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
      override def bind(statement: BoundStatement, index: Int, value: H :: T): (BoundStatement, Int) = {
        val (applied, nextIndex) = Binder[H].bind(statement, index, value.head)
        Binder[T].bind(applied, nextIndex, value.tail)
      }
    }
  }
}
