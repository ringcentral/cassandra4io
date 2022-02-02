package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.`type`.{ DataType, UserDefinedType }
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import shapeless.Lazy

import java.nio.ByteBuffer
import java.time.LocalDate
import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * A compile-time safe alternative to reflection for primitive Cassandra types
 * This typeclass is used to hold onto the associated Cassandra types associated with the Scala types for the underlying Datastax API
 * and handle boxing where needed
 *
 * @tparam Scala is the Scala type that is being mapped to the Datastax type
 */
trait CassandraTypeMapper[Scala] {
  type Cassandra
  def classType: Class[Cassandra]
  def toCassandra(in: Scala, dataType: DataType): Cassandra
  def fromCassandra(in: Cassandra, dataType: DataType): Scala
}
object CassandraTypeMapper       {
  type WithCassandra[Sc, Cas] = CassandraTypeMapper[Sc] { type Cassandra = Cas }

  def apply[A](implicit ev: CassandraTypeMapper[A]): CassandraTypeMapper[A] = ev

  implicit val strCassandraTypeMapper: CassandraTypeMapper.WithCassandra[String, String] =
    new CassandraTypeMapper[String] {
      type Cassandra = String
      def classType: Class[Cassandra]                              = classOf[String]
      def toCassandra(in: String, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): String = in
    }

  implicit val doubleCassandraTypeMapper: CassandraTypeMapper.WithCassandra[Double, java.lang.Double] =
    new CassandraTypeMapper[Double] {
      type Cassandra = java.lang.Double
      def classType: Class[Cassandra]                              = classOf[java.lang.Double]
      def toCassandra(in: Double, dataType: DataType): Cassandra   = Double.box(in)
      def fromCassandra(in: Cassandra, dataType: DataType): Double = in
    }

  implicit val intCassandraTypeMapper: CassandraTypeMapper.WithCassandra[Int, java.lang.Integer] =
    new CassandraTypeMapper[Int] {
      type Cassandra = java.lang.Integer
      def classType: Class[Cassandra]                           = classOf[java.lang.Integer]
      def toCassandra(in: Int, dataType: DataType): Cassandra   = scala.Int.box(in)
      def fromCassandra(in: Cassandra, dataType: DataType): Int = in
    }

  implicit val longCassandraTypeMapper: CassandraTypeMapper.WithCassandra[Long, java.lang.Long] =
    new CassandraTypeMapper[Long] {
      type Cassandra = java.lang.Long
      def classType: Class[Cassandra]                            = classOf[java.lang.Long]
      def toCassandra(in: Long, dataType: DataType): Cassandra   = Long.box(in)
      def fromCassandra(in: Cassandra, dataType: DataType): Long = in
    }

  implicit val byteBufferCassandraTypeMapper: CassandraTypeMapper.WithCassandra[ByteBuffer, ByteBuffer] =
    new CassandraTypeMapper[ByteBuffer] {
      type Cassandra = java.nio.ByteBuffer
      def classType: Class[Cassandra]                                  = classOf[java.nio.ByteBuffer]
      def toCassandra(in: ByteBuffer, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): ByteBuffer = in
    }

  implicit val localDateCassandraTypeMapper: CassandraTypeMapper.WithCassandra[LocalDate, LocalDate] =
    new CassandraTypeMapper[java.time.LocalDate] {
      type Cassandra = java.time.LocalDate
      def classType: Class[Cassandra]                                           = classOf[java.time.LocalDate]
      def toCassandra(in: java.time.LocalDate, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): java.time.LocalDate = in
    }

  implicit val localTimeCassandraTypeMapper
    : CassandraTypeMapper.WithCassandra[java.time.LocalTime, java.time.LocalTime] =
    new CassandraTypeMapper[java.time.LocalTime] {
      type Cassandra = java.time.LocalTime
      def classType: Class[Cassandra]                                           = classOf[java.time.LocalTime]
      def toCassandra(in: java.time.LocalTime, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): java.time.LocalTime = in
    }

  implicit val instantCassandraTypeMapper: CassandraTypeMapper.WithCassandra[java.time.Instant, java.time.Instant] =
    new CassandraTypeMapper[java.time.Instant] {
      type Cassandra = java.time.Instant
      def classType: Class[Cassandra]                                         = classOf[java.time.Instant]
      def toCassandra(in: java.time.Instant, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): java.time.Instant = in
    }

  implicit val boolCassandraTypeMapper: CassandraTypeMapper.WithCassandra[Boolean, java.lang.Boolean] =
    new CassandraTypeMapper[Boolean] {
      type Cassandra = java.lang.Boolean
      def classType: Class[Cassandra]                               = classOf[java.lang.Boolean]
      def toCassandra(in: Boolean, dataType: DataType): Cassandra   = Boolean.box(in)
      def fromCassandra(in: Cassandra, dataType: DataType): Boolean = in
    }

  implicit val uuidCassandraTypeMapper: CassandraTypeMapper.WithCassandra[java.util.UUID, java.util.UUID] =
    new CassandraTypeMapper[java.util.UUID] {
      type Cassandra = java.util.UUID
      def classType: Class[Cassandra]                                      = classOf[java.util.UUID]
      def toCassandra(in: java.util.UUID, dataType: DataType): Cassandra   = in
      def fromCassandra(in: Cassandra, dataType: DataType): java.util.UUID = in
    }

  implicit val shortCassandraTypeMapper: CassandraTypeMapper.WithCassandra[Short, java.lang.Short] =
    new CassandraTypeMapper[Short] {
      type Cassandra = java.lang.Short
      def classType: Class[Cassandra]                             = classOf[java.lang.Short]
      def toCassandra(in: Short, dataType: DataType): Cassandra   = scala.Short.box(in)
      def fromCassandra(in: Cassandra, dataType: DataType): Short = in.shortValue()
    }

  implicit val bigDecimalCassandraTypeMapper
    : CassandraTypeMapper.WithCassandra[scala.BigDecimal, java.math.BigDecimal] =
    new CassandraTypeMapper[scala.BigDecimal] {
      type Cassandra = java.math.BigDecimal
      def classType: Class[Cassandra]                                        = classOf[java.math.BigDecimal]
      def toCassandra(in: scala.BigDecimal, dataType: DataType): Cassandra   = in.bigDecimal
      def fromCassandra(in: Cassandra, dataType: DataType): scala.BigDecimal = in
    }

  implicit val bigIntCassandraTypeMapper: CassandraTypeMapper.WithCassandra[scala.BigInt, java.math.BigInteger] =
    new CassandraTypeMapper[scala.BigInt] {
      type Cassandra = java.math.BigInteger
      def classType: Class[Cassandra]                                    = classOf[java.math.BigInteger]
      def toCassandra(in: scala.BigInt, dataType: DataType): Cassandra   = in.bigInteger
      def fromCassandra(in: Cassandra, dataType: DataType): scala.BigInt = in
    }

  /**
   * We require proof that A has a ToUdtValue[A] in order to turn any A into a UdtValue and proof that A has a
   * FromUdtValue[A] in order to turn a UdtValue into an A
   *
   * This is an example of mutual induction (CassandraTypeMapper relies on ToUdtValue and FromUdtValue) and ToUdtValue
   * and FromUdtValue both rely on CassandraTypeMapper
   *
   * @param evToUdt
   * @param evFromUdt
   * @tparam A
   * @return
   */
  implicit def udtCassandraTypeMapper[A](implicit
    evToUdt: Lazy[ToUdtValue.Object[A]],
    evFromUdt: Lazy[FromUdtValue.Object[A]]
  ): CassandraTypeMapper.WithCassandra[A, UdtValue] =
    new CassandraTypeMapper[A] {
      override type Cassandra = UdtValue

      override def classType: Class[Cassandra] = classOf[UdtValue]

      override def toCassandra(in: A, dataType: DataType): Cassandra = {
        val schema = dataType.asInstanceOf[UserDefinedType]
        evToUdt.value.convert(FieldName.Unused, in, schema.newValue())
      }

      override def fromCassandra(in: Cassandra, dataType: DataType): A =
        evFromUdt.value.convert(FieldName.Unused, in)
    }

  implicit def setCassandraTypeMapper[A](implicit
    ev: CassandraTypeMapper[A]
  ): CassandraTypeMapper.WithCassandra[Set[A], java.util.Set[ev.Cassandra]] =
    new CassandraTypeMapper[Set[A]] {
      override type Cassandra = java.util.Set[ev.Cassandra]

      override def classType: Class[java.util.Set[ev.Cassandra]] = classOf[Cassandra]

      override def toCassandra(in: Set[A], dataType: DataType): Cassandra = {
        val elementOfSetDataType = dataType.asInstanceOf[DefaultSetType].getElementType
        in.map(ev.toCassandra(_, elementOfSetDataType)).asJava
      }

      override def fromCassandra(in: Cassandra, dataType: DataType): Set[A] = {
        val elementOfSetDataType = dataType.asInstanceOf[DefaultSetType].getElementType
        in.asScala.map(ev.fromCassandra(_, elementOfSetDataType)).toSet
      }
    }

  implicit def listCassandraTypeMapper[A](implicit
    ev: CassandraTypeMapper[A]
  ): CassandraTypeMapper.WithCassandra[List[A], java.util.List[ev.Cassandra]] =
    new CassandraTypeMapper[List[A]] {
      override type Cassandra = java.util.List[ev.Cassandra]

      override def classType: Class[java.util.List[ev.Cassandra]] = classOf[Cassandra]

      override def toCassandra(in: List[A], dataType: DataType): Cassandra = {
        val elementOfSetDataType = dataType.asInstanceOf[DefaultListType].getElementType
        in.map(ev.toCassandra(_, elementOfSetDataType)).asJava
      }

      override def fromCassandra(in: Cassandra, dataType: DataType): List[A] = {
        val elementOfListDataType = dataType.asInstanceOf[DefaultListType].getElementType
        in.asScala.map(ev.fromCassandra(_, elementOfListDataType)).toList
      }
    }

  implicit def mapCassandraTypeMapper[K, V](implicit
    kEv: CassandraTypeMapper[K],
    vEv: CassandraTypeMapper[V]
  ): CassandraTypeMapper.WithCassandra[Map[K, V], java.util.Map[kEv.Cassandra, vEv.Cassandra]] =
    new CassandraTypeMapper[Map[K, V]] {
      override type Cassandra = java.util.Map[kEv.Cassandra, vEv.Cassandra]

      override def classType: Class[java.util.Map[kEv.Cassandra, vEv.Cassandra]] = classOf[Cassandra]

      override def toCassandra(in: Map[K, V], dataType: DataType): Cassandra = {
        val mapDataType   = dataType.asInstanceOf[DefaultMapType]
        val keyDataType   = mapDataType.getKeyType
        val valueDataType = mapDataType.getValueType
        in.map { case (k, v) =>
          (kEv.toCassandra(k, keyDataType), vEv.toCassandra(v, valueDataType))
        }.asJava
      }

      override def fromCassandra(in: Cassandra, dataType: DataType): Map[K, V] = {
        val mapDataType   = dataType.asInstanceOf[DefaultMapType]
        val keyDataType   = mapDataType.getKeyType
        val valueDataType = mapDataType.getValueType
        in.asScala.map { case (kC, vC) =>
          (kEv.fromCassandra(kC, keyDataType), vEv.fromCassandra(vC, valueDataType))
        }.toMap
      }
    }

  implicit def optionCassandraTypeMapper[A, Cass](implicit
    ev: CassandraTypeMapper.WithCassandra[A, Cass]
  ): CassandraTypeMapper.WithCassandra[Option[A], Cass] =
    new CassandraTypeMapper[Option[A]] {
      override type Cassandra = Cass

      override def classType: Class[Cassandra] = ev.classType

      // NOTE: This is safe to do as the underlying Datastax driver allows you to use null values to represent the absence of data
      override def toCassandra(in: Option[A], dataType: DataType): Cassandra =
        in.map(ev.toCassandra(_, dataType)) match {
          case Some(value) => value
          case None        => null.asInstanceOf[Cassandra]
        }

      override def fromCassandra(in: Cassandra, dataType: DataType): Option[A] =
        Option(in).map(ev.fromCassandra(_, dataType))
    }
}
