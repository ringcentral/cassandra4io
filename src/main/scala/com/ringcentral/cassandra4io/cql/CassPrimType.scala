package com.ringcentral.cassandra4io.cql

import java.nio.ByteBuffer

// A compile-time safe alternative to reflection for primitive Cassandra types
// This typeclass is used to hold onto the associated Cassandra types associated with the Scala types for the underlying Datastax API
// and handle boxing where needed
trait CassPrimType[A] {
  type CassType
  def cassType: Class[CassType]
  def toCassandra(in: A): CassType
  def fromCassandra(in: CassType): A
}
object CassPrimType   {
  def apply[A](implicit ev: CassPrimType[A]): CassPrimType[A] = ev

  implicit case object Str extends CassPrimType[String] {
    type CassType = String
    def cassType: Class[CassType]           = classOf[String]
    def toCassandra(in: String): CassType   = in
    def fromCassandra(in: CassType): String = in
  }

  implicit case object Dbl extends CassPrimType[Double] {
    type CassType = java.lang.Double
    def cassType: Class[CassType]           = classOf[java.lang.Double]
    def toCassandra(in: Double): CassType   = Double.box(in)
    def fromCassandra(in: CassType): Double = in
  }

  implicit case object Int extends CassPrimType[Int] {
    type CassType = java.lang.Integer
    def cassType: Class[CassType]        = classOf[java.lang.Integer]
    def toCassandra(in: Int): CassType   = scala.Int.box(in)
    def fromCassandra(in: CassType): Int = in
  }

  implicit case object Lng extends CassPrimType[Long] {
    type CassType = java.lang.Long
    def cassType: Class[CassType]         = classOf[java.lang.Long]
    def toCassandra(in: Long): CassType   = Long.box(in)
    def fromCassandra(in: CassType): Long = in
  }

  implicit case object ByteBuf extends CassPrimType[ByteBuffer] {
    type CassType = java.nio.ByteBuffer
    def cassType: Class[CassType]               = classOf[java.nio.ByteBuffer]
    def toCassandra(in: ByteBuffer): CassType   = in
    def fromCassandra(in: CassType): ByteBuffer = in
  }

  implicit case object LocalDate extends CassPrimType[java.time.LocalDate] {
    type CassType = java.time.LocalDate
    def cassType: Class[CassType]                        = classOf[java.time.LocalDate]
    def toCassandra(in: java.time.LocalDate): CassType   = in
    def fromCassandra(in: CassType): java.time.LocalDate = in
  }

  implicit case object LocalTime extends CassPrimType[java.time.LocalTime] {
    type CassType = java.time.LocalTime
    def cassType: Class[CassType]                        = classOf[java.time.LocalTime]
    def toCassandra(in: java.time.LocalTime): CassType   = in
    def fromCassandra(in: CassType): java.time.LocalTime = in
  }

  implicit case object Instant extends CassPrimType[java.time.Instant] {
    type CassType = java.time.Instant
    def cassType: Class[CassType]                      = classOf[java.time.Instant]
    def toCassandra(in: java.time.Instant): CassType   = in
    def fromCassandra(in: CassType): java.time.Instant = in
  }

  implicit case object Bool extends CassPrimType[Boolean] {
    type CassType = java.lang.Boolean
    def cassType: Class[CassType]            = classOf[java.lang.Boolean]
    def toCassandra(in: Boolean): CassType   = Boolean.box(in)
    def fromCassandra(in: CassType): Boolean = in
  }

  implicit case object UUID extends CassPrimType[java.util.UUID] {
    type CassType = java.util.UUID
    def cassType: Class[CassType]                   = classOf[java.util.UUID]
    def toCassandra(in: java.util.UUID): CassType   = in
    def fromCassandra(in: CassType): java.util.UUID = in
  }

  implicit case object Short extends CassPrimType[Short] {
    type CassType = java.lang.Short
    def cassType: Class[CassType]          = classOf[java.lang.Short]
    def toCassandra(in: Short): CassType   = scala.Short.box(in)
    def fromCassandra(in: CassType): Short = in.shortValue()
  }

  implicit case object BigDecimal extends CassPrimType[scala.BigDecimal] {
    type CassType = java.math.BigDecimal
    def cassType: Class[CassType]                     = classOf[java.math.BigDecimal]
    def toCassandra(in: scala.BigDecimal): CassType   = in.bigDecimal
    def fromCassandra(in: CassType): scala.BigDecimal = in
  }

  implicit case object BigInt extends CassPrimType[scala.BigInt] {
    type CassType = java.math.BigInteger
    def cassType: Class[CassType]                 = classOf[java.math.BigInteger]
    def toCassandra(in: scala.BigInt): CassType   = in.bigInteger
    def fromCassandra(in: CassType): scala.BigInt = in
  }
}
