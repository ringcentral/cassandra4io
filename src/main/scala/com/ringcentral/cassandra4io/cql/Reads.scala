package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{DefaultListType, DefaultMapType, DefaultSetType}
import shapeless.{::, Generic, HList, HNil}

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate}
import java.util.UUID
import scala.jdk.CollectionConverters._

trait Reads[T] { self =>
  def read(row: Row, index: Int): (T, Int)

  def map[U](f: T => U): Reads[U] =
    new Reads[U] {
      override def read(row: Row, index: Int): (U, Int) = {
        val (t, nextIndex) = self.read(row, index)
        (f(t), nextIndex)
      }
    }
}
object Reads extends ReadsLowerPriority with ReadsLowestPriority {
  def apply[T](implicit r: Reads[T]): Reads[T] = r

  implicit val rowReads: Reads[Row]               = (row: Row, i: Int) => (row, i)
  implicit val stringReads: Reads[String]         = (row: Row, index: Int) => (row.getString(index), index + 1)
  implicit val doubleReads: Reads[Double]         = (row: Row, index: Int) => (row.getDouble(index), index + 1)
  implicit val intReads: Reads[Int]               = (row: Row, index: Int) => (row.getInt(index), index + 1)
  implicit val longReads: Reads[Long]             = (row: Row, index: Int) => (row.getLong(index), index + 1)
  implicit val byteBufferReads: Reads[ByteBuffer] = (row: Row, index: Int) => (row.getByteBuffer(index), index + 1)
  implicit val localDateReads: Reads[LocalDate]   = (row: Row, index: Int) => (row.getLocalDate(index), index + 1)
  implicit val instantReads: Reads[Instant]       = (row: Row, index: Int) => (row.getInstant(index), index + 1)
  implicit val booleanReads: Reads[Boolean]       = (row: Row, index: Int) => (row.getBoolean(index), index + 1)
  implicit val uuidReads: Reads[UUID]             = (row: Row, index: Int) => (row.getUuid(index), index + 1)
  implicit val bigIntReads: Reads[BigInt]         = (row: Row, index: Int) => (row.getBigInteger(index), index + 1)
  implicit val bigDecimalReads: Reads[BigDecimal] = (row: Row, index: Int) => (row.getBigDecimal(index), index + 1)
  implicit val shortReads: Reads[Short]           = (row: Row, index: Int) => (row.getShort(index), index + 1)
  implicit val udtReads: Reads[UdtValue]          = (row: Row, index: Int) => (row.getUdtValue(index), index + 1)

  implicit def optionReads[T: Reads]: Reads[Option[T]] =
    (row: Row, index: Int) =>
      if (row.isNull(index)) (None, index + 1)
      else {
        val (t, i) = Reads[T].read(row, index)
        Some(t) -> i
      }
}

/**
 * Note: We define instances for collections rather than A where A has evidence of a CassandraTypeMapper instance to
 * prevent an implicit resolution clash with the case class parser
 */
trait ReadsLowerPriority {
  implicit def deriveSetFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Reads[Set[A]] = {
    (row: Row, index: Int) =>
      val datatype     = row.getType(index).asInstanceOf[DefaultSetType].getElementType
      val cassandraSet = row.getSet(index, ev.classType)
      val scala        = cassandraSet.asScala.map(cas => ev.fromCassandra(cas, datatype)).toSet
      (scala, index + 1)
  }

  implicit def deriveListFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Reads[List[A]] = {
    (row: Row, index: Int) =>
      val datatype     = row.getType(index).asInstanceOf[DefaultListType].getElementType
      val cassandraSet = row.getList(index, ev.classType)
      val scala        = cassandraSet.asScala.map(cas => ev.fromCassandra(cas, datatype)).toList
      (scala, index + 1)
  }

  implicit def deriveMapFromCassandraTypeMapper[K, V](implicit
                                                      evK: CassandraTypeMapper[K],
                                                      evV: CassandraTypeMapper[V]
                                                     ): Reads[Map[K, V]] = { (row: Row, index: Int) =>
    val top          = row.getType(index).asInstanceOf[DefaultMapType]
    val keyType      = top.getKeyType
    val valueType    = top.getValueType
    val cassandraMap = row.getMap(index, evK.classType, evV.classType)
    val scala        =
      cassandraMap.asScala.map { case (k, v) =>
        (evK.fromCassandra(k, keyType), evV.fromCassandra(v, valueType))
      }.toMap
    (scala, index + 1)
  }
}

trait ReadsLowestPriority {
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