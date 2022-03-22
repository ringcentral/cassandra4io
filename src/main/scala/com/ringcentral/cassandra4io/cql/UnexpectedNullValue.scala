package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue

sealed trait UnexpectedNullValue extends Throwable

class UnexpectedNullValueInColumn(row: Row, index: Int) extends RuntimeException() with UnexpectedNullValue {
  override def getMessage: String = {
    val cl       = row.getColumnDefinitions.get(index)
    val table    = cl.getTable.toString
    val column   = cl.getName.toString
    val keyspace = cl.getKeyspace.toString
    val tpe      = cl.getType.asCql(true, true)

    s"Read NULL value from $keyspace.$table column $column expected $tpe. Row ${row.getFormattedContents}"
  }
}

class UnexpectedNullValueInUdt(row: Row, index: Int, udt: UdtValue, fieldName: String)
    extends RuntimeException()
    with UnexpectedNullValue {
  override def getMessage: String = {
    val cl       = row.getColumnDefinitions.get(index)
    val table    = cl.getTable.toString
    val column   = cl.getName.toString
    val keyspace = cl.getKeyspace.toString
    val tpe      = cl.getType.asCql(true, true)

    val udtTpe = udt.getType(fieldName)

    s"Read NULL value from $keyspace.$table inside UDT column $column with type $tpe. NULL value in $fieldName, expected type $udtTpe. Row ${row.getFormattedContents}"
  }

}

object UnexpectedNullValueInUdt {

  private[cql] case class NullValueInUdt(udtValue: UdtValue, fieldName: String) extends Throwable("", null, true, false)

}
