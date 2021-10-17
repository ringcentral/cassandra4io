package com.ringcentral.cassandra4io.cql

/**
 * This type is used by FromUdtValue and ToUdtValue to decide whether to utilize schema data
 * when reading and writing data from the Datastax DataType
 */
private[cql] sealed trait FieldName
object FieldName {
  case object Unused                       extends FieldName
  final case class Labelled(value: String) extends FieldName
}
