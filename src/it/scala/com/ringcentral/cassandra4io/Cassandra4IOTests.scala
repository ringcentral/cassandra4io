package com.ringcentral.cassandra4io

import com.ringcentral.cassandra4io.cql.CqlSuite
import weaver.IOSuite

object Cassandra4IOTests extends IOSuite with CassandraTestsSharedInstances with CassandraSessionSuite with CqlSuite
