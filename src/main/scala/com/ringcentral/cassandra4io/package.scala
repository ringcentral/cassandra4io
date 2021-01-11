package com.ringcentral

import java.util.Optional

package object cassandra4io {
  // for cross-build between scala 2.13 and 2.12
  final implicit class RichOptional[A](private val o: Optional[A]) extends AnyVal {
    def asScala: Option[A] = if (o.isPresent) Some(o.get()) else None
  }
}
