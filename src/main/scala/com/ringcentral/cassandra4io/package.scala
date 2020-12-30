package com.ringcentral

import java.util.Optional

package object cassandra4io {
  // for cross-build between scala 2.13 and 2.12
  implicit class RichOptional[A](o: Optional[A]) {
    def asScala: Option[A] = if (o.isPresent) Some(o.get()) else None
  }
}
