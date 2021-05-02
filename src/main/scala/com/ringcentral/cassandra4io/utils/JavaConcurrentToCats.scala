package com.ringcentral.cassandra4io.utils

import cats.effect.{ Async, Sync }
import cats.syntax.functor._

import java.util.concurrent.CompletionStage

object JavaConcurrentToCats {

  def fromJavaAsync[F[_]: Async, T](cs: => CompletionStage[T]): F[T] =
    Async[F].fromCompletableFuture(Sync[F].delay(cs).map(_.toCompletableFuture))
}
