package com.ringcentral.cassandra4io.utils

import java.util.concurrent.{ CompletionException, CompletionStage }

import cats.effect.{ Async, ContextShift, Sync }
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.apply._

import scala.concurrent.ExecutionException

object JavaConcurrentToCats {

  def unwrapJavaConcurrentThrowable: PartialFunction[Throwable, Throwable] = {
    case e @ (_: CompletionException | _: ExecutionException) if e.getCause ne null =>
      e.getCause
  }

  def fromJavaAsync[F[_]: Async: ContextShift, T](cs: => CompletionStage[T]): F[T] =
    Sync[F]
      .delay(cs)
      .flatMap { cs =>
        val cf = cs.toCompletableFuture
        if (cf.isDone) {
          Sync[F].catchNonFatal(cf.get())
        } else {
          Async[F].async[T] { cb =>
            cs.handle[Unit] { (value: T, t: Throwable) =>
              if (t eq null) cb(Right(value))
              else cb(Left(t))
            }
          } <* ContextShift[F].shift // shift from driver I/O thread pool
        }
      }
      .adaptErr(unwrapJavaConcurrentThrowable)
}
