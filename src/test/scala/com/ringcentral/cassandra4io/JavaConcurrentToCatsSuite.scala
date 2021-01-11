package com.ringcentral.cassandra4io

import java.util.concurrent.{ CancellationException, CompletableFuture, CompletionStage }

import cats.effect.IO
import com.ringcentral.cassandra4io.utils.JavaConcurrentToCats._
import weaver._

import scala.util.control.ControlThrowable
import scala.concurrent.duration._

object JavaConcurrentToCatsSuite extends SimpleIOSuite {
  final implicit class IOExt[T](private val io: IO[T]) extends AnyVal {
    def when(b: => Boolean): IO[Unit] =
      IO(b).flatMap(b => if (b) io.void else IO.unit)
  }

  private def failedFuture[A](e: Throwable): CompletableFuture[A] = {
    val f = new CompletableFuture[A]
    f.completeExceptionally(e)
    f
  }
  private def canceledFuture[A]: CompletableFuture[A] = {
    val f = new CompletableFuture[A]
    f.cancel(true)
    f
  }

  private def failedAsyncFuture[A](ex: Throwable): CompletableFuture[A] =
    CompletableFuture.supplyAsync(() => throw ex)

  // doesn't cath by NonFatal
  final case class FatalFailure(msg: String) extends ControlThrowable()

  simpleTest("be lazy on CompletionStage") {
    var evaluated                     = false
    def future: CompletionStage[Unit] = CompletableFuture.supplyAsync(() => evaluated = true)
    fromJavaAsync(future).when(false).as(evaluated).map { result =>
      expect(!result)
    }
  }

  simpleTest("catch exceptions thrown by lazy block") {
    val ex                            = new Exception("boom!")
    def future: CompletionStage[Unit] = throw ex
    fromJavaAsync(future).attempt.map { error =>
      expect(error == Left(ex))
    }
  }

  simpleTest("return an `IO` that fails if `Future` fails (failedFuture)") {
    val ex                                      = new Exception(":-p")
    lazy val failedValue: CompletionStage[Unit] = failedFuture(ex)
    fromJavaAsync(failedValue).attempt.map { error =>
      expect(error == Left(ex))
    }
  }

  simpleTest("return an `IO` that fails if `Future` fails (supplyAsync)") {
    val ex                                      = new Exception(":-p")
    lazy val failedValue: CompletionStage[Unit] = failedAsyncFuture(ex)
    fromJavaAsync(failedValue).attempt.map { error =>
      expect(error == Left(ex))
    }
  }
  simpleTest("return an `IO` that fails on canceled future") {
    fromJavaAsync(canceledFuture[Int]).attempt.map { error =>
      expect(
        error.swap.getOrElse(new RuntimeException("it's not CancellationException")).isInstanceOf[CancellationException]
      )
    }
  }

  simpleTest("return an `IO` that fails on NonFatal errors (supplyAsync)") {
    val failure = FatalFailure("so fatal")
    fromJavaAsync(failedAsyncFuture(failure)).attempt.timeout(1.seconds).map { error =>
      expect(error == Left(failure))
    }
  }

  simpleTest("return an `IO` that fails on NonFatal errors (failedFuture)") {
    val failure = FatalFailure("so fatal")
    fromJavaAsync(failedFuture(failure)).attempt.timeout(1.seconds).map { error =>
      expect(error == Left(failure))
    }
  }

  simpleTest("return an `IO` that produces the value from `Future`") {
    lazy val someValue: CompletionStage[Int] = CompletableFuture.completedFuture(42)
    fromJavaAsync(someValue).map(x => expect(x == 42))
  }

  simpleTest("handle null produced by the completed `Future`") {
    lazy val someValue: CompletionStage[String] = CompletableFuture.completedFuture[String](null)
    fromJavaAsync(someValue).map(Option(_)).map(x => expect(x.isEmpty))
  }

  simpleTest("be referentially transparent") {
    var n    = 0
    val task = fromJavaAsync(CompletableFuture.supplyAsync(() => n += 1))
    for {
      _ <- task
      _ <- task
    } yield expect(n == 2)
  }

}
