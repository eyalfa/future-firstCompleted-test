package scala.concurrent

import java.util.concurrent.atomic.AtomicReference

import org.scalatest.{FunSuite, Matchers}

import scala.util.{Random, Try}
import ExecutionContext.Implicits.global
import scala.concurrent.FutureFirstCompletedSuite.FirstCompleteHandler

class FutureFirstCompletedSuite extends FunSuite with Matchers{

  val arrSz = 50 * 10000
  val numFutures = 10000

  val rng = new Random()

  test("Future.firstCompletedOf OOMs"){
    val longStandingPromise = Promise[Nothing]
    val shortTermFuture = Promise[Int]

    val futures = List.tabulate(numFutures){ i =>
      val arr = Array.tabulate(arrSz)(identity)
      val idx = rng.nextInt(arrSz)
      val f1 = Future{ arr }
      val f2 = Future.firstCompletedOf(List(longStandingPromise.future, f1))
      f2.map( arr => arr(idx))
    }
    val fSeq = Future.sequence(futures)
    val finalF = fSeq.map(_.sum)
    val res = Await.result(finalF, duration.Duration.Inf)
    res should not be 0
  }

  def firstCompletedAlt[A](fs : TraversableOnce[Future[A]]) = {
    val p = Promise[A]
    val firstCompleteHandler = new FirstCompleteHandler(p)
    fs foreach (_ onComplete firstCompleteHandler)
    p.future
  }

  test("Future.firstCompletedOf alt impl solved OOM?"){
    val longStandingPromise = Promise[Nothing]
    val shortTermFuture = Promise[Int]

    val futures = List.tabulate(numFutures){ i =>
      val arr = Array.tabulate(arrSz)(identity)
      val idx = rng.nextInt(arrSz)
      val f1 = Future{ arr }
      val f2 = firstCompletedAlt(List(longStandingPromise.future, f1))
      f2.map( arr => arr(idx))
    }
    val fSeq = Future.sequence(futures)
    val finalF = fSeq.map(_.sum)
    val res = Await.result(finalF, duration.Duration.Inf)
    res should not be 0
  }

}

object FutureFirstCompletedSuite{
  class FirstCompleteHandler[T](p : Promise[T]) extends AtomicReference[Promise[T]](p) with (Try[T] => Unit) {
    override def apply(v1: Try[T]): Unit = {
      val p = getAndSet(null)
      if( null != p ){
        p tryComplete v1
      }
    }
  }
}