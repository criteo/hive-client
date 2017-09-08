package com.criteo.hive

import java.time.Duration
import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.{Future, Promise}

/**
  * Hive client utilities.
  */
object Utils {
  private object Timeout {
    private val scheduler = Executors.newScheduledThreadPool(1)
    def apply(timeout: Duration): Future[Unit] = {
      val p = Promise[Unit]()
      scheduler.schedule(new Runnable { def run = p.success(()) }, timeout.toMillis, TimeUnit.MILLISECONDS)
      p.future
    }
  }

  def timeout(timeout: Duration) = {
    Timeout(timeout)
  }
}
