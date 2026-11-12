// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.blocking

private[contentunderstanding] object ContentUnderstandingBudget {
  val CancellationPollMs: Long = 50L
}

/** Thread-confined operation budget; concurrent rows must use separate instances. */
private[contentunderstanding] class ContentUnderstandingBudget(
    durationMs: Int, nanoTime: () => Long = () => System.nanoTime()) {
  private var started = nanoTime()

  def mark(): Long = nanoTime()

  // Exclude only the interval since this mark; previously charged time remains charged.
  def refundSince(mark: Long): Unit = {
    started += nanoTime() - mark
  }

  def remainingMs: Long = math.max(0L,
    durationMs.toLong - TimeUnit.NANOSECONDS.toMillis(nanoTime() - started))

  def pause(delay: Long): Unit = pause(delay, () => ())

  // Tick failures propagate to the caller, allowing a retry's shared-stop signal to abort this wait.
  def pause(delay: Long, onTick: () => Unit): Unit = blocking {
    ContentUnderstandingProtocol.checkInterrupted()
    onTick()
    if (delay > 0) {
      try {
        val start = System.nanoTime()
        @tailrec
        def sleep(): Unit = {
          ContentUnderstandingProtocol.checkInterrupted()
          onTick()
          val remaining = delay - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
          if (remaining > 0) {
            Thread.sleep(math.min(remaining, ContentUnderstandingBudget.CancellationPollMs))
            sleep()
          }
        }
        sleep()
      } catch {
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw error
      }
    }
  }
}
