// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec

private[contentunderstanding] object ContentUnderstandingBudget {
  val CancellationPollMs: Long = 50L
}

private[contentunderstanding] class ContentUnderstandingBudget(durationMs: Int) {
  private val started = System.nanoTime()

  def remainingMs: Long = math.max(0L,
    durationMs.toLong - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))

  def pause(delay: Long): Unit = {
    ContentUnderstandingProtocol.checkInterrupted()
    if (delay > 0) {
      try {
        val start = System.nanoTime()
        @tailrec
        def sleep(): Unit = {
          ContentUnderstandingProtocol.checkInterrupted()
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
