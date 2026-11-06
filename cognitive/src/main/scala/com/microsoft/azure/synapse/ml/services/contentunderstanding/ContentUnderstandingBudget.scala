// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import java.util.concurrent.TimeUnit

private[contentunderstanding] class ContentUnderstandingBudget(durationMs: Int) {
  private val started = System.nanoTime()

  def remainingMs: Long = math.max(0L,
    durationMs.toLong - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started))

  def pause(delay: Long): Unit = {
    ContentUnderstandingProtocol.checkInterrupted()
    if (delay > 0) {
      try {
        Thread.sleep(delay)
      } catch {
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw error
      }
    }
  }
}
