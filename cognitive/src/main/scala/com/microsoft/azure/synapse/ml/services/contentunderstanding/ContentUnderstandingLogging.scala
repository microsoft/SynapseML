// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import com.microsoft.azure.synapse.ml.io.http.AsyncClient
import org.apache.spark.internal.Logging

import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

private[contentunderstanding] object ContentUnderstandingLogging extends Logging {
  val ThrottleReportIntervalMs: Long = 60000L

  class ThrottleReporter(nanoTime: () => Long = () => System.nanoTime()) extends Serializable {
    private var retryCount = 0L
    private var lastReport = Option.empty[Long]

    def retrying(method: String, waitMs: Long): Unit = synchronized {
      retryCount += 1
      report(method, waitMs)
    }

    def waiting(method: String, waitMs: Long): Unit = synchronized {
      report(method, waitMs)
    }

    private def report(method: String, waitMs: Long): Unit = {
      val now = nanoTime()
      if (lastReport.forall(previous =>
        TimeUnit.NANOSECONDS.toMillis(now - previous) >= ThrottleReportIntervalMs)) {
        throttled(method, retryCount, waitMs)
        lastReport = Some(now)
      }
    }
  }

  private def taskDescription: String = AsyncClient.currentTaskContext.map(task =>
    s"stageId=${task.stageId()} partitionId=${task.partitionId()} taskAttemptId=${task.taskAttemptId()}")
    .getOrElse("taskContext=unavailable")

  def safely(action: => Unit): Unit = {
    try {
      action
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
      case NonFatal(_) => ()
    }
  }

  def cleanup(operation: String, error: Throwable): Unit = safely {
    logWarning(s"Content Understanding $operation cleanup failed (${error.getClass.getName}).")
  }

  def throttled(method: String, retryCount: Long, waitMs: Long): Unit = safely {
    logWarning(s"Content Understanding throttled: method=$method retryCount=$retryCount " +
      s"waitMs=$waitMs $taskDescription")
  }

  def sharedStop(error: ContentUnderstandingException): Unit = safely {
    val category = error match {
      case shared: ContentUnderstandingProtocol.SharedFailure => shared.category
      case _ => "unknown"
    }
    logWarning(s"Content Understanding SharedConfigurationFailure (HTTP ${error.response.httpStatus}): " +
      s"category=$category $taskDescription; new submissions stopped in this partition.")
  }
}
