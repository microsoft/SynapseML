// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.contentunderstanding

import org.apache.spark.internal.Logging
import org.apache.spark.TaskContext

import scala.util.control.NonFatal

private[contentunderstanding] object ContentUnderstandingLogging extends Logging {
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

  def sharedStop(error: ContentUnderstandingException): Unit = safely {
    val category = error match {
      case shared: ContentUnderstandingProtocol.SharedFailure => shared.category
      case _ => "unknown"
    }
    val context = Option(TaskContext.get()).map(task =>
      s"stageId=${task.stageId()} partitionId=${task.partitionId()} taskAttemptId=${task.taskAttemptId()}")
      .getOrElse("taskContext=unavailable")
    logWarning(s"Content Understanding SharedConfigurationFailure (HTTP ${error.response.httpStatus}): " +
      s"category=$category $context; new submissions stopped in this partition.")
  }
}
