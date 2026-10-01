// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import scala.concurrent.duration.{Duration, SECONDS}

/** Thrown when training without barrier execution mode stops waiting for tasks that never reported. */
class LightGBMMissingTasksException private[lightgbm] (message: String, cause: Throwable)
  extends Exception(message, cause)

private[lightgbm] object LightGBMMissingTasksException {
  private val MaxListedPartitions = 20
  private val MaxDriverWaitSeconds = 30

  /** How long a failed training job waits to learn whether the driver timed out first. */
  val MaxDriverWait: Duration = Duration(MaxDriverWaitSeconds, SECONDS)

  def apply(numTasks: Int,
            missingPartitions: Seq[Int],
            timeoutSeconds: Double,
            cause: Throwable): LightGBMMissingTasksException = {
    val listed = missingPartitions.take(MaxListedPartitions).mkString(", ")
    val unlisted = missingPartitions.size - MaxListedPartitions
    val missingList = if (unlisted > 0) s"$listed, and $unlisted more" else listed
    val timeoutText = if (timeoutSeconds.isWhole) timeoutSeconds.toLong.toString else timeoutSeconds.toString
    val message =
      s"The LightGBM driver received network reports from ${numTasks - missingPartitions.size} of $numTasks " +
        s"training tasks, then stopped waiting because no task connected for $timeoutText seconds. Missing " +
        s"partitions: $missingList. Without barrier execution mode, all numTasks tasks must run at the same " +
        "time. Check that numTasks is no larger than the number of tasks Spark can run at once (each " +
        "executor's cores divided by spark.task.cpus, summed across executors), and that no executor was " +
        "lost or still starting. If a task failed before reporting, its own error is the root cause. Later " +
        "\"could not reach the driver\" or \"connection refused\" errors from other tasks are a result of " +
        "this timeout."
    new LightGBMMissingTasksException(message, cause)
  }
}
