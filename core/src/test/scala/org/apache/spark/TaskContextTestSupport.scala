// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem

import java.util.Properties

object TaskContextTestSupport {
  class ControlledContext(onMonitorCheck: () => Unit)
    extends TaskContextImpl(0, 0, 0, 0, 0, 1, Option.empty[TaskMemoryManager].orNull,
      new Properties(), Option.empty[MetricsSystem].orNull, TaskMetrics.empty, 1) {
    override def isInterrupted(): Boolean = {
      if (Thread.currentThread().getName == "synapseml-http-cancellation-monitor") onMonitorCheck()
      super.isInterrupted()
    }

    def interrupt(): Unit = markInterrupted("Synthetic task cancellation")

    def complete(): Unit = markTaskCompleted(None)
  }

  def withContext[T](context: TaskContext)(action: => T): T = {
    val previous = TaskContext.get()
    TaskContext.setTaskContext(context)
    try action finally TaskContext.setTaskContext(previous)
  }
}
