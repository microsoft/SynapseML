// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.io.IOException

/**
  * The line protocol tasks use to report themselves to the driver while the LightGBM network
  * topology is being assembled.
  *
  * A task report is `status:host:port:partitionId:executorId[:stageAttemptNumber]`, and the
  * barrier-stage marker is `finished:stageAttemptNumber[:barrierTaskCount]`. The trailing
  * fields are parsed defensively so a message written without them is still understood.
  */
private[lightgbm] final case class WorkerMessage(status: String,
                                                 taskHost: String,
                                                 localListenPort: Int,
                                                 partitionId: Int,
                                                 executorId: String,
                                                 stageAttemptNumber: Int,
                                                 barrierTaskCount: Option[Int] = None) {
  val isForTraining: Boolean = status == LightGBMConstants.EnabledTask
  val isForLoadOnly: Boolean = status == LightGBMConstants.IgnoreStatus
  val isFinished: Boolean = status == LightGBMConstants.FinishedStatus

  def toTaskMessage: TaskMessageInfo =
    TaskMessageInfo(status, taskHost, localListenPort, partitionId, executorId)
}

private[lightgbm] object WorkerMessage {
  private val TaskMessageFieldCount = 5
  private val TaskMessageFieldCountWithStageAttempt = 6

  def parse(message: String): WorkerMessage = {
    if (message == null) {
      throw new IOException("Worker closed the connection before sending a status message")
    }
    val components = message.split(":")
    val status = components(0)

    if (status == LightGBMConstants.FinishedStatus) {
      WorkerMessage(status, "", -1, -1, "", parseIntOrDefault(components, 1, 0),
        parseOptionalInt(components, 2))
    } else {
      if (components.length != TaskMessageFieldCount && components.length != TaskMessageFieldCountWithStageAttempt) {
        throw new Exception(s"Unexpected message: $message")
      }

      WorkerMessage(status, components(1), components(2).toInt, components(3).toInt, components(4),
        parseIntOrDefault(components, TaskMessageFieldCount, 0))
    }
  }

  def format(message: TaskMessageInfo, stageAttemptNumber: Int): String =
    s"${message.toString}:$stageAttemptNumber"

  def formatFinished(stageAttemptNumber: Int, barrierTaskCount: Int): String =
    s"${LightGBMConstants.FinishedStatus}:$stageAttemptNumber:$barrierTaskCount"

  private def parseIntOrDefault(components: Array[String], index: Int, default: Int): Int =
    if (components.length > index) components(index).toInt else default

  private def parseOptionalInt(components: Array[String], index: Int): Option[Int] =
    if (components.length > index) Some(components(index).toInt) else None
}
