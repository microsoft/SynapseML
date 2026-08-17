// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.io.IOException

import scala.util.Try

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
    val components = message.split(":", -1)
    val status = components(0)

    if (status == LightGBMConstants.FinishedStatus) {
      WorkerMessage(status, "", -1, -1, "", parseIntOrDefault(components, 1, 0),
        parseOptionalInt(components, 2))
    } else {
      val currentMessage = parseTaskMessage(components, hasStageAttempt = true)
      val legacyMessage = parseTaskMessage(components, hasStageAttempt = false)
      (currentMessage, legacyMessage) match {
        case (Some(current), Some(_)) if components.length > 1 && components(1).startsWith("[") => current
        // An unbracketed IPv6 message that fits both layouts predates the stage-attempt suffix.
        case (Some(_), Some(legacy)) => legacy
        case (Some(current), None) => current
        case (None, Some(legacy)) => legacy
        case _ => throw new IllegalArgumentException(
          s"Unexpected worker message: expected status:host:port:partitionId:executorId[:stageAttemptNumber], " +
            s"but received ${WorkerEndpoint.preview(message)}")
      }
    }
  }

  private def parseTaskMessage(components: Array[String], hasStageAttempt: Boolean): Option[WorkerMessage] = {
    val suffixFieldCount = if (hasStageAttempt) {
      TaskMessageFieldCountWithStageAttempt - 2
    } else {
      TaskMessageFieldCount - 2
    }
    val portIndex = components.length - suffixFieldCount
    if (portIndex <= 1) {
      None
    } else {
      val host = components.slice(1, portIndex).mkString(":")
      val portText = components(portIndex)
      val partitionText = components(portIndex + 1)
      val executorId = components(portIndex + 2)
      val stageAttemptText = if (hasStageAttempt) Some(components(portIndex + 3)) else None

      val endpointText = if (host.contains(":") && !host.startsWith("[")) s"[$host]:$portText" else s"$host:$portText"
      for {
        endpoint <- Try(WorkerEndpoint.parse(endpointText)).toOption
        partitionId <- Try(partitionText.toInt).toOption
        stageAttemptNumber <- stageAttemptText.map(value => Try(value.toInt).toOption).getOrElse(Some(0))
        if executorId.nonEmpty && stageAttemptNumber >= 0
      } yield WorkerMessage(components(0), endpoint.host, endpoint.port, partitionId, executorId, stageAttemptNumber)
    }
  }

  def format(message: TaskMessageInfo, stageAttemptNumber: Int): String = {
    // Validated bracketing keeps an IPv6 host unambiguous and keeps a host that carries a control
    // character or a delimiter out of the line protocol entirely.
    val endpoint = WorkerEndpoint.wireString(message.taskHost, message.localListenPort)
    s"${message.status}:$endpoint:${message.partitionId}:${message.executorId}:" + stageAttemptNumber
  }

  def formatFinished(stageAttemptNumber: Int, barrierTaskCount: Int): String =
    s"${LightGBMConstants.FinishedStatus}:$stageAttemptNumber:$barrierTaskCount"

  private def parseIntOrDefault(components: Array[String], index: Int, default: Int): Int =
    if (components.length > index && components(index).nonEmpty) components(index).toInt else default

  private def parseOptionalInt(components: Array[String], index: Int): Option[Int] =
    if (components.length > index && components(index).nonEmpty) Some(components(index).toInt) else None
}
