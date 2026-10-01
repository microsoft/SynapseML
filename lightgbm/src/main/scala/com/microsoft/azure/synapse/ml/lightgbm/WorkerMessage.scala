// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.io.IOException

import scala.util.Try

/**
  * The line protocol tasks use to report themselves to the driver while the LightGBM network
  * topology is being assembled.
  *
  * A task report is
  * `status:host:port:partitionId:executorId[:stageAttemptNumber[:stageId[:taskAttemptId[:attemptNumber]]]]`,
  * and the barrier-stage marker is `finished:stageAttemptNumber[:barrierTaskCount]`. The trailing
  * fields are parsed defensively so a message written without them is still understood.
  */
private[lightgbm] final case class WorkerMessage(status: String,
                                                 taskHost: String,
                                                 localListenPort: Int,
                                                 partitionId: Int,
                                                 executorId: String,
                                                 stageAttemptNumber: Int,
                                                 barrierTaskCount: Option[Int] = None,
                                                 stageId: Option[Int] = None,
                                                 taskAttemptId: Option[Long] = None,
                                                 attemptNumber: Option[Int] = None) {
  val isForTraining: Boolean = status == LightGBMConstants.EnabledTask
  val isForLoadOnly: Boolean = status == LightGBMConstants.IgnoreStatus
  val isFinished: Boolean = status == LightGBMConstants.FinishedStatus

  def toTaskMessage: TaskMessageInfo =
    TaskMessageInfo(status, taskHost, localListenPort, partitionId, executorId)

  def identitySummary: String =
    WorkerTaskIdentity(stageId, stageAttemptNumber, taskAttemptId, attemptNumber).summary(partitionId)
}

private[lightgbm] final case class WorkerTaskIdentity(stageId: Option[Int],
                                                      stageAttemptNumber: Int,
                                                      taskAttemptId: Option[Long],
                                                      attemptNumber: Option[Int]) {
  def summary(partitionId: Int): String = {
    def render[T](value: Option[T]): String = value.map(_.toString).getOrElse("unavailable")
    s"stageId=${render(stageId)}, stageAttemptNumber=$stageAttemptNumber, partitionId=$partitionId, " +
      s"taskAttemptId=${render(taskAttemptId)}, attemptNumber=${render(attemptNumber)}"
  }
}

private[lightgbm] object WorkerMessage {
  private val TaskMessageFieldCount = 5
  private val MaxTaskMetadataFieldCount = 4

  private final case class ParsedTaskMessage(metadataFieldCount: Int, message: WorkerMessage)

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
      val candidates = (0 to MaxTaskMetadataFieldCount).flatMap { metadataFieldCount =>
        parseTaskMessage(components, metadataFieldCount).map(ParsedTaskMessage(metadataFieldCount, _))
      }
      if (candidates.isEmpty) {
        throw new IllegalArgumentException(
          s"Unexpected worker message: expected status:host:port:partitionId:executorId" +
            "[:stageAttemptNumber[:stageId[:taskAttemptId[:attemptNumber]]]], " +
            s"but received ${WorkerEndpoint.preview(message)}")
      } else {
        val hasUnambiguousHost = components.length > 1 && (components(1).startsWith("[") ||
          candidates.exists(candidate => !candidate.message.taskHost.contains(":")))
        // Current senders bracket IPv6, so their host boundary is unambiguous and the longest valid
        // suffix preserves every field. Only legacy unbracketed IPv6 can require the shortest layout.
        val selected = if (hasUnambiguousHost) candidates.maxBy(_.metadataFieldCount)
        else candidates.minBy(_.metadataFieldCount)
        selected.message
      }
    }
  }

  private def parseTaskMessage(components: Array[String], metadataFieldCount: Int): Option[WorkerMessage] = {
    val suffixFieldCount = TaskMessageFieldCount - 2 + metadataFieldCount
    val portIndex = components.length - suffixFieldCount
    if (portIndex <= 1) {
      None
    } else {
      val host = components.slice(1, portIndex).mkString(":")
      val portText = components(portIndex)
      val partitionText = components(portIndex + 1)
      val executorId = components(portIndex + 2)
      val metadata = components.slice(portIndex + 3, components.length)

      val endpointText = if (host.contains(":") && !host.startsWith("[")) s"[$host]:$portText" else s"$host:$portText"
      for {
        endpoint <- Try(WorkerEndpoint.parse(endpointText)).toOption
        partitionId <- Try(partitionText.toInt).toOption
        stageAttemptNumber <- parseOptionalMetadataInt(metadata, 0, 0)
        stageId <- parseOptionalMetadataInt(metadata, 1)
        taskAttemptId <- parseOptionalMetadataLong(metadata, 2)
        attemptNumber <- parseOptionalMetadataInt(metadata, 3)
        if executorId.nonEmpty && stageAttemptNumber >= 0 && stageId.forall(_ >= 0) &&
          taskAttemptId.forall(_ >= 0) && attemptNumber.forall(_ >= 0)
      } yield WorkerMessage(components(0), endpoint.host, endpoint.port, partitionId, executorId,
        stageAttemptNumber, stageId = stageId, taskAttemptId = taskAttemptId, attemptNumber = attemptNumber)
    }
  }

  def format(message: TaskMessageInfo, stageAttemptNumber: Int): String = {
    // Validated bracketing keeps an IPv6 host unambiguous and keeps a host that carries a control
    // character or a delimiter out of the line protocol entirely.
    val endpoint = WorkerEndpoint.wireString(message.taskHost, message.localListenPort)
    validateExecutorId(message.executorId)
    s"${message.status}:$endpoint:${message.partitionId}:${message.executorId}:" + stageAttemptNumber
  }

  /** The executor id is sent verbatim in this line protocol and in the driver's executor=partitions list. */
  private def validateExecutorId(executorId: String): Unit = {
    val problem =
      if (executorId.isEmpty) Some("it is empty")
      else if (executorId.contains(":")) Some("':' is reserved by the wire protocol")
      else if (executorId.contains("=")) Some("'=' is reserved by the executor partition list")
      else if (executorId.exists(Character.isISOControl)) Some("it contains a control character")
      else None
    problem.foreach(reason => throw new IllegalArgumentException(s"Invalid LightGBM executor id: $reason"))
  }

  def format(message: TaskMessageInfo, identity: WorkerTaskIdentity): String = {
    val current = format(message, identity.stageAttemptNumber)
    Seq(identity.stageId, identity.taskAttemptId, identity.attemptNumber)
      .foldLeft(current)((result, value) => s"$result:${value.map(_.toString).getOrElse("")}")
  }

  def formatFinished(stageAttemptNumber: Int, barrierTaskCount: Int): String =
    s"${LightGBMConstants.FinishedStatus}:$stageAttemptNumber:$barrierTaskCount"

  private def parseIntOrDefault(components: Array[String], index: Int, default: Int): Int =
    if (components.length > index && components(index).nonEmpty) components(index).toInt else default

  private def parseOptionalInt(components: Array[String], index: Int): Option[Int] =
    if (components.length > index && components(index).nonEmpty) Some(components(index).toInt) else None

  private def parseOptionalMetadataInt(metadata: Array[String], index: Int): Option[Option[Int]] =
    if (metadata.length <= index || metadata(index).isEmpty) Some(None)
    else Try(metadata(index).toInt).toOption.map(Some(_))

  private def parseOptionalMetadataInt(metadata: Array[String], index: Int, default: Int): Option[Int] =
    if (metadata.length <= index || metadata(index).isEmpty) Some(default)
    else Try(metadata(index).toInt).toOption

  private def parseOptionalMetadataLong(metadata: Array[String], index: Int): Option[Option[Long]] =
    if (metadata.length <= index || metadata(index).isEmpty) Some(None)
    else Try(metadata(index).toLong).toOption.map(Some(_))
}
