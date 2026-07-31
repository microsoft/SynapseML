// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nbtest

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsString, JsValue}

import java.util.concurrent.TimeoutException
import scala.util.control.NonFatal

private[nbtest] object DatabricksClusterStartup {
  private val RetriableTerminationCodes = Set(
    "CLOUD_PROVIDER_RESOURCE_STOCKOUT",
    "INSTANCE_GROUP_MAX_CAPACITY_REACHED",
    "INSTANCE_POOL_MAX_CAPACITY_REACHED"
  )

  final case class ClusterStatus(
      state: String,
      terminationCode: Option[String] = None,
      message: Option[String] = None)

  final class ClusterStartupException(
      val clusterId: String,
      val status: ClusterStatus)
    extends RuntimeException(clusterStartupFailureMessage(clusterId, status)) {

    def isRetriable: Boolean = status.terminationCode.exists(RetriableTerminationCodes.contains)
  }

  private def clusterStartupFailureMessage(clusterId: String, status: ClusterStatus): String = {
    val details = Seq(
      status.terminationCode.map(code => s"termination code $code"),
      status.message.map(message => s"message: $message")
    ).flatten
    val suffix = if (details.isEmpty) "" else details.mkString(" (", ", ", ")")
    s"Cluster $clusterId entered terminal state ${status.state}$suffix"
  }

  def parseClusterStatus(clusterObj: JsValue): ClusterStatus = {
    val fields = clusterObj.asJsObject.fields
    val terminationFields = fields.get("termination_reason")
      .collect { case JsObject(values) => values }
      .getOrElse(Map.empty[String, JsValue])
    ClusterStatus(
      fields("state").convertTo[String],
      terminationFields.get("code").collect { case JsString(value) => value },
      fields.get("state_message").collect { case JsString(value) => value }.filter(_.nonEmpty)
    )
  }

  def waitForClusterActive(
      clusterId: String,
      statusProvider: String => ClusterStatus,
      pollDelays: Seq[Int] = Seq.fill(60 * 10)(1000),
      sleep: Long => Unit = millis => Thread.sleep(millis)): Unit = {
    def await(delays: List[Int], lastStatus: Option[ClusterStatus]): Unit = {
      delays match {
        case Nil =>
          val lastState = lastStatus.map(_.state).getOrElse("unavailable")
          throw new TimeoutException(s"Cluster $clusterId did not become active; last state was $lastState")
        case delay :: remainingDelays =>
          val status = statusProvider(clusterId)
          println(s"Cluster State: ${status.state}")
          status.state match {
            case "RUNNING" => ()
            case "TERMINATED" | "ERROR" | "UNKNOWN" =>
              throw new ClusterStartupException(clusterId, status)
            case _ =>
              sleep(delay.toLong)
              await(remainingDelays, Some(status))
          }
      }
    }
    await(pollDelays.toList, None)
  }

  def createActiveCluster(
      createCluster: Int => String,
      waitForActive: String => Unit,
      cleanupCluster: String => Unit,
      maxAttempts: Int = 3,
      retryDelayMs: Long = 30 * 1000L,
      sleep: Long => Unit = millis => Thread.sleep(millis)): String = {
    require(maxAttempts > 0, "maxAttempts must be positive")
    def attemptStartup(attempt: Int): String = {
      val clusterId = createCluster(attempt)
      def retryStartup(failure: Throwable): String = {
        cleanupFailedCluster(clusterId, cleanupCluster, failure)
        if (attempt == maxAttempts) {
          throw failure
        }
        println(
          s"Cluster $clusterId failed to start; retrying after ${retryDelayMs / 1000} seconds")
        sleep(retryDelayMs)
        attemptStartup(attempt + 1)
      }

      try {
        waitForActive(clusterId)
        clusterId
      } catch {
        case failure: ClusterStartupException if failure.isRetriable =>
          retryStartup(failure)
        case failure: TimeoutException =>
          retryStartup(failure)
        case NonFatal(failure) =>
          cleanupFailedCluster(clusterId, cleanupCluster, failure)
          throw failure
      }
    }
    attemptStartup(1)
  }

  private def cleanupFailedCluster(
      clusterId: String,
      cleanupCluster: String => Unit,
      startupFailure: Throwable): Unit = {
    try {
      cleanupCluster(clusterId)
    } catch {
      case NonFatal(cleanupFailure) =>
        startupFailure.addSuppressed(cleanupFailure)
        println(
          s"Failed to clean up cluster $clusterId after startup failure: " +
            cleanupFailure.getMessage)
    }
  }

  def gpuWorkerCount(attempt: Int): Int = {
    if (attempt == 1) 2 else 1
  }
}
