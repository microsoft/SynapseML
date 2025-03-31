// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.logging.common.SASScrubber
import com.microsoft.azure.synapse.ml.logging.fabric.CertifiedEventClient.logToCertifiedEvents
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class RequiredLogFields(uid: String,
                             className: String,
                             method: String) {
  def toMap: Map[String, String] = {
    Map(
      "modelUid" -> uid,
      "className" -> className,
      "method" -> method,
      "libraryVersion" -> BuildInfo.version,
      "libraryName" -> "SynapseML",
      "protocolVersion" -> "0.0.1" // which version of the logging protocol this schema is,
    )
  }
}

case class RequiredErrorFields(errorType: String,
                               errorMessage: String) {

  def this(e: Exception) = {
    this(e.getClass.getName, e.getMessage)
  }

  def toMap: Map[String, String] = {
    Map(
      "errorType" -> errorType,
      "errorMessage" -> errorType
    )
  }
}


object SynapseMLLogging extends Logging {

  val HadoopKeysToLog: mutable.Map[String, String] = mutable.Map(
    "trident.artifact.id" -> "artifactId",
    "trident.workspace.id" -> "workspaceId",
    "trident.capacity.id" -> "capacityId",
    "trident.artifact.workspace.id" -> "artifactWorkspaceId",
    "trident.lakehouse.id" -> "lakehouseId",
    "trident.activity.id" -> "livyId",
    "trident.artifact.type" -> "artifactType",
    "trident.tenant.id" -> "tenantId"
  )

  private[ml] val LoggedClasses: mutable.Set[String] = mutable.HashSet[String]()

  private[ml] def getHadoopConfEntries: Map[String, String] = {
    SparkSession.getActiveSession.map { spark =>
      val hc = spark.sparkContext.hadoopConfiguration
      //noinspection ScalaStyle
      HadoopKeysToLog.flatMap { case (field, name) =>
        Option(hc.get(field)).map { v: String => (name, v) }
      }.toMap
    }.getOrElse(Map())
  }

  def logExternalInfo(uid: String,
                      className: String,
                      methodName: String,
                      extraFields: java.util.HashMap[String, String]): Unit = {
    val mapToPrint = RequiredLogFields(uid, className, methodName).toMap
      .++(extraFields.asScala.toMap)
      .++(getHadoopConfEntries)

    SynapseMLLogging.LoggedClasses.add(className)
    logInfo(mapToPrint.toJson.compactPrint)
  }

  def logMessage(message: String): Unit = {
    logInfo(SASScrubber.scrub(message))
  }

  override def logDebug(msg: => String): Unit = super.logDebug(SASScrubber.scrub(msg))
}

trait SynapseMLLogging extends Logging {

  val uid: String

  protected def getPayload(methodName: String,
                           numCols: Option[Int],
                           executionSeconds: Option[Double],
                           exception: Option[Exception]
                          ): Map[String, String] = {
    val info = RequiredLogFields(uid, getClass.toString, methodName).toMap
      .++(SynapseMLLogging.getHadoopConfEntries)
      .++(numCols.toSeq.map(nc =>
        "dfInfo" -> Map("input" -> Map("numCols" -> nc.toString)).toJson.compactPrint
      ).toMap)
      .++(executionSeconds.toSeq.map(s => "executionSeconds" -> s.toString).toMap)
      .++(exception.map(e => new RequiredErrorFields(e).toMap).getOrElse(Map()))

    SynapseMLLogging.LoggedClasses.add(info("className"))
    info
  }

  protected def logBase(methodName: String,
                        numCols: Option[Int],
                        executionSeconds: Option[Double],
                        featureName: Option[String]
                       ): Unit = {
    logBase(getPayload(
      methodName,
      numCols,
      executionSeconds,
      None), featureName)
  }

  protected def logBase(info: Map[String, String], featureName: Option[String]): Unit = {
    if (featureName.isDefined) {
      Future {
        logToCertifiedEvents(
          info("libraryName"),
          featureName.get
        )
      }.failed.map {
        case e: Exception => logErrorBase("certifiedEventLogging", e)
      }
    }

    logInfo(info.toJson.compactPrint)
  }

  protected def logErrorBase(methodName: String, e: Exception): Unit = {
    logError(
      getPayload(methodName, None, None, Some(e)).toJson.compactPrint,
      e)
  }

  def logClass(featureName: String): Unit = {
    logBase("constructor", None, None, Some(featureName))
  }

  def logFit[T](f: => T, columns: Int): T = {
    logVerb("fit", f, Some(columns))
  }

  def logTransform[T](f: => T, columns: Int): T = {
    logVerb("transform", f, Some(columns))
  }

  def logVerb[T](verb: String, f: => T, columns: Option[Int] = None): T = {
    val startTime = System.nanoTime()
    try {
      val ret = f
      logBase(verb, columns, Some((System.nanoTime() - startTime) / 1e9), None)
      ret
    } catch {
      case e: Exception =>
        logErrorBase(verb, e)
        throw e
    }
  }
}
