// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.logging.common.SASScrubber
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import spray.json.DefaultJsonProtocol._
import spray.json._
import scala.collection.JavaConverters._
import scala.collection.mutable

case class RequiredLogFields(uid: String,
                             className: String,
                             method: String) {
  def toMap: Map[String, String] = {
    Map(
      "uid" -> uid,
      "className" -> className,
      "method" -> method,
      "libraryVersion" -> BuildInfo.version,
      "libraryName" -> "SynapseML",
      "protocolVersion" -> "0.0.1"
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
    "trident.activity.id" -> "activityId",
    "trident.artifact.type" -> "artifactType",
    "trident.tenant.id" -> "tenantId"
  )

  private[ml] val LoggedClasses: mutable.Set[String] = mutable.HashSet[String]()

  private[ml] def getHadoopConfEntries: Map[String, String] = {
    val hc = SparkContext.getOrCreate().hadoopConfiguration
    //noinspection ScalaStyle
    HadoopKeysToLog.flatMap { case (field, name) =>
      Option(hc.get(field)).map { v: String => (name, v) }
    }.toMap
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

}

trait SynapseMLLogging extends Logging {

  val uid: String

  protected def logBase(methodName: String, numCols: Option[Int]): Unit = {
    logBase(
      RequiredLogFields(uid, getClass.toString, methodName).toMap
        .++(SynapseMLLogging.getHadoopConfEntries)
        .++(numCols.toSeq.map(c => "numCols" -> c.toString))
    )
  }

  protected def logBase(info: Map[String, String]): Unit = {
    SynapseMLLogging.LoggedClasses.add(info("className"))
    logInfo(info.toJson.compactPrint)
  }

  protected def logErrorBase(methodName: String, e: Exception): Unit = {
    val message: String = RequiredLogFields(uid, getClass.toString, methodName).toMap
      .++(new RequiredErrorFields(e).toMap)
      .toJson.compactPrint
    logError(message, e)
  }

  def logClass(): Unit = {
    logBase("constructor", None)
  }

  def logFit[T](f: => T, columns: Int): T = {
    logVerb("fit", f, Some(columns))
  }

  def logTransform[T](f: => T, columns: Int): T = {
    logVerb("transform", f, Some(columns))
  }

  def logVerb[T](verb: String, f: => T, columns: Option[Int] = None): T = {
    logBase(verb, columns)
    try {
      f
    } catch {
      case e: Exception =>
        logErrorBase(verb, e)
        throw e
    }
  }
}
