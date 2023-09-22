// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.logging.common.CommonUtils
import com.microsoft.azure.synapse.ml.logging.common.SASScrubber
import com.microsoft.azure.synapse.ml.logging.Usage.FeatureUsagePayload
import com.microsoft.azure.synapse.ml.logging.Usage.UsageTelemetry.reportUsage
import org.apache.spark.internal.Logging
import scala.collection.JavaConverters._
import scala.collection.mutable
import spray.json.{DefaultJsonProtocol, RootJsonFormat, NullOptions}

case class SynapseMLLogInfo(uid: String,
                            className: String,
                            method: String,
                            buildVersion: String,
                            columns: Option[Int] = None)

object LogJsonProtocol extends DefaultJsonProtocol with NullOptions
{
  implicit val LogFormat: RootJsonFormat[SynapseMLLogInfo] = jsonFormat5(SynapseMLLogInfo)
}

import com.microsoft.azure.synapse.ml.logging.LogJsonProtocol._
import spray.json._

object SynapseMLLogging extends Logging {

  private[ml] val LoggedClasses: mutable.Set[String] = mutable.HashSet[String]()

  def logExternalInfo(uid: String,
                      className: String,
                      methodName: String,
                      extraFields: java.util.HashMap[String, String]): Unit = {
    val mapToPrint = Map(
      "uid" -> uid,
      "className" -> className,
      "methodName" -> methodName).++(extraFields.asScala.toMap)

    SynapseMLLogging.LoggedClasses.add(className)
    logInfo(s"metrics/ ${mapToPrint.toJson.compactPrint}")
  }

  def logMessage(message: String): Unit = {
    logInfo(SASScrubber.scrub(message))
  }

}

trait SynapseMLLogging extends Logging {

  val uid: String

  protected def logBase(methodName: String, columns: Option[Int]): Unit = {
    logBase(SynapseMLLogInfo(
      uid,
      getClass.toString,
      methodName,
      BuildInfo.version,
      columns))
  }

  protected def logBase(info: SynapseMLLogInfo): Unit = {
    val message: String = info.toJson.compactPrint
    SynapseMLLogging.LoggedClasses.add(info.className)
    logInfo(s"metrics/ $message")
  }

  protected def logErrorBase(methodName: String, e: Exception): Unit = {
    val message: String = SynapseMLLogInfo(uid, getClass.toString, methodName, BuildInfo.version).toJson.compactPrint
    logError(message, e)
  }

  def logClass(): Unit = {
    logBase("constructor", None)
  }

  def logFit[T](f: => T, columns: Int, logCertifiedEvent: Boolean = true,
                certifiedEventAttributes: Map[String, String] = Map()): T = {
    logVerb("fit", f, columns, logCertifiedEvent, certifiedEventAttributes)
  }

  def logTrain[T](f: => T, columns: Int, logCertifiedEvent: Boolean = true,
                  certifiedEventAttributes: Map[String, String] = Map()): T = {
    logVerb("train", f, columns, logCertifiedEvent, certifiedEventAttributes)
  }

  def logTransform[T](f: => T, columns: Int, logCertifiedEvent: Boolean = true,
                      certifiedEventAttributes: Map[String, String] = Map()): T = {
    logVerb("transform", f, columns, logCertifiedEvent, certifiedEventAttributes)
  }
  def logVerb[T](verb: String, f: => T, columns: Int = -1, logCertifiedEvent: Boolean = true,
                 certifiedEventAttributes: Map[String, String] = Map()): T = {
    logBase(verb, if(columns == -1) None else Some(columns))
    try {
      // Begin emitting certified event.
      if(logCertifiedEvent)
      {
        val certifiedEventPayload = new FeatureUsagePayload("SynapseML",
          verb, certifiedEventAttributes)
        reportUsage(certifiedEventPayload)
      }
      f
    } catch {
      case e: Exception => {
        logErrorBase(verb, e)
        throw e
      }
    }
  }
}
