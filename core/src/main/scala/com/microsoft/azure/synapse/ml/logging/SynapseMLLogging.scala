// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import org.apache.spark.internal.Logging
import spray.json.{DefaultJsonProtocol, RootJsonFormat, NullOptions}
import scala.collection.JavaConverters._
import scala.collection.mutable

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

}

trait SynapseMLLogging extends Logging {

  val uid: String

  protected def logBase(methodName: String, columns:Int = -1): Unit = {
    logBase(SynapseMLLogInfo(
      uid,
      getClass.toString,
      methodName,
      BuildInfo.version,
      if (columns == -1) None else Some(columns)))
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
    logBase("constructor")
  }

  def logFit[T](f: => T, columns: Int = -1): T = {
    logVerb("fit", f, columns)
  }

  def logTrain[T](f: => T, columns: Int = -1): T = {
    logVerb("train", f, columns)
  }

  def logTransform[T](f: => T, columns: Int = -1): T = {
    logVerb("transform", f, columns)
  }
  def logVerb[T](verb: String, f: => T, columns: Int = -1): T = {
    logBase(verb, columns)
    try {
      f
    } catch {
      case e: Exception => {
        logErrorBase(verb, e)
        throw e
      }
    }
  }
}
