// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.build.BuildInfo
import org.apache.spark.internal.Logging
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import scala.collection.JavaConverters._
import scala.collection.mutable

case class SynapseMLLogInfo(uid: String,
                            className: String,
                            method: String,
                            buildVersion: String)

object LogJsonProtocol extends DefaultJsonProtocol {
  implicit val LogFormat: RootJsonFormat[SynapseMLLogInfo] = jsonFormat4(SynapseMLLogInfo)
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

  protected def logBase(methodName: String): Unit = {
    logBase(SynapseMLLogInfo(uid, getClass.toString, methodName, BuildInfo.version))
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

  def logFit[T](f: => T): T = {
    logVerb("fit", f)
  }

  def logTrain[T](f: => T): T = {
    logVerb("train", f)
  }

  def logTransform[T](f: => T): T = {
    logVerb("transform", f)
  }

  def logVerb[T](verb: String, f: => T): T = {
    logBase(verb)
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
