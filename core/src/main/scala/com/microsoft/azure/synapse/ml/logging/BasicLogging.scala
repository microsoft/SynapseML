// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import org.apache.spark.internal.Logging
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import com.microsoft.azure.synapse.ml.build.BuildInfo

case class BasicLogInfo(
                       uid: String,
                       className: String,
                       method: String,
                       buildVersion: String
                       )

object LogJsonProtocol extends DefaultJsonProtocol {
  implicit val LogFormat: RootJsonFormat[BasicLogInfo] = jsonFormat4(BasicLogInfo)
}

import LogJsonProtocol._
import spray.json._

trait BasicLogging extends Logging {

  val uid: String
  val ver: String = BuildInfo.version

  protected def logBase(methodName: String): Unit = {
    val message: String = BasicLogInfo(uid, getClass.toString, methodName, ver).toJson.compactPrint
    logInfo(s"metrics/ $message")
  }

  protected def logErrorBase(methodName: String, e: Exception): Unit = {
    val message: String = BasicLogInfo(uid, getClass.toString, methodName, ver).toJson.compactPrint
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

  def logPredict[T](f: => T): T = {
    logVerb("predict", f)
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
