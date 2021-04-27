// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import com.microsoft.ml.spark.build.BuildInfo

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
    logBase("fit")
    try {
      f
    } catch {
      case e: Exception => {
        logErrorBase("fit", e)
        throw e
      }
    }
  }

  def logTrain[T](f: => T): T = {
    logBase("train")
    try {
      f
    } catch {
      case e: Exception => {
        logErrorBase("train", e)
        throw e
      }
    }
  }

  def logTransform[T](f: => T): T = {
    logBase("transform")
    try {
      f
    } catch {
      case e: Exception => {
        logErrorBase("transform", e)
        throw e
      }
    }
  }

  def logPredict[T](f: => T): T = {
    logBase("predict")
    try {
      f
    } catch {
      case e: Exception => {
        logErrorBase("predict", e)
        throw e
      }
    }
  }

}
