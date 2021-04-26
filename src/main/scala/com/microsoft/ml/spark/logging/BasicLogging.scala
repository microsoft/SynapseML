// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class BasicLogInfo(
                       uid: String,
                       className: String,
                       method: String
                       )

object LogJsonProtocol extends DefaultJsonProtocol {
  implicit val LogFormat: RootJsonFormat[BasicLogInfo] = jsonFormat3(BasicLogInfo)
}

import LogJsonProtocol._
import spray.json._

trait BasicLogging extends Logging {

  val uid: String

  protected def logBase(methodName: String): Unit = {
    val message: String = BasicLogInfo(uid, getClass.toString, methodName).toJson.compactPrint
    logInfo(s"metrics/ $message")
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
        logError("fit exception ", e)
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
        logError("train exception ", e)
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
        logError("transform exception ", e)
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
        logError("predict exception ", e)
        throw e
      }
    }
  }

}
