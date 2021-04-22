// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
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
    logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, methodName).toJson.compactPrint)
  }

  def logClass(): Unit = {
    logBase("constructor")
  }

  def logFit(): Unit = {
    logBase("fit")
  }

  def logTrain(): Unit = {
    logBase("train")
  }

  def logTransform(): Unit = {
    logBase("transform")
  }

  def logPredict(): Unit = {
    logBase("predict")
  }

}
