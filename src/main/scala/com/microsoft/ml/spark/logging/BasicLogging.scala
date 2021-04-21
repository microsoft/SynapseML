// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class BasicLogInfo(
                       uid: String,
                       className: String,
                       method: String,
                       )

object LogJsonProtocol extends DefaultJsonProtocol {
  implicit val LogFormat: RootJsonFormat[BasicLogInfo] = jsonFormat3(BasicLogInfo)
}

import LogJsonProtocol._
import spray.json._

trait BasicLogging extends Logging {

  val uid: String

  def logClass(): Unit = {
    logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, "constructor").toJson.compactPrint)
  }

  def logFit(): Unit = {
      logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, "fit").toJson.compactPrint)
  }

  def logTrain(): Unit = {
    logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, "train").toJson.compactPrint)
  }

  def logTransform(dataset: Dataset[_]): Unit = {
     logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, "transform").toJson.compactPrint)
  }

  def logPredict(): Unit = {
    logInfo("metrics/ " + BasicLogInfo(uid, getClass.toString, "predict").toJson.compactPrint)
  }

}
