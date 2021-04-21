// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset

trait BasicLogging extends Logging {

  val uid: String

  def logClass(): Unit = {
    logInfo(s"metrics/ {uid: $uid, class: $getClass}")
  }

  def logFit(): Unit = {
      logInfo(s"metrics/ {uid: $uid, method: fit}")
  }

  def logTrain(): Unit = {
    logInfo(s"metrics/ {uid: $uid, method: train}")
  }

  def logTransform(dataset: Dataset[_]): Unit = {
     logInfo(s"metrics/ {uid: $uid, method: transform, dataset: {columns: ${dataset.columns.mkString}}}")
  }

  def logPredict(): Unit = {
    logInfo(s"metrics/ {uid: $uid, method: predict}")
  }

}
