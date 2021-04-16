// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging

trait BasicLogging extends Logging{

  def logClass(uid: String): Unit = {
    logInfo(s"metrics/ uid $uid Calling $getClass")
  }

  def logFit(uid: String): Unit = {
      logInfo(s"metrics/ uid $uid Calling function fit")
  }

  def logFitGeneric(): Unit = {
    logInfo("metrics/ Calling function fitGeneric")
  }

  def logFitOptimized(): Unit = {
    logInfo("metrics/ Calling function fitOptimized")
  }

  def logTrain(uid: String): Unit = {
    logInfo(s"metrics/ uid $uid Calling function train")
  }

  def logTransform(uid: String): Unit = {
     logInfo(s"metrics/ uid $uid Calling function transform")
  }

  def logTranspose(): Unit = {
    logInfo("metrics/ Calling function transpose")
  }

  def logPredict(uid: String): Unit = {
    logInfo(s"metrics/ uid $uid Calling function predict")
  }

}
