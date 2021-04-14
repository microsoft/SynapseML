// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.logging

import org.apache.spark.internal.Logging

trait BasicLogging extends Logging{

  def logClass(): Unit = {
    logInfo(s"Calling $getClass --- telemetry record")
  }

  def logFit(): Unit = {
      logInfo("Calling function fit --- telemetry record")
  }

  def logFitGeneric(): Unit = {
    logInfo("Calling function fitGeneric --- telemetry record")
  }

  def logFitOptimized(): Unit = {
    logInfo("Calling function fitOptimized --- telemetry record")
  }

  def logTrain(): Unit = {
    logInfo("Calling function train --- telemetry record")
  }

  def logTransform(): Unit = {
     logInfo("Calling function transform --- telemetry record")
  }

  def logTranspose(): Unit = {
    logInfo("Calling function transpose --- telemetry record")
  }

  def logPredict(): Unit = {
    logInfo("Calling function predict --- telemetry record")
  }

}