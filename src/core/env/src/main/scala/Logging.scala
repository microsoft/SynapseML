// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.logging.log4j.scala.{Logging => Logging4J}
import org.apache.logging.log4j._

// Ilya has the logging functions already in a separate branch, so log APIs here removed.
// Merge those into a single trait "Logging" here and have MMLParams incorporate it.

// Utility to provide log-related canonical construction
// There should be a separate logger at each package (mml, cntk, tlc)
object Logging {

  lazy val config = MMLConfig.get
  lazy val logRoot = config.root

  def getLogger(customSuffix: String): Logger = {
    LogManager.getLogger(s"$logRoot.$customSuffix")
  }

}
