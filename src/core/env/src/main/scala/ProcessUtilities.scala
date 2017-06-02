// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import scala.sys.process._

object ProcessUtils {

  // These are only here until we create a more robust
  // stream-redirected utility
  def getProcessOutput(cmd: String): String = {
    println(s"Capturing external process $cmd...")
    val ret = cmd.!!
    println(s"$ret...done!")
    ret
  }

  def runProcess(cmd: String): Int = {
    println(s"Executing external process $cmd...")
    val ret = cmd .!
    println(s"$ret...done!")
    ret
  }

}
