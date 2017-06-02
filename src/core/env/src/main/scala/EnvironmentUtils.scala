// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.nio.file.Paths
import scala.sys.process._

import org.apache.spark._
import org.apache.spark.sql.SparkSession

import ProcessUtils._

object EnvironmentUtils {

  // We should use Apache Commons Lang instead
  def IsWindows: Boolean = System.getProperty("os.name").toLowerCase().indexOf("win") >= 0

  // Make this overrideable so people have control over the granularity
  private lazy val nvInfo: Option[String] = {
    println(s"Computing GPU count on ${if(IsWindows) "Windows" else "Linux"}")
    val nvsmicmd = if (IsWindows) {
      // Unlikely nvidia is on the path
      val nvsmi = Paths.get(
        System.getenv("ProgramFiles"),
        "NVIDIA Corporation",
        "NVSMI",
        "nvidia-smi.exe").toAbsolutePath.toString
      "\"" + nvsmi + "\""
    } else {
      "nvidia-smi"
    }
    // Probably a more Scala-idiomatic way to do this
    try {
      Some(ProcessUtils.getProcessOutput(s"$nvsmicmd -L"))
    } catch {
      // Use the logging API to do this properly
      case e: Exception => {
        println(s"Couldn't query Nvidia SMI for GPU info: $e")
        None
      }
    }
  }

  lazy val GPUCount: Option[Int] = if (nvInfo.isEmpty) None else {
    // Commons Lang has isNotBlank
    val gpucnt = nvInfo.get.split("\n").filter(!_.trim.isEmpty).length
    println(s"$gpucnt GPUs detected")
    Some(gpucnt)
  }

}
