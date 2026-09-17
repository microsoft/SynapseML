// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import java.io.{File, IOException}

private[lightgbm] object ValidationDataSpool {
  private val PartitionPrefix = "part-"

  def listPartitionFiles(spoolDirectory: File, expectedPartitionCount: Int): Array[File] = {
    require(expectedPartitionCount >= 0, "Expected validation partition count cannot be negative")
    val entries = Option(spoolDirectory.listFiles()).getOrElse {
      throw new IOException(
        s"Could not list validation spool directory ${spoolDirectory.getAbsolutePath}")
    }
    val indexedFiles = entries
      .filter(_.getName.startsWith(PartitionPrefix))
      .map { file =>
        val suffix = file.getName.stripPrefix(PartitionPrefix)
        val partitionIndex = try {
          suffix.toInt
        } catch {
          case malformed: NumberFormatException =>
            throw new IOException(s"Invalid validation partition file ${file.getName}", malformed)
        }
        if (partitionIndex < 0 || file.getName != s"$PartitionPrefix$partitionIndex" || !file.isFile) {
          throw new IOException(s"Invalid validation partition file ${file.getName}")
        }
        partitionIndex -> file
      }
      .sortBy(_._1)

    if (indexedFiles.length != expectedPartitionCount) {
      throw new IOException(
        s"Expected $expectedPartitionCount validation partition files but found ${indexedFiles.length}")
    }
    indexedFiles.zipWithIndex.foreach { case ((partitionIndex, file), expectedIndex) =>
      if (partitionIndex != expectedIndex) {
        throw new IOException(
          s"Expected validation partition file $PartitionPrefix$expectedIndex but found ${file.getName}")
      }
    }
    indexedFiles.map(_._2)
  }
}
