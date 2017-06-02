// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.FileUtilities.File
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
  * Exposes utilities used for saving and loading pipelines.
  */
object PipelineUtilities {
  /**
    * Saves metadata that is required by spark pipeline model in order to read a model.
    * @param uid The id of the PipelineModel saved.
    * @param cls The class name.
    * @param metadataPath The metadata path.
    * @param sc The spark context.
    */
  def saveMetadata(uid: String,
                   cls: String,
                   metadataPath: String,
                   sc: SparkContext,
                   overwrite: Boolean): Unit = {
    val metadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> "{}")

    val metadataJson: String = compact(render(metadata))
    val metadataFile = new File(metadataPath)
    val fileExists = metadataFile.exists()
    if (fileExists) {
      if (overwrite) {
        metadataFile.delete()
      } else {
        throw new Exception(
          s"Failed to save pipeline, metadata file $metadataPath already exists, please turn on overwrite option")
      }
    }
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  def makeQualifiedPath(sc: SparkContext, path: String): Path = {
    val modelPath = new Path(path)
    val hadoopConf = sc.hadoopConfiguration
    // Note: to get correct working dir, must use root path instead of root + part
    val fs = modelPath.getFileSystem(hadoopConf)
    modelPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }
}
