// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.PrintWriter
import java.net.URI

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg._
import FileUtilities._
import hadoop.HadoopUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.slf4j.Logger

/**
  * Utilities for reducing data to CNTK format and generating the text file output to disk.
  */
object DataTransferUtils {

  def toText(form: String)(value: Any): String = {
    value match {
      case v: Vector => return convertVectorToText(v, form)
      case d: Double => return d.toString
      case f: Float => return f.toString
    }
  }

  def toVec(value: Any): Vector = {
    value match {
      case v: Vector => return v
      case d: Double => return new DenseVector(Array(d))
      case f: Float => return new DenseVector(Array(f.toDouble))
      case i: Integer => return new DenseVector(Array(i.toDouble))
      case l: Long => return new DenseVector(Array(l.toDouble))
    }
  }

  def convertVectorToText(v: Vector, form: String): String = {
    val heuristicBloat = 8
    val sb = new StringBuilder(v.numActives * heuristicBloat)
    v match {
      case sv: SparseVector => {
        if (form == CNTKLearner.sparseForm) {
          sv.foreachActive { (idx, value) =>
            sb.append(idx).append(":").append(value).append(" ")
            ()
          }
        } else if (form == CNTKLearner.denseForm) {
          convertVectorToText(sv.toDense, form)
        } else {
          throw new Exception(s"Unknown vector form $form")
        }
      }
      case dv: DenseVector => {
        if (form == CNTKLearner.denseForm) {
          dv.values.foreach(value => sb.append(value).append(" "))
        } else if (form == CNTKLearner.sparseForm) {
          for (i <- 0 until dv.values.length) {
            sb.append(i).append(":").append(dv.values(i)).append(" ")
          }
        } else {
          throw new Exception(s"Unknown vector form $form")
        }
      }
    }
    sb.toString
  }

  private def col2vec: UserDefinedFunction  = udf(toVec _)
  private def col2str(form: String): UserDefinedFunction = udf(toText(form) _)

  def reduceAndAssemble(data: Dataset[_],
                        label: String,
                        outputVecName: String,
                        precision: String,
                        features: Int): DataFrame = {
    if (precision != "double") throw new NotImplementedError("only doubles")

    val tempFeaturizer = new Featurize()
      .setFeatureColumns(Map(outputVecName -> data.columns.filter(_ != label)))
      .setNumberOfFeatures(features)
      .setOneHotEncodeCategoricals(true)
      .fit(data)
    val reduced = tempFeaturizer.transform(data)
    reduced.select(col2vec(reduced(label)).as(label), reduced(outputVecName))
  }

  def convertDatasetToCNTKTextFormat(data: Dataset[_],
                                     label: String,
                                     feats: String,
                                     labelForm: String,
                                     featureForm: String): DataFrame = {
    val labelStrCol = col2str(labelForm)(data(label))
    val featStrCol = col2str(featureForm)(data(feats))
    val uberCol = concat(
        lit(s"|$label "),
        labelStrCol,
        lit(" "),
        lit(s"|$feats "),
        featStrCol)
    data.select(uberCol.as('value))
  }

}

abstract class DataWriter(log: Logger, destPath: String) {
  protected val destUri = new URI(destPath)
  protected val relativeDest = destUri.getPath

  protected val partitions: Int

  protected def remapPath(ext: String): String

  def constructedPath: String

  def checkpointToText(data: Dataset[_]): String = {
    val fullPath = constructedPath
    // For local file, collect and write to current driver's file location
    if (partitions == 1 && constructedPath.startsWith("file:")) {
      val localdata = data.collect()

      log.info(s"Writing dataset to $relativeDest")
      val relDest = new File(relativeDest)
      relDest.mkdirs()
      val writer = new PrintWriter(new File(new Path(relativeDest, "part0.txt").toString))
      try {
        localdata.foreach(row => writer.println(row.asInstanceOf[Row].getString(0)))
      } finally {
        writer.close()
      }
    } else {
      log.info(s"Writing dataset to $fullPath")
      data.coalesce(partitions).write.text(fullPath)
    }
    remapPath("txt")
  }

  def checkpointToParquet(data: Dataset[_]): String = {
    val fullPath = constructedPath
    log.info(s"Writing dataset to $fullPath")
    data.coalesce(partitions).write.format("parquet").save(fullPath)
    remapPath("parquet")
  }
}

// This is used when Hadoop creates the actual single part file
// inside the path we've provided - we want that one.
abstract class SingleFileResolver(log: Logger, path: String) extends DataWriter(log, path) {
  protected val remappedRoot: String

  protected def remapPath(extension: String): String = {
    val dir = new File(remappedRoot)
    log.info(s"Probing $dir for single file $constructedPath")
    val file = dir.listFiles.filter(f => f.isFile && f.getName.endsWith(extension)).head
    log.info(s"Resolving single file ${file.getAbsolutePath}")
    file.getAbsolutePath
  }
}

class LocalWriter(log: Logger, path: String) extends SingleFileResolver(log, path) {
  val partitions = 1
  val constructedPath = new URI("file", null, relativeDest, null, null).normalize.toString

  // TODO: Move this logic to Apache commons lang helper that already exists
  // And then provide a helper function in FileUtilities. Why doesn't URI normalize properly for new File()?
  val remappedRoot = {
    val root = if (EnvironmentUtils.IsWindows) "C:" else ""
    root + relativeDest
  }
}

class HdfsMountWriter(log: Logger, localMnt: String, parts: Int, path: String, sc: SparkContext)
  extends SingleFileResolver(log, path) {
  val partitions = parts
  val hConf = sc.hadoopConfiguration
  val namenode = new HadoopUtils(log, hConf).getActiveNameNode
  val constructedPath = new URI("hdfs", namenode, relativeDest, null, null).toString
  val mountPoint = if (localMnt.startsWith("/")) localMnt else "/" + localMnt
  val remappedRoot = mountPoint + relativeDest

  override protected def remapPath(extension: String): String = {
    log.info(s"Wrote unmerged text files to hdfs path: $constructedPath")
    log.info(s"Mount point for process: $mountPoint")
    log.info(s"Remapped root for file: $remappedRoot")
    val file = s"$remappedRoot/merged-input.txt"
    log.info(s"Path to mnt file on GPU VM: $file")
    file
  }

  // The HDFS URL to mount on the edge node
  def getHdfsToMount: String = new URI("hdfs", namenode, "/", null, null).toString
  // The directory containing the input data in HDFS
  def getHdfsInputDataDir: String = constructedPath
  // The mount point
  def getMountPoint: String = mountPoint
}
