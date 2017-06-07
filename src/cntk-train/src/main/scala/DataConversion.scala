// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.SparkContext

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util.Identifiable

import FileUtilities._
import hadoop.HadoopUtils

object DataTransferUtils {

  // This needs to be broken up into a few areas:
  // 1. data-conversion is a library that has knowledge
  //    of type mappings, schema, and a canonical implementation of
  //    the type map conversion functions themselves
  // 2. Featurize must be moved to a library utilized by the Train* APIs
  //    this is a 2 stage estimator:
  //       a. Type "reduction" utilizing a typemapper
  //       b. Assembly via single or "feature channel" based multi vector assembler
  //    at present, the current architecture limits us to

  def toText(value: Any): String = {
    value match {
      case v: Vector => return convertVectorToText(v)
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

  def convertVectorToText(v: Vector): String = {
    val heuristicBloat = 8
    val sb = new StringBuilder(v.numActives * heuristicBloat)
    v match {
      case sv: SparseVector => {
        sv.foreachActive { (idx, value) =>
          sb.append(idx).append(":").append(value).append(" ")
          ()
        }
      }
      case dv: DenseVector => {
        dv.values.foreach(value => sb.append(value).append(" "))
      }
    }
    sb.toString
  }

  private def col2vec = udf(toVec _)
  private def col2str = udf(toText _)

  // This needs to be converted to a pipeline stage as stated above
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
      .setAllowImages(true)
      .fit(data)
    val reduced = tempFeaturizer.transform(data)
    reduced.select(col2vec(reduced(label)).as(label), reduced(outputVecName))
  }

  def convertDatasetToCNTKTextFormat(data: Dataset[_], label: String, feats: String): DataFrame = {
    val labelStrCol = col2str(data(label))
    val featStrCol = col2str(data(feats))
    val uberCol = concat(
        lit(s"|$label "),
        labelStrCol,
        lit(" "),
        lit(s"|$feats "),
        featStrCol)
    data.select(uberCol.as('value))
  }

}

// This is all horrid, find a better way to be cluster/local agnostic
// via DataSource and DataSink-type model. This will allow us to move to
// other source/sinks in the future more easily, but think is out of scope here
// TODO: this should become a set of extensions onto CheckpointData, which can
// also return a model that is JSON serializable into the DataSource representation
import java.net.URI

abstract class DataWriter(destPath: String) {
  protected val destUri = new URI(destPath)
  protected val relativeDest = destUri.getPath

  protected val partitions: Int

  protected def remapPath(ext: String): String

  def constructedPath: String

  def checkpointToText(data: Dataset[_]): String = {
    val fullPath = constructedPath
    println(s"Writing dataset to $fullPath")
    data.coalesce(partitions).write.text(fullPath)
    remapPath("txt")
  }

  def checkpointToParquet(data: Dataset[_]): String = {
    val fullPath = constructedPath
    println(s"Writing dataset to $fullPath")
    data.coalesce(partitions).write.format("parquet").save(fullPath)
    remapPath("parquet")
  }
}

abstract class NormalWriter(path: String) extends DataWriter(path) {
  protected def remapPath(ext: String): String = constructedPath
}

// This is used when Hadoop creates the actual single part file
// inside the path we've provided - we want that one.
abstract class SingleFileResolver(path: String) extends DataWriter(path) {
  protected val remappedRoot: String

  protected def remapPath(extension: String): String = {
    val dir = new File(remappedRoot)
    println(s"Probing $dir for single file $constructedPath")
    val file = dir.listFiles.filter(f => f.isFile && f.getName.endsWith(extension)).head
    println(s"Resolving single file ${file.getAbsolutePath}")
    file.getAbsolutePath
  }
}

class LocalWriter(path: String) extends SingleFileResolver(path) {
  val partitions = 1
  val constructedPath = new URI("file", null, relativeDest, null, null).normalize.toString

  // TODO: Move this logic to Apache commons lang helper that already exists
  // And then provide a helper function in FileUtilities. Why doesn't URI normalize properly for new File()?
  val remappedRoot = {
    val root = if (EnvironmentUtils.IsWindows) "C:" else ""
    root + relativeDest
  }
}

class DefaultHdfsWriter(parts: Int, path: String) extends NormalWriter(path) {
  val partitions = parts
  val constructedPath = new URI(null, null, relativeDest, null, null).toString
}

class HdfsMountWriter(localMnt: String, parts: Int, path: String, sc: SparkContext) extends SingleFileResolver(path) {
  val partitions = parts
  // TODO: Why is this required on the edge node?
  val hConf = sc.hadoopConfiguration
  val namenode = new HadoopUtils(hConf).getActiveNameNode
  val constructedPath = new URI("hdfs", namenode, relativeDest, null, null).toString
  val remappedRoot = new URI(localMnt).toString + s"/$relativeDest"
}
