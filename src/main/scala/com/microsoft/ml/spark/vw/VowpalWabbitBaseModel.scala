// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import org.apache.spark.binary.BinaryFileFormat
import org.apache.spark.ml.ComplexParamsWritable
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.ml.param.{ByteArrayParam, DataFrameParam, Param}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.vowpalwabbit.spark.{VowpalWabbitArguments, VowpalWabbitExample, VowpalWabbitMurmur, VowpalWabbitNative}
import org.vowpalwabbit.spark.prediction.ScalarPrediction

import scala.io.Source

case class PathAndData(path: String, bytes: Array[Byte])

/**
  * Base trait to wrap the model for prediction.
  */
trait VowpalWabbitBaseModel extends org.apache.spark.ml.param.shared.HasFeaturesCol
  with org.apache.spark.ml.param.shared.HasRawPredictionCol
  with HasAdditionalFeatures
  with ComplexParamsWritable
{
  @transient
  lazy val vw = new VowpalWabbitNative(s"--testonly ${getTestArgs}", getModel)

  @transient
  lazy val example: VowpalWabbitExample = vw.createExample()

  @transient
  lazy val vwArgs: VowpalWabbitArguments = vw.getArguments

  val testArgs = new Param[String](this, "testArgs", "Additional arguments passed to VW at test time")
  setDefault(testArgs -> "")

  def getTestArgs: String = $(testArgs)
  def setTestArgs(value: String): this.type = set(testArgs, value)

  protected def transformImplInternal(dataset: Dataset[_]): DataFrame = {
    val featureColIndices = (Seq(getFeaturesCol) ++ getAdditionalFeatures)
      .zipWithIndex.map { case (col, index) => NamespaceInfo(
      VowpalWabbitMurmur.hash(col, vwArgs.getHashSeed),
      col.charAt(0),
      index)
    }

    val predictUDF = udf { (namespaces: Row) => predictInternal(featureColIndices, namespaces) }

    val allCols = Seq(col($(featuresCol))) ++ getAdditionalFeatures.map(col)

    dataset.withColumn($(rawPredictionCol), predictUDF(struct(allCols: _*)))
  }

  // Model parameter
  val model = new ByteArrayParam(this, "model", "The VW model....")

  def setModel(v: Array[Byte]): this.type = set(model, v)
  def getModel: Array[Byte] = $(model)

  def getReadableModel: String = {
    val readableModelFile = java.io.File.createTempFile("vowpalwabbit", ".txt")

    try
    {
      // start VW, write the model, stop
      new VowpalWabbitNative(s"--readable_model ${readableModelFile.getAbsolutePath}", getModel).close

      // need to read the model after the VW object is disposed as this triggers the file write
      Source.fromFile(readableModelFile).mkString
    }
    finally
      readableModelFile.delete
 }

  // Perf stats parameter
  val performanceStatistics = new DataFrameParam(this,
    "performanceStatistics",
    "Performance statistics collected during training")

  def setPerformanceStatistics(v: DataFrame): this.type = set(performanceStatistics, v)
  def getPerformanceStatistics: DataFrame = $(performanceStatistics)

  protected def predictInternal(featureColIndices: Seq[NamespaceInfo], namespaces: Row): Double = {
    example.clear()

    for (ns <- featureColIndices)
      namespaces.get(ns.colIdx) match {
        case dense: DenseVector => example.addToNamespaceDense(ns.featureGroup,
          ns.hash, dense.values)
        case sparse: SparseVector => example.addToNamespaceSparse(ns.featureGroup,
          sparse.indices, sparse.values)
      }

    // TODO: surface prediction confidence
    example.predict.asInstanceOf[ScalarPrediction].getValue.toDouble
  }

  def saveNativeModel(path: String): Unit = {
    val session = SparkSession.builder().getOrCreate()

    val f = new java.io.File(path)

    session.createDataFrame(Seq(PathAndData(f.getCanonicalPath, getModel)))
        .write
      .mode("overwrite")
        .format(classOf[BinaryFileFormat].getName)
        .save(f.getName)
  }
}
