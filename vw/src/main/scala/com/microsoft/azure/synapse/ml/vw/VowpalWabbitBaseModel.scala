// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.utils.FaultToleranceUtils
import com.microsoft.azure.synapse.ml.io.binary.BinaryFileFormat
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.microsoft.azure.synapse.ml.param.{ByteArrayParam, DataFrameParam}
//import org.apache.spark.ml.param.Param
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.vowpalwabbit.spark.prediction.ScalarPrediction
import org.vowpalwabbit.spark.{VowpalWabbitArguments, VowpalWabbitExample, VowpalWabbitNative}

import scala.io.Source

case class PathAndData(path: String, bytes: Array[Byte])

/**
  * Base trait to wrap the model for prediction.
  */
trait VowpalWabbitBaseModel extends Params {
  @transient
  lazy val vw: VowpalWabbitNative = FaultToleranceUtils.retryWithTimeout() {
    new VowpalWabbitNative(s"--testonly ${getTestArgs}", getModel)
  }

  @transient
  lazy val vwArgs: VowpalWabbitArguments = vw.getArguments

  val testArgs = new Param[String](this, "testArgs", "Additional arguments passed to VW at test time")
  setDefault(testArgs -> "")

  def getTestArgs: String = $(testArgs)

  def setTestArgs(value: String): this.type = set(testArgs, value)

  // Model parameter
  val model = new ByteArrayParam(this, "model", "The VW model....")

  def setModel(v: Array[Byte]): this.type = set(model, v)

  def getModel: Array[Byte] = $(model)

  def getReadableModel: String = {
    val readableModelFile = java.io.File.createTempFile("vowpalwabbit", ".txt")

    try {
      // start VW, write the model, stop
      new VowpalWabbitNative(s"--readable_model ${readableModelFile.getAbsolutePath}", getModel).close()

      // need to read the model after the VW object is disposed as this triggers the file write
      StreamUtilities.using(Source.fromFile(readableModelFile)) {
        _.mkString
      }.get
    } finally {
      readableModelFile.delete
    }
  }

  // Perf stats parameter
  val performanceStatistics = new DataFrameParam(this,
    "performanceStatistics",
    "Performance statistics collected during training")

  def setPerformanceStatistics(v: DataFrame): this.type = set(performanceStatistics, v)

  def getPerformanceStatistics: DataFrame = $(performanceStatistics)

  def saveNativeModel(path: String): Unit = {
    val session = SparkSession.builder().getOrCreate()

    val f = new java.io.File(path)

    session.createDataFrame(Seq(PathAndData(f.getName, getModel)))
      .write.mode("overwrite")
      .format(classOf[BinaryFileFormat].getName)
      .save(f.getPath)
  }
}
