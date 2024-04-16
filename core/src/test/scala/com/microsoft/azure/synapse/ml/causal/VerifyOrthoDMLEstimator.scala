// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.benchmarks.DatasetUtils._
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalactic.Equality


class VerifyOrthoDMLEstimator extends EstimatorFuzzing[OrthoForestDMLEstimator] {

  lazy val schema: StructType = StructType(Array(
    StructField("Label", StringType),
    StructField("X1", DoubleType),
    StructField("X2", DoubleType),
    StructField("X3", DoubleType),
    StructField("W1", DoubleType),
    StructField("W2", DoubleType),
    StructField("W3", DoubleType),
    StructField("W4", DoubleType),
    StructField("W5", DoubleType),
    StructField("W6", DoubleType),
    StructField("W7", DoubleType),
    StructField("W8", DoubleType),
    StructField("W9", DoubleType),
    StructField("W10", DoubleType),
    StructField("Y", DoubleType),
    StructField("T", DoubleType),
    StructField("TE", DoubleType)
  ))
  lazy val treatmentCol = "T"
  lazy val outcomeCol = "Y"
  lazy val heterogeneityCols: Array[String] = Array("X1", "X2", "X3")
  lazy val heterogeneityVecCol = "XVec"
  lazy val confounderCols: Array[String] = Array("W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10")
  lazy val confounderVecCol = "XWVec"

  lazy val filePath: String = causalTrainFile("OrthoForestData.csv").toString

  lazy val df: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(filePath)

  lazy val heterogeneityVector: VectorAssembler =
    new VectorAssembler()
      .setInputCols(heterogeneityCols)
      .setOutputCol(heterogeneityVecCol)

  lazy val confounderVector: VectorAssembler =
    new VectorAssembler()
      .setInputCols(confounderCols)
      .setOutputCol(confounderVecCol)

  lazy val pipeline: Pipeline = new Pipeline()
    .setStages(Array(heterogeneityVector,
      confounderVector))

  lazy val ppfit: DataFrame = pipeline.fit(df).transform(df)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    val dropCols = List("EffectAverage", "EffectLowerBound", "EffectUpperBound", "XVec", "XWVec")

    def prep(df: DataFrame) = {
      df.drop(dropCols: _*)
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Test Ortho Forest DML") {

    val ortho = new OrthoForestDMLEstimator()
      .setNumTrees(100)
      .setTreatmentCol("T")
      .setOutcomeCol("Y")
      .setHeterogeneityVecCol(heterogeneityVecCol)
      .setConfounderVecCol(confounderVecCol)
      .setMaxDepth(12)
      .setMinSamplesLeaf(5)

    val finalModel = ortho.fit(ppfit)

    val finalPred = finalModel.transform(ppfit)
      .withColumn("IsWithinBounds",
        (col("TE") > col(ortho.getOutputLowCol)
          && col("TE") < col(ortho.getOutputHighCol)).cast(IntegerType))

    val samplesInBound = finalPred
      .agg(sum("IsWithinBounds"))
      .first()
      .getLong(0)

    /* Since the sample is 1000, we expect 100%- 5% +/- 1.34%. Setting a safer limit for test case to always pass*/
    assert(samplesInBound > 890 && samplesInBound < 1000)
  }

  override def testObjects(): Seq[TestObject[OrthoForestDMLEstimator]] =
    Seq(new TestObject(new OrthoForestDMLEstimator()
      .setNumTrees(10)
      .setTreatmentCol("T")
      .setOutcomeCol("Y")
      .setHeterogeneityVecCol(heterogeneityVecCol)
      .setConfounderVecCol(confounderVecCol)
      .setMaxDepth(10)
      .setMinSamplesLeaf(100),
      ppfit, ppfit))


  override def reader: MLReadable[_] = OrthoForestDMLEstimator

  override def modelReader: MLReadable[_] = OrthoForestDMLModel
}
