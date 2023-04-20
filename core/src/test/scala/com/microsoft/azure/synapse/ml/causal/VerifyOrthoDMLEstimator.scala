// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalactic.Equality

class VerifyOrthoDMLEstimator extends EstimatorFuzzing[OrthoForestDMLEstimator] {
    val schema = StructType(Array(
      StructField("Label",StringType),
      StructField("X1",DoubleType),
      StructField("X2",DoubleType),
      StructField("X3",DoubleType),
      StructField("W1",DoubleType),
      StructField("W2",DoubleType),
      StructField("W3",DoubleType),
      StructField("W4",DoubleType),
      StructField("W5",DoubleType),
      StructField("W6",DoubleType),
      StructField("W7",DoubleType),
      StructField("W8",DoubleType),
      StructField("W9",DoubleType),
      StructField("W10",DoubleType),
      StructField("Y",DoubleType),
      StructField("T",DoubleType),
      StructField("TE",DoubleType)
    ))
    val treatmentCol = "T"
    val outcomeCol = "Y"

    val heterogeneityCols  = Array("X1","X2","X3")
    val heterogeneityVecCol = "XVec"
    val confounderCols = Array("W1","W2","W3","W4","W5","W6","W7","W8","W9","W10")
    val confounderVecCol = "XWVec"

    val filePath = s"${sys.env("DATASETS_HOME")}/Causal/OrthoForestData.csv"

    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(filePath)

    val heterogeneityVector =
      new VectorAssembler()
        .setInputCols(heterogeneityCols)
        .setOutputCol(heterogeneityVecCol)

    val confounderVector =
      new VectorAssembler()
        .setInputCols(confounderCols)
        .setOutputCol(confounderVecCol)

    val pipeline = new Pipeline()
      .setStages(Array(heterogeneityVector,
        confounderVector))

    var ppfit = pipeline.fit(df).transform(df)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    val dropCols = List("estAvg","estLow","estHigh","XVec","XWVec")

    def prep(df: DataFrame) = {
      df.drop(dropCols: _*)
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Test Ortho Forest DML") {

    val mtTransform = new OrthoForestDMLEstimator()
      .setNumTrees(100)
      .setTreatmentCol("T")
      .setOutcomeCol("Y")
      .setHeterogeneityVecCol(heterogeneityVecCol)
      .setConfounderVecCol(confounderVecCol)
      .setMaxDepth(12)
      .setMinSamplesLeaf(5)

    val finalModel = mtTransform
      .fit(ppfit)

    val finalPred = finalModel
      .transform(ppfit)
      .withColumn("IsWithinBounds", when(col("TE") > col("estLow") && col("TE") < col("estHigh"), 1).otherwise(0))

    val samplesInBound = finalPred
      .agg(sum("IsWithinBounds"))
      .first()
      .getLong(0)

    /* Since the sample is 1000, we expect 100%- 5% +/- 1.34%. Setting a safer limit for test case to always pass*/
    assert(samplesInBound > 900 && samplesInBound < 995)
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
      ppfit,ppfit))

  override def reader: MLReadable[_] = OrthoForestDMLEstimator

  override def modelReader: MLReadable[_] = OrthoForestDMLModel
}
