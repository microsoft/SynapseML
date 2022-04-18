// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.benchmarks.DatasetUtils
import com.microsoft.azure.synapse.ml.featurize.ValueIndexer
import com.microsoft.azure.synapse.ml.lightgbm._
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

// scalastyle:off magic.number
trait LightGBMTestUtils extends TestBase {

  /** Reads a CSV file given the file name and file location.
    *
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileLocation: String): DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileLocation.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  def loadBinary(name: String, originalLabelCol: String): DataFrame = {
    val df = readCSV(DatasetUtils.binaryTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    LightGBMUtils.getFeaturizer(df, labelCol, featuresCol).transform(df)
  }

  def loadRegression(name: String,
                     originalLabelCol: String,
                     columnsFilter: Option[Seq[String]] = None): DataFrame = {
    lazy val df = readCSV(DatasetUtils.regressionTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    lazy val df2 =
      if (columnsFilter.isDefined) {
        df.select(columnsFilter.get.map(col): _*)
      } else {
        df
      }
    LightGBMUtils.getFeaturizer(df2, labelCol, featuresCol).transform(df)
  }

  def loadMulticlass(name: String, originalLabelCol: String): DataFrame = {
    val df = readCSV(DatasetUtils.multiclassTrainFile(name).toString).repartition(numPartitions)
      .withColumnRenamed(originalLabelCol, labelCol)
    val featurizedDF = LightGBMUtils.getFeaturizer(df, labelCol, featuresCol).transform(df)
    val indexedDF = new ValueIndexer().setInputCol(labelCol).setOutputCol(labelCol)
      .fit(featurizedDF).transform(featurizedDF)
    indexedDF
  }

  def assertProbabilities(tdf: DataFrame, model: LightGBMClassifier): Unit = {
    tdf.select(model.getRawPredictionCol, model.getProbabilityCol)
      .collect()
      .foreach(row => {
        val probabilities = row.getAs[DenseVector](1).values
        assert((probabilities.sum - 1.0).abs < 0.001)
        assert(probabilities.forall(probability => probability >= 0 && probability <= 1))
      })
  }

  def assertFitWithoutErrors(model: Estimator[_ <: Model[_]], df: DataFrame): Unit = {
    assert(model.fit(df).transform(df).collect().length > 0)
  }

  def assertImportanceLengths(fitModel: Model[_] with LightGBMModelMethods, df: DataFrame): Unit = {
    val splitLength = fitModel.getFeatureImportances("split").length
    val gainLength = fitModel.getFeatureImportances("gain").length
    val featuresLength = df.select(featuresCol).first().getAs[Vector](featuresCol).size
    assert(splitLength == gainLength && splitLength == featuresLength)
  }

  def assertFeatureShapLengths(fitModel: Model[_] with LightGBMModelMethods, features: Vector, df: DataFrame): Unit = {
    val shapLength = fitModel.getFeatureShaps(features).length
    val featuresLength = df.select(featuresCol).first().getAs[Vector](featuresCol).size
    assert(shapLength == featuresLength + 1)
  }

  def validateHeadRowShapValues(evaluatedDf: DataFrame, expectedShape: Int): Unit = {
    val featuresShap: Array[Double] = evaluatedDf.select(featuresShapCol).rdd.map {
      case Row(v: Vector) => v
    }.first.toArray

    assert(featuresShap.length == expectedShape)
  }

  lazy val numPartitions = 2
  val startingPortIndex = 0
  private var portIndex = startingPortIndex

  def getAndIncrementPort(): Int = {
    portIndex += numPartitions
    LightGBMConstants.DefaultLocalListenPort + portIndex
  }

  val boostingTypes: Array[String] = Array("gbdt", "rf", "dart", "goss")
  val featuresCol = "features"
  val labelCol = "labels"
  val rawPredCol = "rawPrediction"
  val leafPredCol = "leafPrediction"
  val featuresShapCol = "featuresShap"
  val initScoreCol = "initScore"
  val predCol = "prediction"
  val probCol = "probability"
  val weightCol = "weight"
  val validationCol = "validation"
  val seed = 42L
}
