// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.benchmarks.DatasetUtils
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CBDatasetHelper {
  def readCSV(session: SparkSession, fileName: String, fileLocation: String): DataFrame = {
    session.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  def getCBDataset(session: SparkSession): DataFrame = {
    val fileName = "cbdata.train.csv"
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir, "VowpalWabbit", "Train", fileName).toString
    val rawDataset = readCSV(session, fileName, fileLocation)
      .repartition(1)

    // schema inference didn't seem to work so we need to convert a few columns.
    val dataset = rawDataset
      .withColumn("chosen_action", col("chosen_action").cast("Int"))
      .withColumnRenamed("chosen_action", "chosenAction")
      .withColumn("cost", col("cost").cast("Double"))
      .withColumnRenamed("cost", "label")
      .withColumn("prob", col("prob").cast("Double"))
      .withColumnRenamed("prob", "probability")

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_id", "shared_major", "shared_hobby", "shared_fav_character"))
      .setOutputCol("shared")

    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_topic"))
      .setOutputCol("action1_features")
    val actionTwoFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_topic"))
      .setOutputCol("action2_features")
    val actionThreeFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action3_topic"))
      .setOutputCol("action3_features")
    val actionFourFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action4_topic"))
      .setOutputCol("action4_features")
    val actionFiveFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action5_topic"))
      .setOutputCol("action5_features")

    val actionMerger = new VectorZipper()
      .setInputCols(Array("action1_features", "action2_features", "action3_features", "action4_features",
        "action5_features"))
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionTwoFeaturizer, actionThreeFeaturizer,
        actionFourFeaturizer, actionFiveFeaturizer, actionMerger))

    val transformPipeline = pipeline.fit(dataset)
    transformPipeline.transform(dataset)
  }
}

class VerifyVowpalWabbitContextualBanditFuzzing extends EstimatorFuzzing[VowpalWabbitContextualBandit] {
  override def testObjects(): Seq[TestObject[VowpalWabbitContextualBandit]] = {
    val trainData = CBDatasetHelper.getCBDataset(session)
    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
    cb.fit(trainData)
    Seq(new TestObject(
      cb,
      trainData))
  }

  override def reader: MLReadable[_] = VowpalWabbitContextualBandit

  override def modelReader: MLReadable[_] = VowpalWabbitContextualBanditModel
}
