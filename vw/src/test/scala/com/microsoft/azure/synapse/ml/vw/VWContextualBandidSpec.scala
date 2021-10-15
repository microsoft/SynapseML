// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.build.BuildInfo
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

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

class VWContextualBandidSpec extends TestBase with EstimatorFuzzing[VowpalWabbitContextualBandit] {
  import spark.implicits._

  test("Verify VerifyVowpalWabbitContextualBandit can be run on toy dataset") {
    val df = Seq(
      ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 1, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 1, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 4, 0.8),
      ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 0, 0.8)
    ).toDF("shared", "action1", "action2_feat1", "action2_feat2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")

    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")

    val actionTwoFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_feat1", "action2_feat2"))
      .setOutputCol("action2_features")

    val actionMerger = new VectorZipper()
      .setInputCols(Array("action1_features", "action2_features"))
      .setOutputCol("action_features")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionTwoFeaturizer, actionMerger))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)
      .select("chosen_action", "cost", "prob", "shared_features", "action_features")

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)
    assert(m.getPerformanceStatistics.select("snipsEstimate").first.getDouble(0) > 0)
  }

  test("Verify VectorZipper can merge two columns") {
    val df = Seq(
      ("thing1", "thing2")
    ).toDF("col1", "col2")

    val sequencer = new VectorZipper()
      .setInputCols(Array("col1", "col2"))
      .setOutputCol("combined_things")
    val transformedDf = sequencer.transform(df)
    val result = transformedDf.collect.head.getAs[Seq[String]]("combined_things")
    assert(result == Seq("thing1", "thing2"))
  }

  test("Verify columns are correct data type") {
    val cbWithIncorrectActionCol = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
      .setEpsilon(0.2)
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared")
      .setFeaturesCol("action1")
      .setUseBarrierExecutionMode(false)

    val untransformedDf = Seq(
      ("shared_f", "action1_f", 1, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](cbWithIncorrectActionCol.fit(untransformedDf))

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared"))
      .setOutputCol("shared_features")
    val actionOneFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1"))
      .setOutputCol("action1_features")
    val actionMerger = new VectorZipper()
      .setInputCols(Array("action1_features"))
      .setOutputCol("action_features")
    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")
      .setUseBarrierExecutionMode(false)

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionMerger, cb))
    pipeline.fit(untransformedDf)

    pipeline.fit(untransformedDf, Array(new ParamMap()))

    val untransformedDfWithDecimalChosenAction = Seq(
      ("shared_f", "action1_f", 1.5, 1, 0.8)
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformedDfWithDecimalChosenAction))

    val untransformedDfWithStringProb = Seq(
      ("shared_f", "action1_f", 1.5, 1, "I'm a prob!")
    ).toDF("shared", "action1", "chosen_action", "cost", "prob")

    assertThrows[AssertionError](pipeline.fit(untransformedDfWithStringProb))
  }

  test("Verify raises exception when used with incompatible args") {
    val df = CBDatasetHelper.getCBDataset(spark)
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb --episilon 0.5").fit(df))
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb_explore").fit(df))
    assertThrows[NotImplementedError](
      new VowpalWabbitContextualBandit().setArgs("--quiet --cb_adf").fit(df))
    new VowpalWabbitContextualBandit().setArgs("--quiet --cb_explore_adf").fit(df)
  }

  test("Verify can use paralellized fit") {
    val df = CBDatasetHelper.getCBDataset(spark)
      .coalesce(1)
      .cache()

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
      .setParallelismForParamListFit(4)
      .setUseBarrierExecutionMode(false)

    val paramGrid = new ParamGridBuilder()
      .addGrid(cb.learningRate, Array(0.5, 0.05, 0.001))
      .addGrid(cb.epsilon, Array(0.1, 0.2, 0.4))
      .build()

    val m = cb.fit(df, paramGrid)
      .map(model => model.getPerformanceStatistics)
      .reduce((a, b) => a.union(b))
    assert(m.select("ipsEstimate").first.getDouble(0) < 0)
  }

  test("Verify can supply additional namespaces") {
    val df = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 4, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 0, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1",
      "action2_f2_ns1", "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns1"))
      .setOutputCol("shared_features_ns1")

    val sharedFeaturizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns2"))
      .setOutputCol("shared_features_ns2")

    val a1Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns1"))
      .setOutputCol("action1_features_ns1")

    val a1Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns2"))
      .setOutputCol("action1_features_ns2")

    val a2Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns1", "action2_f2_ns1"))
      .setOutputCol("action2_features_ns1")

    val a2Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns2"))
      .setOutputCol("action2_features_ns2")

    val actionMergerNs1 = new VectorZipper()
      .setInputCols(Array("action1_features_ns1", "action2_features_ns1"))
      .setOutputCol("action_features_ns1")

    val actionMergerNs2 = new VectorZipper()
      .setInputCols(Array("action1_features_ns2", "action2_features_ns2"))
      .setOutputCol("action_features_ns2")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, sharedFeaturizer2, a1Ns1Featurizer, a1Ns2Featurizer,
        a2Ns1Featurizer, a2Ns2Featurizer, actionMergerNs1, actionMergerNs2))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features_ns1")
      .setAdditionalSharedFeatures(Array("shared_features_ns2"))
      .setFeaturesCol("action_features_ns1")
      .setAdditionalFeatures(Array("action_features_ns2"))
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)
    assert(m.getPerformanceStatistics.select("snipsEstimate").first.getDouble(0) > 0)
  }

  test("Verify can predict on the resulting model") {

    val df = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 4, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 0, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1", "action2_f2_ns1",
      "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns1"))
      .setOutputCol("shared_features_ns1")

    val sharedFeaturizer2 = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_ns2"))
      .setOutputCol("shared_features_ns2")

    val a1Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns1"))
      .setOutputCol("action1_features_ns1")

    val a1Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action1_ns2"))
      .setOutputCol("action1_features_ns2")

    val a2Ns1Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns1", "action2_f2_ns1"))
      .setOutputCol("action2_features_ns1")

    val a2Ns2Featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("action2_f1_ns2"))
      .setOutputCol("action2_features_ns2")

    val actionMergerNs1 = new VectorZipper()
      .setInputCols(Array("action1_features_ns1", "action2_features_ns1"))
      .setOutputCol("action_features_ns1")

    val actionMergerNs2 = new VectorZipper()
      .setInputCols(Array("action1_features_ns2", "action2_features_ns2"))
      .setOutputCol("action_features_ns2")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, sharedFeaturizer2, a1Ns1Featurizer, a1Ns2Featurizer, a2Ns1Featurizer,
        a2Ns2Featurizer, actionMergerNs1, actionMergerNs2))
    val model = pipeline.fit(df)
    val transformedDf = model.transform(df)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features_ns1")
      .setAdditionalSharedFeatures(Array("shared_features_ns2"))
      .setFeaturesCol("action_features_ns1")
      .setPredictionCol("output_prediction")
      .setAdditionalFeatures(Array("action_features_ns2"))
      .setUseBarrierExecutionMode(false)

    val m = cb.fit(transformedDf)
    assert(m.getPerformanceStatistics.select("ipsEstimate").first.getDouble(0) > 0)

    val predictSet = Seq(
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=0", "action2_ns2", 1, 1, 0.8),
      ("shared_f", "shared_f2", "action1_f", "action1_ns2", "action2_f", "action2_f2=1", "action2_ns2", 2, 1, 0.8)
    ).toDF("shared_ns1", "shared_ns2", "action1_ns1", "action1_ns2", "action2_f1_ns1", "action2_f2_ns1",
      "action2_f1_ns2", "chosen_action", "cost", "prob")
      .coalesce(1)
      .cache()

    val transformedPredictSet = model.transform(predictSet)
    val results = m.transform(transformedPredictSet)
    assert(results.collect.head.getAs[Seq[Float]]("output_prediction").length == 2)
  }

  override def testObjects(): Seq[TestObject[VowpalWabbitContextualBandit]] = {
    val trainData = CBDatasetHelper.getCBDataset(spark)
    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--quiet")
    Seq(new TestObject(
      cb,
      trainData))
  }

  override def reader: MLReadable[_] = VowpalWabbitContextualBandit

  override def modelReader: MLReadable[_] = VowpalWabbitContextualBanditModel
}
