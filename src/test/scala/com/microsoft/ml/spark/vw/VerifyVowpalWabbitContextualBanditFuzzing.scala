package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.core.env.FileUtilities
import com.microsoft.ml.spark.core.test.benchmarks.DatasetUtils
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class VerifyVowpalWabbitContextualBanditFuzzing extends EstimatorFuzzing[VowpalWabbitContextualBandit]{
  def readCSV(fileName: String, fileLocation: String): DataFrame = {
    session.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  override def testObjects(): Seq[TestObject[VowpalWabbitContextualBandit]] = {
    val fileName = "cbdata.train.csv"
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,"VowpalWabbit", "Train", fileName).toString
    val rawDataset = readCSV(fileName, fileLocation)
      .repartition(1)

    rawDataset.printSchema()

    // schema inference didn't seem to work so we need to convert a few columns.
    val dataset = rawDataset
      .withColumn("chosen_action", col("chosen_action").cast("Int"))
      .withColumn("cost", col("cost").cast("Double"))
      .withColumn("prob", col("prob").cast("Double"))

    dataset.printSchema()
    dataset.show(false)
    println("count")
    println(dataset.count())
    println("count")
    val sharedFeaturizer = new VowpalWabbitFeaturizer()
      .setInputCols(Array("shared_id","shared_major","shared_hobby","shared_fav_character"))
      .setOutputCol("shared_features")

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
      .setInputCols(Array("action1_features", "action2_features","action3_features","action4_features","action5_features"))
      .setOutputCol("action_features")

    val pipeline = new Pipeline()
      .setStages(Array(sharedFeaturizer, actionOneFeaturizer, actionTwoFeaturizer, actionThreeFeaturizer,
        actionFourFeaturizer, actionFiveFeaturizer, actionMerger))

    val transformPipeline = pipeline.fit(dataset)

    val cb = new VowpalWabbitContextualBandit()
      .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")
      .setLabelCol("cost")
      .setProbabilityCol("prob")
      .setChosenActionCol("chosen_action")
      .setSharedCol("shared_features")
      .setFeaturesCol("action_features")

    val trainData = transformPipeline.transform(dataset)
    trainData.show(false)
    val model = cb.fit(trainData)

    Seq(new TestObject(
      cb,
      trainData))
  }

  override def reader: MLReadable[_] = VowpalWabbitContextualBandit

  override def modelReader: MLReadable[_] = VowpalWabbitContextualBanditModel
}
