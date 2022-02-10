// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split2

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.dataset.DatasetUtils.countCardinality
import com.microsoft.azure.synapse.ml.lightgbm.split1.LightGBMTestUtils
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRanker, LightGBMRankerModel, LightGBMUtils}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, _}
import org.apache.spark.sql.types.StructType

import java.io.File
import java.nio.file.{Files, Path, Paths}


//scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM Ranker module. */
class VerifyLightGBMRanker extends Benchmarks with EstimatorFuzzing[LightGBMRanker]
  with LightGBMTestUtils {

  import spark.implicits._

  val queryCol = "query"

  lazy val rankingDF: DataFrame = {
    val df1 = spark.read.format("libsvm")
      .load(DatasetUtils.rankingTrainFile("rank.train").toString)
      .withColumn("iid", monotonically_increasing_id())

    def createRows = udf((colValue: Int, index: Int) => List.fill(colValue)(index).toArray)

    val df2 = spark.read.format("csv")
      .option("inferSchema", value = true)
      .load(DatasetUtils.rankingTrainFile("rank.train.query").toString)
      .withColumn("index", monotonically_increasing_id())
      .withColumn(queryCol, explode(createRows(col("_c0"), col("index"))))
      .withColumn("iid", monotonically_increasing_id())
      .drop("_c0", "index")
      .join(df1, "iid").drop("iid")
      .withColumnRenamed("label", labelCol)
      .repartition(numPartitions)

    LightGBMUtils.getFeaturizer(df2, labelCol, "_features", groupColumn = Some(queryCol))
      .transform(df2)
      .drop("features")
      .withColumnRenamed("_features", "features")
      .cache()
  }

  def baseModel: LightGBMRanker = {
    new LightGBMRanker()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setGroupCol(queryCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setRepartitionByGroupingColumn(false)
      .setNumLeaves(5)
      .setNumIterations(10)
  }

  override def testExperiments(): Unit = {
    super.testExperiments()
  }

  override def testSerialization(): Unit = {
    super.testSerialization()
  }

  test("Verify LightGBM Ranker on ranking dataset") {
    assertFitWithoutErrors(baseModel, rankingDF)
  }

  test("Verify LightGBM Ranker on ranking dataset with repartitioning") {
    assertFitWithoutErrors(baseModel.setRepartitionByGroupingColumn(true), rankingDF)
  }

  test("Throws error when group column is not long, int or string") {
    val df = rankingDF.withColumn(queryCol, from_json(lit("{}"), StructType(Seq())))
    assertThrows[IllegalArgumentException] {
      baseModel.fit(df).transform(df).collect()
    }
  }

  test("Verify LightGBM Ranker with int, long and string query column") {
    val baseDF = Seq(
      (0L, 1, 1.2, 2.3),
      (0L, 0, 3.2, 2.35),
      (1L, 0, 1.72, 1.39),
      (1L, 1, 1.82, 3.8)
    ).toDF(queryCol, labelCol, "f1", "f2")

    val df = new VectorAssembler()
      .setInputCols(Array("f1", "f2"))
      .setOutputCol(featuresCol)
      .transform(baseDF)
      .select(queryCol, labelCol, featuresCol)

    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray), df)
    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray),
      df.withColumn(queryCol, col(queryCol).cast("Int")))
    assertFitWithoutErrors(baseModel.setEvalAt(1 to 3 toArray),
      df.withColumn(queryCol, concat(lit("str_"), col(queryCol))))
  }

  test("Verify LightGBM Ranker feature shaps") {
    val baseDF = Seq(
      (0L, 1, 1.2, 2.3),
      (0L, 0, 3.2, 2.35),
      (1L, 0, 1.72, 1.39),
      (1L, 1, 1.82, 3.8)
    ).toDF(queryCol, labelCol, "f1", "f2")

    val df = new VectorAssembler()
      .setInputCols(Array("f1", "f2"))
      .setOutputCol(featuresCol)
      .transform(baseDF)
      .select(queryCol, labelCol, featuresCol)

    val fitModel = baseModel.setEvalAt(1 to 3 toArray).fit(df)

    val featuresInput = Vectors.dense(Array[Double](0.0, 0.0))
    assert(fitModel.numFeatures == 2)
    assertFeatureShapLengths(fitModel, featuresInput, df)
    assert(fitModel.predict(featuresInput) == fitModel.getFeatureShaps(featuresInput).sum)
  }

  test("verify cardinality counts: int") {
    val counts = countCardinality(Seq(1, 1, 2, 2, 2, 3))
   assert(counts === Seq(2, 3, 1))
  }

  test("verify cardinality counts: string") {
    val counts = countCardinality(Seq("a", "a", "b", "b", "b", "c"))
    assert(counts === Seq(2, 3, 1))
  }

  test("verify with max position") {
    val maxPosition = Array(-500, -1, 0, 1, 500)
    maxPosition.foreach(position => {
      assertFitWithoutErrors(baseModel.setMaxPosition(position), rankingDF)
    })
  }

  test("verify with label gain") {
    val labelGain = Array[Double](-2.0, -0.5, 0.0, 0.5, 2.0)
    assertFitWithoutErrors(baseModel.setLabelGain(labelGain), rankingDF)
  }

  test("verify with barrier") {
    val numTasks = Array(0, 1, 2)
    numTasks.foreach(nTasks => {
      assertFitWithoutErrors(baseModel.setNumTasks(nTasks).setUseBarrierExecutionMode(true), rankingDF)
    })
  }


  test("verify string from trained model") {
    val fileName = "PimaIndian.csv"
    val outputFileName = "model.txt"
    val labelColumnName = "Diabetes mellitus"
    val colsToVerify = Array("Diabetes pedigree function", "Age (years)")
    val model = baseModel
    val trainParams = model.getTrainParams(2, rankingDF, 2)
    //val booster = baseModel.getLightGBMBooster()
/*
    val booster = model.getModelString

    val rankerModel = model.getModel. //(trainParams, booster)
    val modelString = baseModel.stringFromTrainedModel(rankerModel)
    assert(modelString == "foo")
    */
  }

  /*verifySaveBooster(
    fileName = "PimaIndian.csv",
    labelColumnName = "Diabetes mellitus",
    outputFileName = "model.txt",
    colsToVerify = Array("Diabetes pedigree function", "Age (years)"))

  def verifySaveBooster(fileName: String,
                        outputFileName: String,
                        labelColumnName: String,
                        colsToVerify: Array[String]): Unit = { */
/*
  test("Verify LightGBMClassifier save booster") {
    val fileName = "PimaIndian.csv"
    val outputFileName = "model.txt"
    val labelColumnName = "Diabetes mellitus"
    val colsToVerify = Array("Diabetes pedigree function", "Age (years)")
    val model = baseModel
    val df = loadBinary(fileName, labelColumnName)
    val fitModel = model.fit(df)

    val targetDir: Path = Paths.get(getClass.getResource("/").toURI)
    val modelPath = targetDir.toString + "/" + outputFileName
    FileUtils.deleteDirectory(new File(modelPath))
    fitModel.saveNativeModel(modelPath, overwrite = true)
    assert(Files.exists(Paths.get(modelPath)), true)

    val oldModelString = fitModel.getModel.modelStr.get
    // Verify model string contains some feature
    colsToVerify.foreach(col => oldModelString.contains(col))

    assertFitWithoutErrors(model.setModelString(oldModelString), df)

    // Verify can load model from file
    val resultsFromString = LightGBMRankerModel
      .loadNativeModelFromString(oldModelString)
      .setFeaturesCol(featuresCol)
      .setLeafPredictionCol(leafPredCol)
      .setFeaturesShapCol(featuresShapCol)
      .transform(df)

    val resultsFromFile = LightGBMRankerModel
      .loadNativeModelFromFile(modelPath)
      .setFeaturesCol(featuresCol)
      .setLeafPredictionCol(leafPredCol)
      .setFeaturesShapCol(featuresShapCol)
      .transform(df)

    val resultsOriginal = fitModel.transform(df)

    assert(resultsFromString === resultsOriginal)
    assert(resultsFromFile === resultsOriginal)
  }*/

  override def testObjects(): Seq[TestObject[LightGBMRanker]] = {
    Seq(new TestObject(baseModel, rankingDF))
  }

  override def reader: MLReadable[_] = LightGBMRanker

  override def modelReader: MLReadable[_] = LightGBMRankerModel

 }
