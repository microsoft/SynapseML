// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.TaskContext
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.io.File

class VerifyVowpalWabbitClassifier extends Benchmarks with EstimatorFuzzing[VowpalWabbitClassifier] {
  lazy val moduleName = "vw"
  val numPartitions = 2

  def getAlaTrainDataFrame(localNumPartitions: Int = numPartitions): Dataset[Row] = {
    val fileLocation = DatasetUtils.binaryTrainFile("a1a.train.svmlight").toString
    spark.read.format("libsvm").load(fileLocation).repartition(localNumPartitions)
  }

  test ("Verify VowpalWabbit Classifier can save native model and test audit output") {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()

    val model = vw.fit(dataset)

    // print audit information
    model.setTestArgs("--audit")
    model.transform(dataset.limit(3)).show()

    // save model
    val saveDir = new File(tmpDir.toFile, "vw-model")

    model.saveNativeModel(saveDir.toString)

    assert(saveDir.exists)
    assert(saveDir.length() > 100)
  }

  test ("Verify VowpalWabbit Classifier returns diagnostics") {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()
      .setNumBits(10)
      .setLearningRate(3.1)
      .setPowerT(0)
      .setLabelConversion(false)

    val model = vw.fit(dataset)

    model.getPerformanceStatistics.show

    assert(model.getPerformanceStatistics.count == numPartitions)

    val perfRow = model.getPerformanceStatistics.where("partitionId == 0").collect.head

    assert(perfRow.getDouble(perfRow.fieldIndex("powerT")) == 0.0)
    assert(803 + 802 == model.getPerformanceStatistics.agg(sum("numberOfExamplesPerPass")).collect().head.getLong(0))
  }

  test ("Verify VowpalWabbit Classifier accept parameters") {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()
      .setNumBits(10)
      .setLearningRate(3.1)
      .setPowerT(0)
      .setL1(1e-5)
      .setL2(1e-6)

    val model = vw.fit(dataset)

    assert(model.vwArgs.getNumBits == 10)
  }

  test ("Verify VowpalWabbit Classifier audit") {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()
      .setPassThroughArgs("-a")

    vw.fit(dataset.limit(10))
  }

  test("Verify VowpalWabbit Classifier can be run with TrainValidationSplit") {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()
        .setPassThroughArgs("--link=logistic --quiet")
        .setLabelConversion(false)

    val paramGrid = new ParamGridBuilder()
       .addGrid(vw.learningRate, Array(0.5, 0.05, 0.001))
       .addGrid(vw.numPasses, Array(1, 3, 5))
       .build()

    val cv = new CrossValidator()
      .setEstimator(vw)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setParallelism(2) // thanks to dynamic port assignment in spanning_tree.cc :)

    val model = cv.fit(dataset)
    model.transform(dataset)

    val auc = model.avgMetrics(0)

    println(s"areaUnderROC: $auc")
    assert(auc > 0.85)
  }

  test("Verify VowpalWabbit Classifier can convert 0/1 labels") {
    val dataset = getAlaTrainDataFrame()
    // convert -1/1 to 0/1
    val df = dataset.withColumn("label01", (col("label") + 1) / 2)

    val vw = new VowpalWabbitClassifier()
      .setLabelCol("label01")
      .setLabelConversion(true)

    val classifier = vw.fit(df)
    val labelOneCnt = classifier.transform(df).select("prediction").filter(_.getDouble(0) == 1.0).count()

    assert(labelOneCnt == 275)
  }

  private def testVerifyVowpalWabbitClassifierWithLibSVM(useBarrierMode: Boolean): Unit = {
    val dataset = getAlaTrainDataFrame()

    val vw = new VowpalWabbitClassifier()
      .setPassThroughArgs("--passes 3")
      .setPowerT(0.3)
      .setNumPasses(3)
      .setUseBarrierExecutionMode(useBarrierMode)
      .setLabelConversion(false)

    val classifier = vw.fit(dataset)
    assert(classifier.getModel.length > 400)

    val labelOneCnt = classifier.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    assert(labelOneCnt < dataset.count)
    assert(labelOneCnt > 10)

    val readableModel = classifier.getReadableModel
    assert(readableModel.length > 10)
  }

  test("Verify VowpalWabbit Classifier can be run with libsvm (barrier mode)") {
    testVerifyVowpalWabbitClassifierWithLibSVM(true)
  }

  test("Verify VowpalWabbit Classifier can be run with libsvm (no barrier mode)") {
    testVerifyVowpalWabbitClassifierWithLibSVM(false)
  }

  test("Verify VowpalWabbit Classifier does not generate duplicate options (short)") {
    val dataset = getAlaTrainDataFrame(3)

    println(s"dataset partitions ${dataset.rdd.getNumPartitions}")

    val vw = new VowpalWabbitClassifier()
      .setPassThroughArgs("-b 15")
      .setNumBits(22)
      .setLabelConversion(false)

    val classifier = vw.fit(dataset)

    // command line args take precedence
    assert(classifier.vwArgs.getNumBits == 15)
  }

  test("Verify VowpalWabbit Classifier does not generate duplicate options (long)") {
    val dataset = getAlaTrainDataFrame(3)

    val vw = new VowpalWabbitClassifier()
      .setPassThroughArgs("-b 4")
      .setNumBits(22)
      .setLabelConversion(false)

    // command line args take precedence
    assert(vw.fit(dataset).vwArgs.getNumBits == 4)
  }

  test("Verify VowpalWabbit Classifier can deal with empty partitions") {
    val dataset = getAlaTrainDataFrame(3)

    val vw = new VowpalWabbitClassifier()
      .setPowerT(0.3)
      .setNumPasses(3)
      .setLabelConversion(false)

    val infoEnc = ExpressionEncoder(dataset.schema)
    val trainData = dataset
      .mapPartitions(iter => {
        val ctx = TaskContext.get
        val partId = ctx.partitionId
        // Create an empty partition
        if (partId == 0) {
          Iterator()
        } else {
          iter
        }
      })(infoEnc)

    val classifier = vw.fit(trainData)
    assert(classifier.getModel.length > 400)
  }

  test("Verify VowpalWabbit Classifier w/ and w/o link=logistic produce same results") {
    val dataset = getAlaTrainDataFrame()

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Predicting-probabilities
    val vw1 = new VowpalWabbitClassifier()
        .setNumPasses(2)
        .setLabelConversion(false)

    val classifier1 = vw1.fit(dataset)

    val vw2 = new VowpalWabbitClassifier()
        .setPassThroughArgs("--link=logistic")
        .setNumPasses(2)
        .setLabelConversion(false)

    val classifier2 = vw2.fit(dataset)

    val labelOneCnt1 = classifier1.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()
    val labelOneCnt2 = classifier2.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    assert(labelOneCnt1 == labelOneCnt2)
  }

//  test("Verify VowpalWabbit Classifier w/ bfgs and cache file") {
//    val dataset = getAlaTrainDataFrame()
//
//    val vw1 = new VowpalWabbitClassifier()
//      .setNumPasses(3)
//      .setPassThroughArgs("--loss_function=logistic --bfgs")
//      .setLabelConversion(false)
//
//    val classifier1 = vw1.fit(dataset)
//
//    val labelOneCnt1 = classifier1.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()
//
//    println(labelOneCnt1)
//  }

  case class ClassificationInput[T](label: Int, in: T)

  test("Verify VowpalWabbit Classifier w/ ngrams") {
    val featurizer = new VowpalWabbitFeaturizer()
      .setStringSplitInputCols(Array("in"))
      .setPreserveOrderNumBits(2)
      .setNumBits(18)
      .setPrefixStringsWithColumnName(false)
      .setOutputCol("features")

    val dataset  = spark.createDataFrame(Seq(
      ClassificationInput[String](1, "marie markus fun"),
      ClassificationInput[String](0, "marie markus no fun")
    )).repartition(1)

    val datasetFeaturized = featurizer.transform(dataset)

    val vw1 = new VowpalWabbitClassifier()
      .setPassThroughArgs("--ngram f2 -a")
    val classifier1 = vw1.fit(datasetFeaturized)

    // 3 (words) + 2 (ngrams) + 1 (constant) = 6
    // 4 (words) + 3 (ngrams) + 1 (constant) = 8

    assert(classifier1.getPerformanceStatistics.select("totalNumberOfFeatures").head.get(0) == 14)
  }

  val numClasses = 11

  def getVowelTrainDataFrame(localNumPartitions: Int): Dataset[Row] = {
    val fileLocation = DatasetUtils.multiclassTrainFile(s"vowel.train.svmlight").toString
    spark.read.format("libsvm")
      .load(fileLocation)
      .repartition(localNumPartitions)
      .select(col("features"), col("label").cast(IntegerType).alias("label"))
  }

  def getVowelTestDataFrame(localNumPartitions: Int): Dataset[Row] = {
    val fileLocation = DatasetUtils.multiclassTestFile(s"vowel.test.svmlight").toString
    spark.read.format("libsvm")
      .load(fileLocation)
      .repartition(localNumPartitions)
      .select(col("features"), col("label").cast(IntegerType).alias("label"))
  }

  test("Verify VowpalWabbit Multiclass Classifier") {
    val train = getVowelTrainDataFrame(1)
    val test = getVowelTestDataFrame(2)

    // find max-label
    // train.select(max(col("label"))).show()

    val vw = new VowpalWabbitClassifier()
      .setNumClasses(numClasses)
      .setNumPasses(30)
      .setPassThroughArgs(s"--oaa $numClasses --indexing 0 --quiet --holdout_off --loss_function=logistic -q ::")

    val model = vw.fit(train)

    // for evaluation to work need to pass --indexing 0
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    assert(evaluator.evaluate(model.transform(train)) > 0.9)
    assert(evaluator.evaluate(model.transform(test)) > 0.5)
  }

  test("Verify VowpalWabbit Multiclass Classifier Probability output") {
    val train = getVowelTrainDataFrame(1)

    val vw = new VowpalWabbitClassifier()
      .setNumClasses(numClasses)
      .setPassThroughArgs(s"--oaa $numClasses --indexing 0 --quiet --holdout_off --loss_function=logistic")

    val model = vw.fit(train)

    model.setTestArgs("--probabilities --loss_function=logistic")

//    model.transform(train).show()

    val outputSchema = model.transform(train).schema

    assert(outputSchema("prediction").dataType == DoubleType)
    assert(outputSchema("probability").dataType == org.apache.spark.ml.linalg.SQLDataTypes.VectorType)
  }

  /** Reads a CSV file given the file name and file location.
    * @param fileName The name of the csv file.
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileName: String, fileLocation: String): DataFrame = {
    spark.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  override def reader: MLReadable[_] = VowpalWabbitClassifier
  override def modelReader: MLReadable[_] = VowpalWabbitClassificationModel

  override def testObjects(): Seq[TestObject[VowpalWabbitClassifier]] = {
    val dataset = getAlaTrainDataFrame()
    val datasetMulti = getVowelTrainDataFrame(1)

    Seq(new TestObject(
      new VowpalWabbitClassifier(),
      dataset),
      new TestObject(
        new VowpalWabbitClassifier()
          .setNumClasses(numClasses),
        datasetMulti))
  }
}

