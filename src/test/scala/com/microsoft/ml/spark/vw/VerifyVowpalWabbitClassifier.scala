// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import com.microsoft.ml.spark.core.test.benchmarks.{Benchmarks, DatasetUtils}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

class VerifyVowpalWabbitClassifier extends Benchmarks with EstimatorFuzzing[VowpalWabbitClassifier] {
  lazy val moduleName = "vw"
  val numPartitions = 2

  test("Verify VowpalWabbit Classifier can be run with TrainValidationSplit") {
      val fileName = "a1a.train.svmlight"

      val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
      val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    val vw = new VowpalWabbitClassifier()
        .setArgs("--link=logistic --quiet")

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
  }

  test("Verify VowpalWabbit Classifier can be run with libsvm") {
    val fileName = "a1a.train.svmlight"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    val vw = new VowpalWabbitClassifier()
      .setPowerT(0.3)
      .setNumPasses(3)

    val classifier = vw.fit(dataset)
    assert(classifier.getModel.length > 400)

    val labelOneCnt = classifier.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    assert(labelOneCnt < dataset.count)
    assert(labelOneCnt > 10)
  }

  test("Verify VowpalWabbit Classifier w/ and w/o link=logistic produce same results") {
    val fileName = "a1a.train.svmlight"
    val labelColumnName = "Diabetes mellitus"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Predicting-probabilities
    val vw1 = new VowpalWabbitClassifier()
        .setNumPasses(2)
    val classifier1 = vw1.fit(dataset)

    val vw2 = new VowpalWabbitClassifier()
        .setArgs("--link=logistic")
        .setNumPasses(2)
    val classifier2 = vw1.fit(dataset)

    val labelOneCnt1 = classifier1.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()
    val labelOneCnt2 = classifier2.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    assert(labelOneCnt1 == labelOneCnt2)
  }

  test("Verify VowpalWabbit Classifier w/ bfgs and cache file") {
    val fileName = "a1a.train.svmlight"
    val labelColumnName = "Diabetes mellitus"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    val vw1 = new VowpalWabbitClassifier()
      .setNumPasses(3)
      .setArgs("--loss_function=logistic --bfgs")
    val classifier1 = vw1.fit(dataset)

    val labelOneCnt1 = classifier1.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    println(labelOneCnt1)
  }

  /** Reads a CSV file given the file name and file location.
    * @param fileName The name of the csv file.
    * @param fileLocation The full path to the csv file.
    * @return A dataframe from read CSV file.
    */
  def readCSV(fileName: String, fileLocation: String): DataFrame = {
    session.read
      .option("header", "true").option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
      .csv(fileLocation)
  }

  override def reader: MLReadable[_] = VowpalWabbitClassifier
  override def modelReader: MLReadable[_] = VowpalWabbitClassificationModel

  override def testObjects(): Seq[TestObject[VowpalWabbitClassifier]] = {

    val fileName = "a1a.train.svmlight"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    Seq(new TestObject(
      new VowpalWabbitClassifier(),
      dataset))
  }
}
