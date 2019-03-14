// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.File

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.ml.feature.{Binarizer, StringIndexer}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.linalg.Vector

class VerifyVowpalWabbitClassifier extends Benchmarks { // with EstimatorFuzzing[VowpalWabbitClassifier] {
  lazy val moduleName = "vw"
  val numPartitions = 2

  test("Verify VowpalWabbit Classifier can be run with TrainValidationSplit") {
    val fileName = "PimaIndian.csv"
    val labelColumnName = "Diabetes mellitus"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = readCSV(fileName, fileLocation).repartition(numPartitions)

    dataset.show
  }

  test("Verify VowpalWabbit Classifier can be run with libsvm") {
    val fileName = "a1a.train.svmlight"
    val labelColumnName = "Diabetes mellitus"

    val fileLocation = DatasetUtils.binaryTrainFile(fileName).toString
    val dataset = session.read.format("libsvm").load(fileLocation).repartition(numPartitions)

    var vw = new VowpalWabbitClassifier()

    // dataset.show
    val classifier = vw.fit(dataset)
    println(classifier.model.model.length)

    // val labelOneCnt = model.transform(dataset).select("prediction").filter(_.getDouble(0) == 1.0).count()

    //assert(labelOneCnt > 10)
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
}
