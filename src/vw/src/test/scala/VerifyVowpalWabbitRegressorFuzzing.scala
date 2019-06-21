// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{Column, DataFrame}

class VerifyVowpalWabbitRegressorFuzzing extends EstimatorFuzzing[VowpalWabbitRegressor] {
  val numPartitions = 2

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

  override def reader: MLReadable[_] = VowpalWabbitRegressor
  override def modelReader: MLReadable[_] = VowpalWabbitRegressorModel

  override def testObjects(): Seq[TestObject[VowpalWabbitRegressor]] = {
    val fileName = "energyefficiency2012_data.train.csv"
    val columnsFilter = Some("X1,X2,X3,X4,X5,X6,X7,X8,Y1,Y2")
    val labelCol = "Y1"

    val fileLocation = DatasetUtils.regressionTrainFile(fileName).toString
    val readDataset = readCSV(fileName, fileLocation).repartition(numPartitions)
    val dataset =
      if (columnsFilter.isDefined) {
        readDataset.select(columnsFilter.get.split(",").map(new Column(_)): _*)
      } else {
        readDataset
      }

    val featuresColumn = "features"

    val featurizer = new VowpalWabbitFeaturizer()
      .setInputCols(dataset.columns.filter(col => col != labelCol))
      .setOutputCol("features")

    val vw = new VowpalWabbitRegressor()
    val predCol = "pred"
    val trainData = featurizer.transform(dataset)
    val model = vw.setLabelCol(labelCol)
      .setFeaturesCol("features")
      .setPredictionCol(predCol)
      .fit(trainData)

    Seq(new TestObject(
      vw,
      trainData))
  }
}
