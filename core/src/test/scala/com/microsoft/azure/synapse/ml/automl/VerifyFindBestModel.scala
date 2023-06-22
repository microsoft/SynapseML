// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{functions => F}

import java.io.File

class VerifyFindBestModel extends EstimatorFuzzing[FindBestModel]{
  import com.microsoft.azure.synapse.ml.train.TrainClassifierTestUtilities._

  val mockLabelColumn = "Label"

  def createMockDataset: DataFrame = {
    spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 4, 0.78, 0.99, 2),
      (1, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")
  }

  test("Smoke test to verify that evaluate can be run") {
    val dataset = createMockDataset
    val randomForestClassifier = createRF.setLabelCol(mockLabelColumn)
    val model = randomForestClassifier.fit(dataset)
    val findBestModel = new FindBestModel()
      .setModels(Array(model.asInstanceOf[Transformer], model.asInstanceOf[Transformer]))
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
    val bestModel = findBestModel.fit(dataset)
    bestModel.transform(dataset)
  }

  test("Verify the best model can be saved") {
    val dataset: DataFrame = createMockDataset
    val logisticRegressor = createLR.setLabelCol(mockLabelColumn)
    val model = logisticRegressor.fit(dataset)

    val findBestModel = new FindBestModel()
      .setModels(Array(model.asInstanceOf[Transformer], model.asInstanceOf[Transformer]))
      .setEvaluationMetric(MetricConstants.AucSparkMetric)
    val bestModel = findBestModel.fit(dataset)

    val myModelFile = new File(tmpDir.toFile, "testEvalModel")
    bestModel.save(myModelFile.toString)
    assert(myModelFile.exists())
  }

  test("Verify the best model metrics can be retrieved and are valid") {
    val dataset: DataFrame = createMockDataset
    val logisticRegressor = createLR.setLabelCol(mockLabelColumn)
    val decisionTreeClassifier = createDT.setLabelCol(mockLabelColumn)
    val gbtClassifier = createGBT.setLabelCol(mockLabelColumn)
    val naiveBayesClassifier = createNB.setLabelCol(mockLabelColumn)
    val randomForestClassifier = createRF.setLabelCol(mockLabelColumn)
    val model1 = logisticRegressor.fit(dataset)
    val model2 = decisionTreeClassifier.fit(dataset)
    val model3 = gbtClassifier.fit(dataset)
    val model4 = naiveBayesClassifier.fit(dataset)
    val model5 = randomForestClassifier.fit(dataset)

    val findBestModel = new FindBestModel()
      .setModels(Array(model1.asInstanceOf[Transformer], model2, model3, model4, model5))
      .setEvaluationMetric(MetricConstants.AucSparkMetric)
    val bestModel = findBestModel.fit(dataset)
    // validate schema is as expected
    assert(bestModel.getAllModelMetrics.schema ==
      StructType(Seq(StructField(FindBestModel.ModelNameCol, StringType, true),
        StructField(FindBestModel.MetricsCol, DoubleType, false),
        StructField(FindBestModel.ParamsCol, StringType, true))))
    // validate we got metrics for every model
    assert(bestModel.getAllModelMetrics.count() == 5)
    // validate AUC looks valid
    bestModel.getAllModelMetrics
      .select(FindBestModel.MetricsCol)
      .collect()
      .foreach(value => assert(value.getDouble(0) >= 0.5))

    val highestAUC = bestModel.getAllModelMetrics
      .select(F.max(FindBestModel.MetricsCol))
      .collect()
      .head
      .getDouble(0)

    val bestModelsId = bestModel.getAllModelMetrics
      .where(F.col(FindBestModel.MetricsCol) >= highestAUC)
      .select(FindBestModel.ModelNameCol)
      .collect()
      .map(_.getString(0))

    assert(bestModelsId.contains(bestModel.getBestModel.uid))
  }

  val reader: MLReadable[_] = FindBestModel
  val modelReader: MLReadable[_] = BestModel

  override def testObjects(): Seq[TestObject[FindBestModel]] = Seq(new TestObject({
    val randomForestClassifier = createRF.setLabelCol(mockLabelColumn)
    val model = randomForestClassifier.fit(createMockDataset)
    new FindBestModel()
      .setModels(Array(model.asInstanceOf[Transformer], model))
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)}, createMockDataset))
}
