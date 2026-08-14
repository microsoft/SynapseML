// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import com.microsoft.azure.synapse.ml.core.test.benchmarks.DatasetUtils
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.train.TrainClassifierTestUtilities._
import com.microsoft.azure.synapse.ml.train.TrainRegressorTestUtilities._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.FastVectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.util.Random

/** Tests to validate the functionality of Evaluate Model module. */
class VerifyComputeModelStatistics extends TransformerFuzzing[ComputeModelStatistics] {
  val labelColumn = "Label"
  lazy val dataset: DataFrame = spark.createDataFrame(Seq(
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
    (1, 4, 0.12, 0.34, 3)
  )).toDF(labelColumn, "col1", "col2", "col3", "col4")

  private lazy val rankedBinaryDataset: DataFrame = {
    import spark.implicits._
    val data = (1 to 100).map { rank =>
      val label = if (rank == 1 || rank == 11) 1.0 else 0.0
      val prediction = if (rank <= 10) 1.0 else 0.0
      val rawPrediction = Vectors.dense(0.0, (101 - rank).toDouble)
      (label, prediction, rawPrediction)
    }.toDF("label", SchemaConstants.SparkPredictionColumn, SchemaConstants.SparkRawPredictionColumn)
    val modelName = SchemaConstants.ScoreModelPrefix + "_ranked binary"
    val withLabel = SparkSchema.setLabelColumnName(
      data, modelName, "label", SchemaConstants.ClassificationKind)
    val withPrediction = SparkSchema.updateColumnMetadata(
      withLabel, modelName, SchemaConstants.SparkPredictionColumn, SchemaConstants.ClassificationKind)
    SparkSchema.updateColumnMetadata(
      withPrediction, modelName, SchemaConstants.SparkRawPredictionColumn, SchemaConstants.ClassificationKind)
  }

  private def rankedBinaryStatistics(metric: String): ComputeModelStatistics =
    new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol(SchemaConstants.SparkPredictionColumn)
      .setScoresCol(SchemaConstants.SparkRawPredictionColumn)
      .setEvaluationMetric(metric)

  private def assertBinaryOnlyMetricsRejected(schema: StructType): Unit = {
    Seq(
      MetricConstants.AucSparkMetric -> "Error: AUC is not available for multiclass case",
      MetricConstants.AreaUnderROCMetric -> "Error: AUC is not available for multiclass case",
      MetricConstants.AreaUnderPRMetric -> "Error: areaUnderPR is not available for multiclass case")
      .foreach { case (metric, expectedMessage) =>
        val error = intercept[IllegalArgumentException] {
          new ComputeModelStatistics()
            .setLabelCol("label")
            .setEvaluationMetric(metric)
            .transformSchema(schema)
        }

        assert(error.getMessage === expectedMessage)
      }
  }

  test("areaUnderPR uses Spark trapezoidal precision-recall AUC") {
    val evaluator = rankedBinaryStatistics(MetricConstants.AreaUnderPRMetric)
    val result = evaluator.transform(rankedBinaryDataset)
    val areaUnderPR = result.first().getAs[Double](MetricConstants.AreaUnderPRColumnName)

    assert(result.columns.last === MetricConstants.AreaUnderPRColumnName)
    assert(result.columns.contains(MetricConstants.AreaUnderPRColumnName))
    assert(evaluator.transformSchema(rankedBinaryDataset.schema) ===
      StructType(Array(StructField(MetricConstants.AreaUnderPRColumnName, DoubleType))))
    assert(math.abs(areaUnderPR - 251.0 / 440.0) < 1e-8)
    assert(math.abs(areaUnderPR - 13.0 / 22.0) > 0.01)
  }

  test("areaUnderROC remains an AUC output alias") {
    val aucEvaluator = rankedBinaryStatistics(MetricConstants.AucSparkMetric)
    val auc = aucEvaluator
      .transform(rankedBinaryDataset)
      .first()
      .getAs[Double](MetricConstants.AucColumnName)
    val rocEvaluator = rankedBinaryStatistics(MetricConstants.AreaUnderROCMetric)
    val areaUnderROCAlias = rocEvaluator
      .transform(rankedBinaryDataset)
      .first()
      .getAs[Double](MetricConstants.AucColumnName)
    val aucSchema = StructType(Array(StructField(MetricConstants.AucColumnName, DoubleType)))

    assert(aucEvaluator.transformSchema(rankedBinaryDataset.schema) === aucSchema)
    assert(rocEvaluator.transformSchema(rankedBinaryDataset.schema) === aucSchema)
    assert(math.abs(auc - 187.0 / 196.0) < 1e-8)
    assert(math.abs(areaUnderROCAlias - 187.0 / 196.0) < 1e-8)
  }

  test("all and classification metrics append binary metrics in runtime output order") {
    val binaryDataset = CategoricalUtilities.setLevels(
      rankedBinaryDataset,
      "label",
      Array(0.0, 1.0))
    val expectedColumns = List(MetricConstants.EvaluationType, MetricConstants.ConfusionMatrix) ++
      MetricConstants.BinaryClassificationColumns
    Seq(MetricConstants.AllSparkMetrics, MetricConstants.ClassificationMetricsName).foreach { metric =>
      val evaluator = rankedBinaryStatistics(metric)
      val result = evaluator.transform(binaryDataset)
      val row = result.first()
      val transformedSchema = evaluator.transformSchema(binaryDataset.schema)

      assert(result.columns.toList === expectedColumns)
      assert(transformedSchema ===
        StructType(MetricConstants.BinaryClassificationColumns.map(StructField(_, DoubleType))))
      assert(math.abs(row.getAs[Double](MetricConstants.AucColumnName) - 187.0 / 196.0) < 1e-8)
      assert(math.abs(row.getAs[Double](MetricConstants.AreaUnderPRColumnName) - 251.0 / 440.0) < 1e-8)
    }
  }

  test("multiclass classification schema retains the legacy common metrics") {
    val multiclassData = spark.createDataFrame(Seq(
      (0.0, 0.0),
      (1.0, 1.0),
      (2.0, 2.0))).toDF("label", "prediction")
    val modelName = SchemaConstants.ScoreModelPrefix + "_multiclass"
    val withLabel = SparkSchema.setLabelColumnName(
      multiclassData, modelName, "label", SchemaConstants.ClassificationKind)
    val withPrediction = SparkSchema.updateColumnMetadata(
      withLabel, modelName, "prediction", SchemaConstants.ClassificationKind)
    val multiclass = CategoricalUtilities.setLevels(
      withPrediction,
      "label",
      Array(0.0, 1.0, 2.0))
    val expectedRuntimeColumns =
      List(MetricConstants.EvaluationType, MetricConstants.ConfusionMatrix) ++
        MetricConstants.ClassificationColumns ++
        List(MetricConstants.AverageAccuracy,
          MetricConstants.MacroAveragedPrecision,
          MetricConstants.MacroAveragedRecall)

    Seq(MetricConstants.AllSparkMetrics, MetricConstants.ClassificationMetricsName).foreach { metric =>
      val evaluator = new ComputeModelStatistics().setEvaluationMetric(metric)
      val schema = evaluator.transformSchema(multiclass.schema)
      val result = evaluator.transform(multiclass)

      assert(schema.fieldNames.toList === MetricConstants.ClassificationColumns)
      assert(!schema.fieldNames.contains(MetricConstants.AucColumnName))
      assert(!schema.fieldNames.contains(MetricConstants.AreaUnderPRColumnName))
      assert(result.columns.toList === expectedRuntimeColumns)
    }
  }

  test("classification schema without cardinality metadata retains the legacy common metrics") {
    Seq(MetricConstants.AllSparkMetrics, MetricConstants.ClassificationMetricsName).foreach { metric =>
      val schema = rankedBinaryStatistics(metric).transformSchema(rankedBinaryDataset.schema)

      assert(schema.fieldNames.toList === MetricConstants.ClassificationColumns)
    }
  }

  test("transformSchema rejects binary-only metrics for multiclass MML categorical labels") {
    val schema = CategoricalUtilities.setLevels(
      spark.createDataFrame(Seq(
        (0.0, 0.0),
        (1.0, 1.0),
        (2.0, 2.0))).toDF("label", "prediction"),
      "label",
      Array(0.0, 1.0, 2.0)).schema

    assertBinaryOnlyMetricsRejected(schema)
  }

  test("transformSchema preserves binary-only metric schema when configured labelCol is absent") {
    val schema = spark.createDataFrame(Seq(
      (0.0, 0.9),
      (1.0, 0.8))).toDF("prediction", "rawPrediction").schema
    val evaluator = new ComputeModelStatistics()
      .setLabelCol("label")
      .setEvaluationMetric(MetricConstants.AreaUnderPRMetric)

    assert(evaluator.transformSchema(schema) ===
      StructType(Array(StructField(MetricConstants.AreaUnderPRColumnName, DoubleType))))
  }

  test("areaUnderPR rejects multiclass and unsupported metric inputs") {
    val multiclass = spark.createDataFrame(Seq(
      (0.0, 0.0, 0.9),
      (1.0, 1.0, 0.8),
      (2.0, 2.0, 0.7))).toDF("label", "prediction", "rawPrediction")
    val multiclassError = intercept[Exception] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setScoresCol("rawPrediction")
        .setEvaluationMetric(MetricConstants.AreaUnderPRMetric)
        .transform(multiclass)
    }
    assert(multiclassError.getMessage === "Error: areaUnderPR is not available for multiclass case")

    assertThrows[Exception] {
      rankedBinaryStatistics("averagePrecision").transform(rankedBinaryDataset)
    }
  private def addScoredModelMetadata(dataset: DataFrame,
                                     modelName: String,
                                     labelCol: String,
                                     scoreValueKind: String): DataFrame = {
    val withLabel = SparkSchema.setLabelColumnName(dataset, modelName, labelCol, scoreValueKind)
    SparkSchema.updateColumnMetadata(
      withLabel, modelName, SchemaConstants.SparkPredictionColumn, scoreValueKind)
  }

  test("Explicit settings select classification after a stale regression score is dropped") {
    val regressionLabel = "regressionLabel"
    val input = dataset
      .withColumn(regressionLabel, col("col2") + col("col3"))
      .select(col(regressionLabel), col(labelColumn), col("col1"), col("col2"), col("col3"), col("col4"))
    val regressionScored = createLinearRegressor(regressionLabel).fit(input).transform(input)

    val regressionEvaluation = new ComputeModelStatistics().transform(regressionScored)
    assert(regressionEvaluation.columns.contains(MetricConstants.MseColumnName))

    val classifierInput = regressionScored.drop(SchemaConstants.SparkPredictionColumn)
    assert(classifierInput.schema(regressionLabel).metadata.contains(SchemaConstants.MMLTag))
    val classificationScored = createLR.setLabelCol(labelColumn).fit(classifierInput).transform(classifierInput)
    val classificationEvaluation = new ComputeModelStatistics()
      .setLabelCol(labelColumn)
      .setScoredLabelsCol(SchemaConstants.SparkPredictionColumn)
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
      .transform(classificationScored)

    assert(classificationEvaluation.columns.contains(MetricConstants.AccuracyColumnName))
    assert(!classificationEvaluation.columns.contains(MetricConstants.MseColumnName))
  }

  test("Explicit columns and metric beat unrelated scored-model metadata") {
    val unrelatedLabel = "unrelatedLabel"
    val selectedLabel = "selectedLabel"
    val selectedPrediction = "selectedPrediction"
    val unrelatedModel = SchemaConstants.ScoreModelPrefix + "_unrelated"
    val wrongKindModel = SchemaConstants.ScoreModelPrefix + "_wrong_kind"
    val input = spark.createDataFrame(Seq(
      (1.0, 0.0, 1.0, 0.0),
      (0.0, 1.0, 0.0, 1.0),
      (1.0, 0.0, 1.0, 0.0),
      (0.0, 1.0, 0.0, 1.0)))
      .toDF(unrelatedLabel, selectedLabel, SchemaConstants.SparkPredictionColumn, selectedPrediction)
    val withUnrelatedModel = addScoredModelMetadata(
      input, unrelatedModel, unrelatedLabel, SchemaConstants.ClassificationKind)
    val scored = addScoredModelMetadata(
      withUnrelatedModel, wrongKindModel, selectedLabel, SchemaConstants.RegressionKind)

    val result = new ComputeModelStatistics()
      .setLabelCol(selectedLabel)
      .setScoredLabelsCol(selectedPrediction)
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
      .transform(scored)

    assert(result.first().getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)
  }

  test("Multiple complete scored-model metadata candidates fail deterministically") {
    val modelA = SchemaConstants.ScoreModelPrefix + "_a"
    val modelB = SchemaConstants.ScoreModelPrefix + "_b"
    val labelA = "labelA"
    val labelB = "labelB"
    val input = spark.createDataFrame(Seq((0.0, 1.0, 0.0)))
      .toDF(labelA, labelB, SchemaConstants.SparkPredictionColumn)
    val withModelB = addScoredModelMetadata(
      input, modelB, labelB, SchemaConstants.ClassificationKind)
    val withBothModels = addScoredModelMetadata(
      withModelB, modelA, labelA, SchemaConstants.RegressionKind)

    val error = intercept[IllegalArgumentException] {
      new ComputeModelStatistics().transformSchema(withBothModels.schema)
    }
    val expectedCandidates =
      s"[$modelA (label=$labelA, kind=${SchemaConstants.RegressionKind}, " +
        s"prediction=${SchemaConstants.SparkPredictionColumn}), " +
        s"$modelB (label=$labelB, kind=${SchemaConstants.ClassificationKind}, " +
        s"prediction=${SchemaConstants.SparkPredictionColumn})]"
    assert(error.getMessage.contains(expectedCandidates))
    assert(error.getMessage.contains("Set labelCol and evaluationMetric"))
  }

  test("Missing default score column produces an actionable error") {
    val label = "label"
    val input = spark.createDataFrame(Seq((0.0, 1.0), (1.0, 2.0))).toDF(label, "feature")
    val error = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol(label)
        .setEvaluationMetric(MetricConstants.RegressionMetricsName)
        .transform(input)
    }

    assert(error.getMessage.contains("regression prediction/score column <unresolved>"))
    assert(error.getMessage.contains("setScoresCol"))
    assert(error.getMessage.contains("Available columns: [feature, label]"))
  }

  test("Invalid explicit scores column fails only for score-consuming metrics") {
    val label = "label"
    val prediction = "selectedPrediction"
    val input = spark.createDataFrame(Seq((0.0, 0.0), (1.0, 1.0))).toDF(label, prediction)
    val statistics = new ComputeModelStatistics()
      .setLabelCol(label)
      .setScoredLabelsCol(prediction)
      .setScoresCol("missingScore")

    val accuracy = statistics
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
      .transform(input)
      .first()
      .getAs[Double](MetricConstants.AccuracyColumnName)
    assert(accuracy === 1.0)

    val error = intercept[IllegalArgumentException] {
      statistics
        .setEvaluationMetric(MetricConstants.AucSparkMetric)
        .transform(input)
    }

    assert(error.getMessage.contains("classification score column 'missingScore'"))
    assert(error.getMessage.contains("setScoresCol"))
    assert(error.getMessage.contains("Available columns: [label, selectedPrediction]"))
  }

  test("Single complete scored-model metadata remains supported") {
    val modelName = SchemaConstants.ScoreModelPrefix + "_single"
    val label = "label"
    val input = spark.createDataFrame(Seq((0.0, 0.0), (1.0, 1.0)))
      .toDF(label, SchemaConstants.SparkPredictionColumn)
    val scored = addScoredModelMetadata(input, modelName, label, SchemaConstants.RegressionKind)

    val result = new ComputeModelStatistics().transform(scored)

    assert(result.first().getAs[Double](MetricConstants.MseColumnName) === 0.0)
  }

  test("Verify multiclass evaluation is not slow for large number of labels") {
    val numRows = 4096
    import spark.implicits._
    val rand = new Random(1337)
    val labelCol = "label"
    val evaluationMetric = MetricConstants.ClassificationMetricsName
    val predCol = SchemaConstants.SparkPredictionColumn
    val df = Seq.fill(numRows)(rand.nextDouble())
      .zip(Seq.fill(numRows)(rand.nextDouble()))
      .toDF(labelCol, predCol)
    val evaluatedData = new ComputeModelStatistics()
      .setLabelCol(labelCol)
      .setScoredLabelsCol(predCol)
      .setEvaluationMetric(evaluationMetric)
      .transform(df)
    val firstRow = evaluatedData.first()
    (2 to 4).foreach { index =>
      assert(firstRow.getDouble(index) === 0.0)
    }
  }

  test("Smoke test for evaluating a dataset") {
    val predictionColumn = SchemaConstants.SparkPredictionColumn
    val scoreModelName = SchemaConstants.ScoreModelPrefix + "_test model"
    val dataset = spark.createDataFrame(Seq(
      (0.0, 2, 0.50, 0.60, 0.0),
      (1.0, 3, 0.40, 0.50, 1.0),
      (2.0, 4, 0.78, 0.99, 2.0),
      (3.0, 5, 0.12, 0.34, 3.0),
      (0.0, 1, 0.50, 0.60, 0.0),
      (1.0, 3, 0.40, 0.50, 1.0),
      (2.0, 3, 0.78, 0.99, 2.0),
      (3.0, 4, 0.12, 0.34, 3.0),
      (0.0, 0, 0.50, 0.60, 0.0),
      (1.0, 2, 0.40, 0.50, 1.0),
      (2.0, 3, 0.78, 0.99, 2.0),
      (3.0, 4, 0.12, 0.34, 3.0)))
      .toDF(labelColumn, "col1", "col2", "col3", predictionColumn)

    val datasetWithLabel =
      SparkSchema.setLabelColumnName(dataset, scoreModelName, labelColumn, SchemaConstants.RegressionKind)
    val datasetWithScores =
      SparkSchema.updateColumnMetadata(datasetWithLabel, scoreModelName, predictionColumn,
                                      SchemaConstants.RegressionKind)

    val evaluatedSchema = new ComputeModelStatistics().transformSchema(datasetWithScores.schema)

    val evaluatedData = new ComputeModelStatistics().transform(datasetWithScores)
    val firstRow = evaluatedData.first()
    assert(firstRow.getDouble(0) === 0.0)
    assert(firstRow.getDouble(1) === 0.0)
    assert(firstRow.getDouble(2) === 1.0)
    assert(firstRow.getDouble(3) === 0.0)

    assert(evaluatedSchema == StructType(MetricConstants.RegressionColumns.map(StructField(_, DoubleType))))
  }

  // scalastyle:off null
  test("Evaluate a dataset with missing values") {
    val predictionColumn = SchemaConstants.SparkPredictionColumn
    val dataset = spark.createDataFrame(sc.parallelize(Seq(
      (0.0, 0.0),
      (0.0, null),
      (1.0, 1.0),
      (2.0, 2.0),
      (null, null),
      (0.0, 0.0),
      (null, 3.0))).map(values => Row(values._1, values._2)),
      StructType(Array(StructField(labelColumn, DoubleType, nullable = true),
        StructField(predictionColumn, DoubleType, nullable = true))))
      .toDF(labelColumn, predictionColumn)

    val scoreModelName = SchemaConstants.ScoreModelPrefix + "_test model"

    val datasetWithLabel =
      SparkSchema.setLabelColumnName(dataset, scoreModelName, labelColumn, SchemaConstants.RegressionKind)
    val datasetWithScores =
      SparkSchema.updateColumnMetadata(datasetWithLabel, scoreModelName, predictionColumn,
        SchemaConstants.RegressionKind)

    val evaluatedData = new ComputeModelStatistics().transform(datasetWithScores)
    val firstRow = evaluatedData.first()
    assert(firstRow.getDouble(0) === 0.0)
    assert(firstRow.getDouble(1) === 0.0)
    assert(firstRow.getDouble(2) === 1.0)
    assert(firstRow.getDouble(3) === 0.0)
  }
  // scalastyle:on null

  test("Verify compute model statistics does not get stuck in a loop in catalyst") {
    val name = "AutomobilePriceRaw.csv"
    val filePath = FileUtilities.join(
      BuildInfo.datasetDir, "MissingValuesRegression", "Train", name)
    val dataset =
      spark.read.option("header", "true").option("inferSchema", "true")
        .option("nullValue", "?")
        .option("treatEmptyValuesAsNulls", "true")
        .option("delimiter", ",")
        .csv(filePath.toString)
    val glr = new GeneralizedLinearRegression().setFamily("poisson").setLink("log")
    val tr = new TrainRegressor().setModel(glr).setLabelCol("price").setNumFeatures(256)
    val model = tr.fit(dataset)
    val prediction = model.transform(dataset)
    val evaluatedData = new ComputeModelStatistics().transform(prediction)
    assert(math.abs(evaluatedData.collect()(0).getDouble(2) - 0.9772518203539127) < .01)
  }

  test("Smoke test to train regressor, score and evaluate on a dataset using all three modules") {
    val dataset = spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (2, 4, 0.78, 0.99, 2),
      (3, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (2, 3, 0.78, 0.99, 2),
      (3, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (2, 3, 0.78, 0.99, 2),
      (3, 4, 0.12, 0.34, 3)
    )).toDF(labelColumn, "col1", "col2", "col3", "col4")

    val otherLabelColumn = "someOtherColumn"

    val datasetWithAddedColumn = dataset.withColumn(otherLabelColumn, org.apache.spark.sql.functions.lit(0.0))

    val linearRegressor = createLinearRegressor(otherLabelColumn)
    val scoredDataset =
      TrainRegressorTestUtilities.trainScoreDataset(otherLabelColumn, datasetWithAddedColumn, linearRegressor)

    val evaluatedData = new ComputeModelStatistics().transform(scoredDataset)
    val firstRow = evaluatedData.first()
    assert(firstRow.getDouble(0) === 0.0)
    assert(firstRow.getDouble(1) === 0.0)
    assert(firstRow.getDouble(2).isNaN)
    assert(firstRow.getDouble(3) === 0.0)
  }

  lazy val logisticRegressor: TrainClassifier = createLR.setLabelCol(labelColumn)
  lazy val scoredDataset: DataFrame = TrainClassifierTestUtilities.trainScoreDataset(
    labelColumn, dataset, logisticRegressor)
  test("Smoke test to train classifier, score and evaluate on a dataset using all three modules") {
    val _ = new ComputeModelStatistics().transform(scoredDataset)

    val evaluatedSchema = new ComputeModelStatistics().transformSchema(scoredDataset.schema)
    assert(evaluatedSchema ==
      StructType(MetricConstants.BinaryClassificationColumns.map(StructField(_, DoubleType))))
  }

  test("Verify computing statistics on generic spark ML estimators is supported") {
    val scoredLabelsCol = "LogRegScoredLabelsCol"
    val scoresCol = "LogRegScoresCol"
    val featuresCol = "features"
    val logisticRegression = new LogisticRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(10)
      .setLabelCol(labelColumn)
      .setPredictionCol(scoredLabelsCol)
      .setRawPredictionCol(scoresCol)
      .setFeaturesCol(featuresCol)
    val assembler = new FastVectorAssembler()
      .setInputCols(Array("col1", "col2", "col3", "col4"))
      .setOutputCol(featuresCol)
    val assembledDataset = assembler.transform(dataset)
    val model = logisticRegression.fit(assembledDataset)
    val scoredData = model.transform(assembledDataset)
    val cms = new ComputeModelStatistics()
      .setLabelCol(labelColumn)
      .setScoredLabelsCol(scoredLabelsCol)
      .setScoresCol(scoresCol)
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
    val evaluatedData = cms.transform(scoredData)
    val firstRow = evaluatedData.select(col("accuracy"), col("precision"), col("recall"), col("AUC")).first()
    assert(firstRow.getDouble(0) === 1.0)
    assert(firstRow.getDouble(1) === 1.0)
    assert(firstRow.getDouble(2) === 1.0)
    assert(firstRow.getDouble(3) === 1.0)
  }

  test("Verify results of multiclass metrics") {
    val labelColumn = "label"
    val predictionColumn = SchemaConstants.SparkPredictionColumn
    val labelsAndPrediction = spark.createDataFrame(
      Seq(
        (0.0, 0.0),
        (0.0, 0.0),
        (0.0, 1.0),
        (0.0, 2.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (1.0, 1.0),
        (1.0, 1.0),
        (2.0, 2.0))).toDF(labelColumn, predictionColumn)

    val scoreModelName = SchemaConstants.ScoreModelPrefix + "_test model"

    val datasetWithLabel =
      SparkSchema.setLabelColumnName(labelsAndPrediction, scoreModelName, labelColumn,
        SchemaConstants.ClassificationKind)
    val datasetWithScoredLabels =
      SparkSchema.updateColumnMetadata(datasetWithLabel, scoreModelName, predictionColumn,
        SchemaConstants.ClassificationKind)

    val evaluatedData = new ComputeModelStatistics().transform(datasetWithScoredLabels)

    val tp0 = 2.0
    val tp1 = 3.0
    val tp2 = 1.0
    val tn0 = 4.0
    val tn1 = 4.0
    val tn2 = 7.0
    val numLabels = 3.0
    val total = labelsAndPrediction.count()

    val precision0 = 2.0 / (2 + 1)
    val precision1 = 3.0 / (3 + 1)
    val precision2 = 1.0 / (1 + 1)
    val recall0 = 2.0 / (2 + 2)
    val recall1 = 3.0 / (3 + 1)
    val recall2 = 1.0 / (1 + 0)

    val overallAccuracy = (tp0 + tp1 + tp2) / total
    val evalRow = evaluatedData.first()
    assert(evalRow.getAs[Double](MetricConstants.AccuracyColumnName) === overallAccuracy)
    assert(evalRow.getAs[Double](MetricConstants.PrecisionColumnName) === overallAccuracy)
    assert(evalRow.getAs[Double](MetricConstants.RecallColumnName) === overallAccuracy)
    val avgAccuracy = ((tp0 + tn0) / total + (tp1 + tn1) / total + (tp2 + tn2) / total) / numLabels
    val macroPrecision = (precision0 + precision1 + precision2) / numLabels
    val macroRecall = (recall0 + recall1 + recall2) / numLabels
    assert(evalRow.getAs[Double](MetricConstants.AverageAccuracy) === avgAccuracy)
    assert(evalRow.getAs[Double](MetricConstants.MacroAveragedPrecision) === macroPrecision)
    assert(evalRow.getAs[Double](MetricConstants.MacroAveragedRecall) === macroRecall)
  }

  test("validate AUC from compute model statistic and binary classification evaluator gives the same result") {
    val fileLocation = DatasetUtils.binaryTrainFile("transfusion.csv").toString
    val label = "Donated"
    val dataset: DataFrame =
      spark.read.format("com.databricks.spark.csv")
        .option("header", "true").option("inferSchema", "true")
        .option("treatEmptyValuesAsNulls", "false")
        .option("delimiter", ",")
        .load(fileLocation)

    val split = dataset.randomSplit(Array(0.75,0.25))
    val train = split(0)
    val test = split(1)

    val trainClassifier = new TrainClassifier()
    val model = trainClassifier.setModel(new LogisticRegression())
      .set(trainClassifier.labelCol, label)
      .set(trainClassifier.numFeatures, 1 << 18)
      .fit(train)
    val scored = model.transform(test)
    val eval = new ComputeModelStatistics().transform(scored)
    val cmsAUC = eval.first().getAs[Double]("AUC")

    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .setLabelCol(label)
      .setRawPredictionCol(SchemaConstants.SparkRawPredictionColumn)

    val levels = CategoricalUtilities.getLevels(scored.schema, label)
    val levelsToIndexMap: Map[Any, Double] = levels.get.zipWithIndex.map(t => t._1 -> t._2.toDouble).toMap

    // Calculate confusion matrix and output it as DataFrame
    val predictionAndLabels = spark
      .createDataFrame(scored.select(col(SchemaConstants.SparkRawPredictionColumn), col(label)).rdd.map {
      case Row(prediction: Vector, label) => (prediction(1), levelsToIndexMap(label))
    }).toDF(SchemaConstants.SparkRawPredictionColumn, label)

    val auc = binaryEvaluator.evaluate(predictionAndLabels)
    assert(auc === cmsAUC)
  }

  override def testObjects(): Seq[TestObject[ComputeModelStatistics]] = Seq(new TestObject(
    new ComputeModelStatistics(), scoredDataset))

  override def reader: MLReadable[_] = ComputeModelStatistics
}
