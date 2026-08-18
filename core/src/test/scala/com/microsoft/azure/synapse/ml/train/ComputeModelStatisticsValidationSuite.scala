// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.metrics.MetricConstants
import com.microsoft.azure.synapse.ml.core.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import java.io.File
import java.util.UUID

class ComputeModelStatisticsValidationSuite extends TestBase {

  test("Explicit labels override scoring metadata and incomplete metadata remains actionable") {
    val data = spark.createDataFrame(Seq(
      (1.0, 0.0, 0.0),
      (0.0, 1.0, 1.0))).toDF("metadataLabel", "explicitLabel", "prediction")
    val modelName = SchemaConstants.ScoreModelPrefix + "_override"
    val withLabel = SparkSchema.setLabelColumnName(
      data, modelName, "metadataLabel", SchemaConstants.ClassificationKind)
    val scored = SparkSchema.updateColumnMetadata(
      withLabel, modelName, "prediction", SchemaConstants.ClassificationKind)
    val evaluator = new ComputeModelStatistics()
      .setLabelCol("explicitLabel")
      .setScoredLabelsCol("prediction")
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)

    assert(evaluator.transform(scored).first().getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)

    val predictionOnlyMetadata = SparkSchema.updateColumnMetadata(
      data, modelName, "prediction", SchemaConstants.ClassificationKind)
    assert(evaluator.transform(predictionOnlyMetadata).first()
      .getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)

    val error = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(predictionOnlyMetadata.schema)
    }
    assert(error.getMessage.contains("requires the label column"))
    assert(error.getMessage.contains("setLabelCol"))
  }

  test("Column validation rejects ambiguous and incompatible input types") {
    val numeric = spark.createDataFrame(Seq((0.0, 0.0), (1.0, 1.0))).toDF("label", "prediction")

    val missingLabel = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("missingLabel")
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transform(numeric)
    }
    assert(missingLabel.getMessage.contains("labelCol 'missingLabel' does not exist"))
    assert(missingLabel.getMessage.contains("setLabelCol"))

    val missingScoredLabels = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoresCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(numeric.schema)
    }
    assert(missingScoredLabels.getMessage.contains("setScoredLabelsCol"))
    assert(missingScoredLabels.getMessage.contains("labelCol and scoredLabelsCol"))

    val duplicateLabel = numeric.select(col("label"), col("prediction").as("label"), col("prediction"))
    val ambiguous = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(duplicateLabel.schema)
    }
    assert(ambiguous.getMessage.contains("labelCol 'label' is ambiguous"))

    val stringLabels = spark.createDataFrame(Seq(("zero", 0.0), ("one", 1.0))).toDF("label", "prediction")
    val invalidLabel = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(stringLabels.schema)
    }
    assert(invalidLabel.getMessage.contains("labelCol 'label' has type string"))

    val vectorPrediction = spark.createDataFrame(Seq(
      (0.0, Vectors.dense(1.0, 0.0)),
      (1.0, Vectors.dense(0.0, 1.0)))).toDF("label", "prediction")
    val invalidPrediction = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(vectorPrediction.schema)
    }
    assert(invalidPrediction.getMessage.contains("scoredLabelsCol 'prediction' has type"))
    assert(invalidPrediction.getMessage.contains("expected a numeric scalar"))

    val invalidRegression = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoresCol("prediction")
        .setEvaluationMetric(MetricConstants.RegressionMetricsName)
        .transformSchema(vectorPrediction.schema)
    }
    assert(invalidRegression.getMessage.contains("scoresCol 'prediction' has type"))
    assert(invalidRegression.getMessage.contains("expected a numeric scalar"))

    val stringScores = spark.createDataFrame(Seq(
      (0.0, 0.0, "low"),
      (1.0, 1.0, "high"))).toDF("label", "prediction", "scores")
    val invalidClassificationScores = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setScoresCol("scores")
        .setEvaluationMetric(MetricConstants.AucSparkMetric)
        .transformSchema(stringScores.schema)
    }
    assert(invalidClassificationScores.getMessage.contains("scoresCol 'scores' has type string"))
    assert(invalidClassificationScores.getMessage.contains("expected a numeric scalar or Spark ML vector"))

    val missingAreaUnderPRScores = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setScoresCol("missingScores")
        .setEvaluationMetric(MetricConstants.AreaUnderPRMetric)
        .transformSchema(numeric.schema)
    }
    assert(missingAreaUnderPRScores.getMessage.contains("scoresCol 'missingScores' does not exist"))
  }

  test("Empty configured column names fail with setter guidance") {
    val data = spark.createDataFrame(Seq((0.0, 0.0))).toDF("label", "prediction")

    val emptyLabel = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("")
        .setScoredLabelsCol("prediction")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(data.schema)
    }
    assert(emptyLabel.getMessage.contains("requires the label column"))
    assert(emptyLabel.getMessage.contains("setLabelCol"))

    val emptyScoredLabels = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("")
        .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
        .transformSchema(data.schema)
    }
    assert(emptyScoredLabels.getMessage.contains("classification prediction column <unresolved>"))
    assert(emptyScoredLabels.getMessage.contains("setScoredLabelsCol"))

    val emptyScores = intercept[IllegalArgumentException] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoresCol("")
        .setEvaluationMetric(MetricConstants.RegressionMetricsName)
        .transformSchema(data.schema)
    }
    assert(emptyScores.getMessage.contains("regression prediction/score column <unresolved>"))
    assert(emptyScores.getMessage.contains("setScoresCol"))
  }

  test("Column resolution follows Spark case sensitivity") {
    val data = spark.createDataFrame(Seq(
      ("no", 0.0),
      ("yes", 1.0))).toDF("Label", "Prediction")
    val categorical = CategoricalUtilities.setLevels(data, "Label", Array("no", "yes"))
    val evaluator = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)

    assert(evaluator.transform(categorical).first()
      .getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)

    val originalCaseSensitivity = spark.conf.get("spark.sql.caseSensitive")
    try {
      spark.conf.set("spark.sql.caseSensitive", "true")
      val error = intercept[IllegalArgumentException] {
        evaluator.transformSchema(categorical.schema)
      }
      assert(error.getMessage.contains("labelCol 'label' does not exist"))
      assert(error.getMessage.contains("[Label, Prediction]"))
    } finally {
      spark.conf.set("spark.sql.caseSensitive", originalCaseSensitivity)
    }
  }

  test("Scalar and vector classification scores agree and null rows are ignored") {
    val scalarData = spark.createDataFrame(Seq(
      ("no", 0, 0.1),
      ("yes", 1, 0.9))).toDF("label", "prediction", "scores")
    val categorical = CategoricalUtilities.setLevels(scalarData, "label", Array("no", "yes"))
    val predictionFallback = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
      .transform(categorical.select("label", "prediction"))
      .first()
    assert(predictionFallback.getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)
    assert(predictionFallback.getAs[Double](MetricConstants.AucColumnName) === 1.0)

    val scalarResult = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setScoresCol("scores")
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
      .transform(categorical)
      .first()
    assert(scalarResult.getAs[Double](MetricConstants.AucColumnName) === 1.0)
    assert(scalarResult.getAs[Double](MetricConstants.AreaUnderPRColumnName) === 1.0)

    val vectorSchema = StructType(Array(
      StructField("label", DoubleType, nullable = true),
      StructField("prediction", DoubleType, nullable = true),
      StructField("scores", SQLDataTypes.VectorType, nullable = true)))
    // scalastyle:off null
    val vectorData = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(0.0, 0.0, Vectors.dense(0.9, 0.1)),
      Row(1.0, 1.0, Vectors.sparse(2, Array(1), Array(0.9))),
      Row(null, null, null))), vectorSchema)
    // scalastyle:on null
    val vectorResult = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol("prediction")
      .setScoresCol("scores")
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
      .transform(vectorData)
      .first()
    assert(vectorResult.getAs[Double](MetricConstants.AucColumnName) === 1.0)
    assert(vectorResult.getAs[Double](MetricConstants.AreaUnderPRColumnName) === 1.0)

    val invalidVectorData = spark.createDataFrame(Seq(
      (0.0, 0.0, Vectors.dense(0.1)),
      (1.0, 1.0, Vectors.dense(0.9)))).toDF("label", "prediction", "scores")
    val invalidVector = intercept[Exception] {
      new ComputeModelStatistics()
        .setLabelCol("label")
        .setScoredLabelsCol("prediction")
        .setScoresCol("scores")
        .setEvaluationMetric(MetricConstants.AucSparkMetric)
        .transform(invalidVectorData)
        .first()
    }
    val messages = Iterator.iterate[Throwable](invalidVector)(_.getCause)
      .takeWhile(_ != null)
      .map(_.getMessage)
      .filter(_ != null)
      .mkString(" ")
    assert(messages.contains("vectors must contain at least two values"))
  }

  test("Regression schemas, copies, and persisted pipelines preserve configured behavior") {
    val data = spark.createDataFrame(Seq((0, 0.0f), (2, 1.0f))).toDF("label", "prediction")
    val evaluator = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoresCol("prediction")
      .setEvaluationMetric(MetricConstants.MseSparkMetric)
    val result = evaluator.transform(data)
    val transformedSchema = evaluator.transformSchema(data.schema)
    assert(result.schema.fieldNames === transformedSchema.fieldNames)
    assert(result.schema.fields.map(_.dataType) === transformedSchema.fields.map(_.dataType))
    assert(result.columns.toSeq === Seq(MetricConstants.MseColumnName))
    assert(result.first().getDouble(0) === 0.5)

    val copied = evaluator.copy(new ParamMap()
      .put(evaluator.evaluationMetric, MetricConstants.RmseSparkMetric))
      .asInstanceOf[ComputeModelStatistics]
    assert(copied.getLabelCol === "label")
    assert(copied.getScoresCol === "prediction")
    assert(copied.getEvaluationMetric === MetricConstants.RmseSparkMetric)

    val pipelineModel = new Pipeline().setStages(Array(evaluator)).fit(data)
    val path = new File(BuildInfo.baseDirectory,
      s"target/compute-model-statistics-pipeline-${UUID.randomUUID()}").getAbsoluteFile
    try {
      pipelineModel.write.overwrite().save(path.toString)
      val loaded = PipelineModel.load(path.toString)
      val loadedResult = loaded.transform(data)
      assert(loadedResult.schema === result.schema)
      assert(loadedResult.first().getDouble(0) === 0.5)
    } finally {
      if (path.exists()) FileUtils.forceDelete(path)
    }
  }

  test("Invalid evaluation metrics report the valid values") {
    val data = spark.createDataFrame(Seq((0.0, 0.0), (1.0, 1.0))).toDF("label", "prediction")
    val modelName = SchemaConstants.ScoreModelPrefix + "_invalid_metric"
    val withLabel = SparkSchema.setLabelColumnName(
      data, modelName, "label", SchemaConstants.RegressionKind)
    val scored = SparkSchema.updateColumnMetadata(
      withLabel, modelName, "prediction", SchemaConstants.RegressionKind)
    val evaluator = new ComputeModelStatistics()
      .setEvaluationMetric("unsupported")

    val schemaError = intercept[IllegalArgumentException] {
      evaluator.transformSchema(scored.schema)
    }
    assert(schemaError.getMessage.contains("evaluationMetric 'unsupported'"))
    assert(schemaError.getMessage.contains("not valid for regression data"))
    assert(schemaError.getMessage.contains("Valid values: all, mae, mse, r2, regression, rmse"))

    val runtimeError = intercept[IllegalArgumentException] {
      evaluator.transform(scored)
    }
    assert(runtimeError.getMessage === schemaError.getMessage)
  }

  test("Top-level columns with SQL punctuation remain addressable") {
    val data = spark.createDataFrame(Seq((0.0, 0.0), (1.0, 1.0)))
      .toDF("label.with.dot", "prediction`quoted")
    val result = new ComputeModelStatistics()
      .setLabelCol("label.with.dot")
      .setScoredLabelsCol("prediction`quoted")
      .setEvaluationMetric(MetricConstants.AccuracySparkMetric)
      .transform(data)

    assert(result.first().getAs[Double](MetricConstants.AccuracyColumnName) === 1.0)
  }
}
