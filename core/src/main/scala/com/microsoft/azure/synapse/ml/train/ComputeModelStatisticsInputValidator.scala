// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.core.metrics.{MetricConstants, MetricUtils}
import com.microsoft.azure.synapse.ml.core.schema.{CategoricalUtilities, SchemaConstants}
import org.apache.spark.ml.linalg.SQLDataTypes
import org.apache.spark.sql.types.{NumericType, StructField, StructType}

private[train] object ComputeModelStatisticsInputValidator {

  final case class ValidatedInputColumns(labelColumnName: String,
                                         scoredLabelsColumnName: Option[String],
                                         scoresColumnName: Option[String])

  private val ClassificationRequirements =
    "Classification metrics require labelCol and scoredLabelsCol; " +
      "scoresCol supplies raw scores when available."
  private val RegressionRequirements = "Regression metrics require labelCol and scoresCol."

  def validate(schema: StructType,
               modelName: String,
               labelColumnName: String,
               scoreValueKind: String,
               scoredLabelsCol: Option[String],
               scoresCol: Option[String],
               evaluationMetric: String,
               caseSensitive: Boolean): ValidatedInputColumns = {
    val labelField = validateColumn(
      schema,
      labelColumnName,
      "labelCol",
      "setLabelCol",
      "label",
      if (scoreValueKind == SchemaConstants.ClassificationKind) ClassificationRequirements
      else RegressionRequirements,
      caseSensitive)
    val hasCategoricalLevels =
      scoreValueKind == SchemaConstants.ClassificationKind &&
        CategoricalUtilities.getLevels(schema, labelField.name).isDefined
    if (!hasCategoricalLevels) {
      validateType(labelField, "labelCol", "a numeric scalar")
    }

    scoreValueKind match {
      case SchemaConstants.ClassificationKind =>
        validateClassificationColumns(
          schema,
          modelName,
          labelField,
          scoredLabelsCol,
          scoresCol,
          evaluationMetric,
          caseSensitive)
      case SchemaConstants.RegressionKind =>
        validateRegressionColumns(schema, modelName, labelField, scoresCol, caseSensitive)
      case _ =>
        ValidatedInputColumns(labelField.name, None, None)
    }
  }

  private def validateClassificationColumns(
      schema: StructType,
      modelName: String,
      labelField: StructField,
      scoredLabelsCol: Option[String],
      scoresCol: Option[String],
      evaluationMetric: String,
      caseSensitive: Boolean): ValidatedInputColumns = {
    val scoredLabelsColumnName = scoredLabelsCol.orElse(
      MetricUtils.getScoreColumnName(
        schema,
        modelName,
        SchemaConstants.SparkPredictionColumn,
        SchemaConstants.ClassificationKind)).getOrElse("")
    val scoredLabelsField = validateColumn(
      schema,
      scoredLabelsColumnName,
      "scoredLabelsCol",
      "setScoredLabelsCol",
      "classification prediction",
      ClassificationRequirements,
      caseSensitive)
    validateType(scoredLabelsField, "scoredLabelsCol", "a numeric scalar")

    val scoresColumnName =
      if (!classificationUsesScores(evaluationMetric)) {
        None
      } else {
        scoresCol.map { columnName =>
          validateClassificationScores(schema, columnName, caseSensitive)
        }.orElse {
          MetricUtils.getScoreColumnName(
            schema,
            modelName,
            SchemaConstants.SparkRawPredictionColumn,
            SchemaConstants.ClassificationKind).map { columnName =>
            validateClassificationScores(schema, columnName, caseSensitive)
          }
        }
      }
    ValidatedInputColumns(labelField.name, Some(scoredLabelsField.name), scoresColumnName)
  }

  private def validateClassificationScores(
      schema: StructType,
      columnName: String,
      caseSensitive: Boolean): String = {
    val scoresField = validateColumn(
      schema,
      columnName,
      "scoresCol",
      "setScoresCol",
      "classification score",
      ClassificationRequirements,
      caseSensitive)
    if (scoresField.dataType != SQLDataTypes.VectorType) {
      validateType(scoresField, "scoresCol", "a numeric scalar or Spark ML vector")
    }
    scoresField.name
  }

  private def validateRegressionColumns(
      schema: StructType,
      modelName: String,
      labelField: StructField,
      scoresCol: Option[String],
      caseSensitive: Boolean): ValidatedInputColumns = {
    val scoresColumnName = scoresCol.orElse(
      MetricUtils.getScoreColumnName(
        schema,
        modelName,
        SchemaConstants.SparkPredictionColumn,
        SchemaConstants.RegressionKind)).getOrElse("")
    val scoresField = validateColumn(
      schema,
      scoresColumnName,
      "scoresCol",
      "setScoresCol",
      "regression prediction/score",
      RegressionRequirements,
      caseSensitive)
    validateType(scoresField, "scoresCol", "a numeric scalar")
    ValidatedInputColumns(labelField.name, None, Some(scoresField.name))
  }

  private def classificationUsesScores(evaluationMetric: String): Boolean = {
    evaluationMetric == MetricConstants.AllSparkMetrics ||
      evaluationMetric == MetricConstants.ClassificationMetricsName ||
      evaluationMetric == MetricConstants.AucSparkMetric ||
      evaluationMetric == MetricConstants.AreaUnderROCMetric ||
      evaluationMetric == MetricConstants.AreaUnderPRMetric
  }

  private def validateColumn(schema: StructType,
                             columnName: String,
                             paramName: String,
                             setterName: String,
                             role: String,
                             requirements: String,
                             caseSensitive: Boolean): StructField = {
    val availableColumns = schema.fieldNames.sorted.mkString("[", ", ", "]")
    if (Option(columnName).forall(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"ComputeModelStatistics requires the $role column. " +
          s"Unable to resolve $role column <unresolved>. " +
          s"Call $setterName(...) with an existing dataset column when scoring metadata is unavailable. " +
          s"$requirements Available columns: $availableColumns")
    }

    val matchingFields =
      if (caseSensitive) schema.fields.filter(_.name == columnName)
      else schema.fields.filter(_.name.equalsIgnoreCase(columnName))
    if (matchingFields.isEmpty) {
      throw new IllegalArgumentException(
        s"ComputeModelStatistics $paramName '$columnName' does not exist in the dataset; " +
          s"unable to resolve $role column '$columnName'. " +
          s"Call $setterName(...) with an existing column. " +
          s"$requirements Available columns: $availableColumns")
    } else if (matchingFields.length > 1) {
      throw new IllegalArgumentException(
        s"ComputeModelStatistics $paramName '$columnName' is ambiguous because the dataset contains " +
          s"${matchingFields.length} columns with that name. Rename or select one column before evaluation.")
    }
    matchingFields.head
  }

  private def validateType(field: StructField, paramName: String, expectedType: String): Unit = {
    field.dataType match {
      case _: NumericType => ()
      case _ =>
        throw new IllegalArgumentException(
          s"ComputeModelStatistics $paramName '${field.name}' has type ${field.dataType.catalogString}; " +
            s"expected $expectedType.")
    }
  }
}
