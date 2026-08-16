// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.metrics

import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants.MMLTag
import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants
import org.apache.spark.sql.types.injections.MetadataUtilities
import org.apache.spark.sql.types.{Metadata, StructType}

import scala.util.control.NonFatal

/** Utilities used by modules for metrics. */
object MetricUtils {

  private case class ScoredModelCandidate(modelName: String,
                                          labelColumnName: String,
                                          scoreValueKind: String,
                                          predictionColumnName: String,
                                          otherColumns: Seq[ScoreColumnMetadata]) {
    def schemaInfo: (String, String, String) = (modelName, labelColumnName, scoreValueKind)

    def description: String =
      s"$modelName (label=$labelColumnName, kind=$scoreValueKind, prediction=$predictionColumnName)"

    def signature(includeOtherColumns: Boolean): (String, String, String, Seq[(String, String, String)]) =
      (labelColumnName, scoreValueKind, predictionColumnName,
        if (includeOtherColumns) {
          otherColumns.map(column => (column.columnKind, column.columnName, column.scoreValueKind))
        } else {
          Seq.empty
        })
  }

  private case class ScoreColumnMetadata(modelName: String,
                                         columnName: String,
                                         columnKind: String,
                                         scoreValueKind: String)

  private case class ScoredModelMetadata(modelName: String, columns: Seq[ScoreColumnMetadata]) {
    private val labelColumns = columns.filter(_.columnKind == SchemaConstants.TrueLabelsColumn)
    private val predictionColumns = columns.filter(_.columnKind == SchemaConstants.SparkPredictionColumn)
    private val otherColumns = columns
      .filter(column =>
        column.columnKind == SchemaConstants.SparkRawPredictionColumn ||
          column.columnKind == SchemaConstants.SparkProbabilityColumn)
      .sortBy(column => (column.columnKind, column.columnName, column.scoreValueKind))

    def candidates: Seq[ScoredModelCandidate] = {
      for {
        label <- labelColumns
        prediction <- predictionColumns
        if label.scoreValueKind == prediction.scoreValueKind
        if ValidScoreValueKinds.contains(label.scoreValueKind)
      } yield ScoredModelCandidate(
        modelName,
        label.columnName,
        label.scoreValueKind,
        prediction.columnName,
        otherColumns)
    }

    def conflictDescription(labelCol: Option[String],
                            requestedKind: Option[String]): Option[String] = {
      val couldMatchLabel = labelCol.forall(label => labelColumns.exists(_.columnName == label))
      val availableKinds = (labelColumns ++ predictionColumns).map(_.scoreValueKind).toSet
      val couldMatchKind = requestedKind.forall(availableKinds.contains)
      if (labelColumns.nonEmpty && predictionColumns.nonEmpty &&
          candidates.isEmpty && couldMatchLabel && couldMatchKind) {
        val labels = describeColumns(labelColumns)
        val predictions = describeColumns(predictionColumns)
        Some(s"$modelName has incompatible label metadata $labels and prediction metadata $predictions")
      } else {
        None
      }
    }

    private def describeColumns(scoreColumns: Seq[ScoreColumnMetadata]): String =
      scoreColumns
        .map(column => s"${column.columnName}:${column.scoreValueKind}")
        .distinct
        .sorted
        .mkString("[", ", ", "]")
  }

  private val ValidScoreValueKinds = Set(SchemaConstants.ClassificationKind, SchemaConstants.RegressionKind)

  def isClassificationMetric(metric: String): Boolean = {
    if (MetricConstants.RegressionMetrics.contains(metric)) false
    else if (MetricConstants.ClassificationMetrics.contains(metric)) true
    else throw new Exception("Invalid metric specified")
  }

  def getSchemaInfo(schema: StructType, labelCol: Option[String],
                    evaluationMetric: String): (String, String, String) = {
    getSchemaInfo(schema, labelCol, evaluationMetric, caseSensitive = true)
  }

  private[ml] def getSchemaInfo(schema: StructType,
                                labelCol: Option[String],
                                evaluationMetric: String,
                                caseSensitive: Boolean): (String, String, String) = {
    val resolvedLabelCol = resolveLabelColumn(schema, labelCol, caseSensitive)
    val requestedKind = getRequestedScoreValueKind(evaluationMetric)
    tryGetSchemaInfo(
      schema,
      resolvedLabelCol,
      requestedKind,
      requiresAuxiliaryMetadata(evaluationMetric, requestedKind)).map(_.schemaInfo).getOrElse {
      (resolvedLabelCol, requestedKind) match {
        case (Some(labelColumnName), Some(scoreValueKind)) =>
          ("custom model", labelColumnName, scoreValueKind)
        case _ =>
          val missingSettings = Seq(
            if (labelCol.isEmpty) Some("labelCol") else None,
            if (requestedKind.isEmpty) Some("evaluationMetric") else None).flatten
          val availableColumns = schema.fieldNames.sorted.mkString("[", ", ", "]")
          val metricHint =
            if (requestedKind.isEmpty) s" (evaluationMetric must not be '${MetricConstants.AllSparkMetrics}')"
            else ""
          throw new IllegalArgumentException(
            "Unable to determine a complete scored model from schema metadata. " +
              s"Set ${missingSettings.mkString(" and ")}$metricHint, " +
              "or score the dataset so one model has both label and prediction metadata. " +
              s"Available columns: $availableColumns")
      }
    }
  }

  private[ml] def getScoreColumnName(schema: StructType,
                                     modelName: String,
                                     columnKind: String,
                                     scoreValueKind: String): Option[String] = {
    val matchingColumns = getScoredModelMetadata(schema)
      .find(_.modelName == modelName)
      .toSeq
      .flatMap(_.columns)
      .filter(_.columnKind == columnKind)
    val descriptions = matchingColumns
      .map(column => s"${column.columnName}:${column.scoreValueKind}")
      .distinct
      .sorted
    matchingColumns.map(_.scoreValueKind).distinct match {
      case Seq(kind) if kind == scoreValueKind && descriptions.size == 1 =>
        Some(matchingColumns.head.columnName)
      case Seq() => None
      case _ =>
        throw new IllegalArgumentException(
          s"Conflicting scored-model metadata. $modelName has $columnKind columns " +
            descriptions.mkString("[", ", ", "]."))
    }
  }

  private def getRequestedScoreValueKind(evaluationMetric: String): Option[String] = {
    if (evaluationMetric == MetricConstants.AllSparkMetrics) None
    else if (isClassificationMetric(evaluationMetric)) Some(SchemaConstants.ClassificationKind)
    else Some(SchemaConstants.RegressionKind)
  }

  private def requiresAuxiliaryMetadata(evaluationMetric: String,
                                        requestedKind: Option[String]): Boolean = {
    requestedKind match {
      case Some(SchemaConstants.RegressionKind) => false
      case Some(SchemaConstants.ClassificationKind) =>
        evaluationMetric != MetricConstants.AccuracySparkMetric &&
          evaluationMetric != MetricConstants.PrecisionSparkMetric &&
          evaluationMetric != MetricConstants.RecallSparkMetric
      case _ => true
    }
  }

  private def tryGetSchemaInfo(schema: StructType,
                               labelCol: Option[String],
                               requestedKind: Option[String],
                               includeOtherColumns: Boolean): Option[ScoredModelCandidate] = {
    val scoredModels = getScoredModelMetadata(schema)
    val conflicts = scoredModels
      .flatMap(_.conflictDescription(labelCol, requestedKind))
      .sorted
    if (conflicts.nonEmpty) {
      throw new IllegalArgumentException(
        "Conflicting scored-model metadata. " + conflicts.mkString("[", ", ", "]."))
    }

    val matchingCandidates = scoredModels
      .flatMap(_.candidates)
      .filter(candidate => labelCol.forall(_ == candidate.labelColumnName))
      .filter(candidate => requestedKind.forall(_ == candidate.scoreValueKind))
    val distinctCandidates = matchingCandidates
      .groupBy(_.signature(includeOtherColumns))
      .values
      .map(_.minBy(_.modelName))
      .toSeq
      .sortBy(candidate =>
        (candidate.modelName, candidate.labelColumnName,
          candidate.scoreValueKind, candidate.predictionColumnName))

    distinctCandidates match {
      case Seq(candidate) => Some(candidate)
      case candidates if candidates.nonEmpty =>
        throw new IllegalArgumentException(
          "Ambiguous scored-model metadata. Multiple complete candidates match: " +
            candidates.map(_.description).mkString("[", ", ", "]. ") +
            "Set labelCol and evaluationMetric to narrow candidates. If metadata still overlaps, " +
            "set scoredLabelsCol/scoresCol explicitly or remove stale score metadata.")
      case _ => None
    }
  }

  private[ml] def resolveLabelColumn(schema: StructType,
                                     labelCol: Option[String],
                                     caseSensitive: Boolean): Option[String] = {
    labelCol.map { requestedLabel =>
      if (caseSensitive) {
        requestedLabel
      } else {
        schema.fieldNames.filter(_.equalsIgnoreCase(requestedLabel)) match {
          case Array(resolvedLabel) => resolvedLabel
          case _ => requestedLabel
        }
      }
    }
  }

  private def getScoredModelMetadata(schema: StructType): Seq[ScoredModelMetadata] = {
    schema.fields
      .flatMap(field => getScoreColumnMetadata(field.name, field.metadata))
      .groupBy(_.modelName)
      .toSeq
      .sortBy(_._1)
      .map { case (modelName, columns) =>
        ScoredModelMetadata(
          modelName,
          columns.sortBy(column => (column.columnKind, column.columnName, column.scoreValueKind)))
      }
  }

  private def getScoreColumnMetadata(columnName: String,
                                     colMetadata: Metadata): Seq[ScoreColumnMetadata] = {
    getMetadata(colMetadata, MMLTag).toSeq.flatMap { mlTagMetadata =>
      MetadataUtilities.getMetadataKeys(mlTagMetadata)
        .filter(_.startsWith(SchemaConstants.ScoreModelPrefix))
        .toSeq
        .sorted
        .flatMap { modelName =>
          for {
            modelMetadata <- getMetadata(mlTagMetadata, modelName)
            columnKind <- getString(modelMetadata, SchemaConstants.ScoreColumnKind)
            scoreValueKind <- getString(modelMetadata, SchemaConstants.ScoreValueKind)
          } yield ScoreColumnMetadata(modelName, columnName, columnKind, scoreValueKind)
        }
    }
  }

  private def getMetadata(metadata: Metadata, key: String): Option[Metadata] =
    try {
      if (metadata.contains(key)) Some(metadata.getMetadata(key)) else None
    } catch {
      case NonFatal(_) => None
    }

  private def getString(metadata: Metadata, key: String): Option[String] =
    try {
      if (metadata.contains(key)) Option(metadata.getString(key)) else None
    } catch {
      case NonFatal(_) => None
    }

}
