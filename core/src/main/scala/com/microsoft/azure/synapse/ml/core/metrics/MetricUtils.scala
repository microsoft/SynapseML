// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.metrics

import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants.MMLTag
import com.microsoft.azure.synapse.ml.core.schema.{SchemaConstants, SparkSchema}
import org.apache.spark.sql.types.injections.MetadataUtilities
import org.apache.spark.sql.types.{Metadata, StructType}

/** Utilities used by modules for metrics. */
object MetricUtils {

  private case class ScoredModelCandidate(modelName: String,
                                          labelColumnName: String,
                                          scoreValueKind: String,
                                          predictionColumnName: String) {
    def schemaInfo: (String, String, String) = (modelName, labelColumnName, scoreValueKind)

    def description: String =
      s"$modelName (label=$labelColumnName, kind=$scoreValueKind, prediction=$predictionColumnName)"
  }

  private val ValidScoreValueKinds = Set(SchemaConstants.ClassificationKind, SchemaConstants.RegressionKind)

  def isClassificationMetric(metric: String): Boolean = {
    if (MetricConstants.RegressionMetrics.contains(metric)) false
    else if (MetricConstants.ClassificationMetrics.contains(metric)) true
    else throw new Exception("Invalid metric specified")
  }

  def getSchemaInfo(schema: StructType, labelCol: Option[String],
                    evaluationMetric: String): (String, String, String) = {
    val requestedKind = getRequestedScoreValueKind(evaluationMetric)
    tryGetSchemaInfo(schema, labelCol, requestedKind).map(_.schemaInfo).getOrElse {
      (labelCol, requestedKind) match {
        case (Some(labelColumnName), Some(scoreValueKind)) =>
          ("custom model", labelColumnName, scoreValueKind)
        case _ =>
          val missingSettings = Seq(
            if (labelCol.isEmpty) Some("labelCol") else None,
            if (requestedKind.isEmpty) Some("evaluationMetric") else None).flatten
          val availableColumns = schema.fieldNames.sorted.mkString("[", ", ", "]")
          throw new IllegalArgumentException(
            "Unable to determine a complete scored model from schema metadata. " +
              s"Set ${missingSettings.mkString(" and ")} " +
              s"(evaluationMetric must not be '${MetricConstants.AllSparkMetrics}'), " +
              "or score the dataset so one model has both label and prediction metadata. " +
              s"Available columns: $availableColumns")
      }
    }
  }

  private def getRequestedScoreValueKind(evaluationMetric: String): Option[String] = {
    if (evaluationMetric == MetricConstants.AllSparkMetrics) None
    else if (isClassificationMetric(evaluationMetric)) Some(SchemaConstants.ClassificationKind)
    else Some(SchemaConstants.RegressionKind)
  }

  private def tryGetSchemaInfo(schema: StructType,
                               labelCol: Option[String],
                               requestedKind: Option[String]): Option[ScoredModelCandidate] = {
    val matchingCandidates = getScoredModelNames(schema)
      .flatMap(modelName => getScoredModelCandidate(schema, modelName))
      .filter(candidate => labelCol.forall(_ == candidate.labelColumnName))
      .filter(candidate => requestedKind.forall(_ == candidate.scoreValueKind))

    matchingCandidates match {
      case Seq(candidate) => Some(candidate)
      case candidates if candidates.nonEmpty =>
        throw new IllegalArgumentException(
          "Ambiguous scored-model metadata. Multiple complete candidates match: " +
            candidates.map(_.description).mkString("[", ", ", "]. ") +
            "Set labelCol and evaluationMetric to select one candidate; remove stale score metadata if needed.")
      case _ => None
    }
  }

  private def getScoredModelCandidate(schema: StructType,
                                      modelName: String): Option[ScoredModelCandidate] = {
    for {
      labelColumnName <- Option(SparkSchema.getLabelColumnName(schema, modelName))
      scoreValueKind <- Option(SparkSchema.getScoreValueKind(schema, modelName, labelColumnName))
      if ValidScoreValueKinds.contains(scoreValueKind)
      predictionColumnName <- Option(SparkSchema.getSparkPredictionColumnName(schema, modelName))
    } yield ScoredModelCandidate(modelName, labelColumnName, scoreValueKind, predictionColumnName)
  }

  private def getScoredModelNames(schema: StructType): Seq[String] = {
    schema.fields
      .flatMap(field => getScoredModelNames(field.metadata))
      .distinct
      .sorted
      .toSeq
  }

  private def getScoredModelNames(colMetadata: Metadata): Seq[String] = {
    if (!colMetadata.contains(MMLTag)) Seq.empty
    else {
      val mlTagMetadata = colMetadata.getMetadata(MMLTag)
      MetadataUtilities.getMetadataKeys(mlTagMetadata)
        .filter(_.startsWith(SchemaConstants.ScoreModelPrefix))
        .toSeq
    }
  }

}
