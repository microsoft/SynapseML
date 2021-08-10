// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import scala.collection.JavaConverters._
import com.azure.ai.textanalytics.models._
import com.azure.ai.textanalytics.util._
import com.microsoft.ml.spark.cognitive.SDKConverters.fromSDK
import com.microsoft.ml.spark.core.schema.SparkBindings

import scala.language.implicitConversions

object DetectLanguageResponseV4 extends SparkBindings[TAResponseV4[DetectedLanguageV4]]

object KeyPhraseResponseV4 extends SparkBindings[TAResponseV4[KeyphraseV4]]

object SentimentResponseV4 extends SparkBindings[TAResponseV4[SentimentScoredDocumentV4]]

object PIIResponseV4 extends SparkBindings[TAResponseV4[PIIEntityCollectionV4]]

object HealthcareResponseV4 extends SparkBindings[TAResponseV4[HealthEntitiesResultV4]]

object NERResponseV4 extends SparkBindings[TAResponseV4[NERCollectionV4]]

case class TAResponseV4[T](result: Seq[Option[T]],
                           error: Seq[Option[TAErrorV4]],
                           statistics: Seq[Option[DocumentStatistics]])
case class DetectedLanguageV4(name: String,
                              iso6391Name: String,
                              confidenceScore: Double,
                              warnings: Seq[TAWarningV4])

case class TAErrorV4(errorCode: String, errorMessage: String, target: String)

case class TAWarningV4(warningCode: String, message: String)

case class TextDocumentInputs(id: String, text: String)

case class TextAnalyticsRequestOptionsV4(modelVersion: String,
                                         includeStatistics: Boolean,
                                         disableServiceLogs: Boolean)

case class KeyphraseV4(keyPhrases: Seq[String], warnings: Seq[TAWarningV4])

case class SentimentConfidenceScoreV4(negative: Double, neutral: Double, positive: Double)

case class SentimentScoredDocumentV4(sentiment: String,
                                     confidenceScores: SentimentConfidenceScoreV4,
                                     sentences: Seq[SentimentSentenceV4],
                                     warnings: Seq[TAWarningV4])

case class SentimentSentenceV4(text: String,
                               sentiment: String,
                               confidenceScores: SentimentConfidenceScoreV4,
                               opinion: Option[Seq[OpinionV4]],
                               offset: Int,
                               length: Int)

case class OpinionV4(target: TargetV4, assessment: Seq[AssessmentV4])

case class TargetV4(text: String,
                    sentiment: String,
                    confidenceScores: SentimentConfidenceScoreV4,
                    offset: Int,
                    length: Int)

case class AssessmentV4(text: String,
                        sentiment: String,
                        confidenceScores: SentimentConfidenceScoreV4,
                        isNegated: Boolean,
                        offset: Int,
                        length: Int)

case class PIIEntityCollectionV4(entities: Seq[PIIEntityV4],
                                 redactedText: String,
                                 warnings: Seq[TAWarningV4])

case class PIIEntityV4(text: String,
                       category: String,
                       subCategory: String,
                       confidenceScore: Double,
                       offset: Int,
                       length: Int)

case class HealthEntitiesResultV4(id: String,
                                  warnings: Seq[TAWarningV4],
                                  entities: Seq[HealthcareEntityV4],
                                  entityRelation: Seq[HealthcareEntityRelationV4])

case class HealthEntitiesOperationDetailV4(createdAt: String,
                                           expiresAt: String,
                                           lastModifiedAt: String,
                                           operationId: String)

case class EntityDataSourceV4(name: String,
                              entityId: String)

case class HealthcareEntityV4(assertion: Option[HealthcareEntityAssertionV4],
                              category: String,
                              confidenceScore: Double,
                              dataSources: Seq[EntityDataSourceV4],
                              length: Int,
                              normalizedText: String,
                              offset: Int,
                              subCategory: String,
                              text: String)

case class HealthcareEntityAssertionV4(association: Option[String],
                                       certainty: Option[String],
                                       conditionality: Option[String])

case class HealthcareEntityRelationV4(relationType: String,
                                      roles: Seq[HealthcareEntityRelationRoleV4])

case class HealthcareEntityRelationRoleV4(entity: HealthcareEntityV4, name: String)

case class NERCollectionV4(entities: Seq[NEREntityV4], warnings: Seq[TAWarningV4])

case class NEREntityV4(text: String,
                       category: String,
                       subCategory: String,
                       confidenceScore: Double,
                       offset: Int)


object SDKConverters {
  implicit def fromSDK(score: SentimentConfidenceScores): SentimentConfidenceScoreV4 = {
    SentimentConfidenceScoreV4(
      score.getNegative,
      score.getNeutral,
      score.getPositive)
  }

  implicit def fromSDK(target: TargetSentiment): TargetV4 = {
    TargetV4(
      target.getText,
      target.getSentiment.toString,
      target.getConfidenceScores,
      target.getOffset,
      target.getLength)
  }

  implicit def fromSDK(assess: AssessmentSentiment): AssessmentV4 = {
    AssessmentV4(
      assess.getText,
      assess.getSentiment.toString,
      assess.getConfidenceScores,
      assess.isNegated,
      assess.getOffset,
      assess.getLength)
  }

  implicit def fromSDK(op: SentenceOpinion): OpinionV4 = {
    OpinionV4(
      op.getTarget,
      op.getAssessments.asScala.toSeq.map(fromSDK)
    )
  }

  implicit def fromSDK(ss: SentenceSentiment): SentimentSentenceV4 = {
    SentimentSentenceV4(
      ss.getText,
      ss.getSentiment.toString,
      ss.getConfidenceScores,
      Option(ss.getOpinions).map(sentenceOpinions =>
        sentenceOpinions.asScala.toSeq.map(fromSDK)
      ),
      ss.getOffset,
      ss.getLength)
  }

  implicit def fromSDK(warning: TextAnalyticsWarning): TAWarningV4 = {
    TAWarningV4(warning.getMessage, warning.getWarningCode.toString)
  }

  implicit def fromSDK(error: TextAnalyticsError): TAErrorV4 = {
    TAErrorV4(
      error.getErrorCode.toString,
      error.getMessage,
      error.getTarget)
  }

  implicit def fromSDK(s: TextDocumentStatistics): DocumentStatistics = {
    DocumentStatistics(s.getCharacterCount, s.getTransactionCount)
  }

  implicit def fromSDK(doc: AnalyzeSentimentResult): SentimentScoredDocumentV4 = {
    SentimentScoredDocumentV4(
      doc.getDocumentSentiment.getSentiment.toString,
      doc.getDocumentSentiment.getConfidenceScores,
      doc.getDocumentSentiment.getSentences.asScala.toSeq.map(fromSDK),
      doc.getDocumentSentiment.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(phrases: ExtractKeyPhraseResult): KeyphraseV4 = {
    KeyphraseV4(
      phrases.getKeyPhrases.asScala.toSeq,
      phrases.getKeyPhrases.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(result: DetectLanguageResult): DetectedLanguageV4 = {
    DetectedLanguageV4(
      result.getPrimaryLanguage.getName,
      result.getPrimaryLanguage.getIso6391Name,
      result.getPrimaryLanguage.getConfidenceScore,
      result.getPrimaryLanguage.getWarnings.asScala.toSeq.map(fromSDK))
  }
  implicit def fromSDK(ent: PiiEntity): PIIEntityV4 = {
    PIIEntityV4(
      ent.getText,
      ent.getCategory.toString,
      ent.getSubcategory,
      ent.getConfidenceScore,
      ent.getOffset,
      ent.getLength)
  }
  implicit def fromSDK(entity: RecognizePiiEntitiesResult): PIIEntityCollectionV4 = {
    PIIEntityCollectionV4(
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntities.getRedactedText,
      entity.getEntities.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(ent: EntityDataSource): EntityDataSourceV4 = {
    EntityDataSourceV4(
      ent.getName,
      ent.getEntityId
    )
  }
  implicit def fromSDK(entity: AnalyzeHealthcareEntitiesResult): HealthEntitiesResultV4 = {
    HealthEntitiesResultV4(
      entity.getId,
      entity.getWarnings.asScala.toSeq.map(fromSDK),
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntityRelations.asScala.toSeq.map(fromSDK)
    )
  }
  implicit def fromSDK(ent: HealthcareEntity): HealthcareEntityV4 = {
    HealthcareEntityV4(
      Option(ent.getAssertion).map(fromSDK),
      ent.getCategory,
      ent.getConfidenceScore,
      ent.getDataSources.asScala.toSeq.map(fromSDK),
      ent.getLength,
      ent.getNormalizedText,
      ent.getOffset,
      ent.getSubcategory,
      ent.getText
    )
  }
  implicit def fromSDK(entityAssertion: HealthcareEntityAssertion): HealthcareEntityAssertionV4 = {
    HealthcareEntityAssertionV4(
      Option(entityAssertion.getAssociation).map(_.toString),
      Option(entityAssertion.getCertainty).map(_.toString),
      Option(entityAssertion.getConditionality).map(_.toString)
    )
  }

  implicit def fromSDK(rel: HealthcareEntityRelation): HealthcareEntityRelationV4 = {
    HealthcareEntityRelationV4(
      rel.getRelationType.toString,
      rel.getRoles.asScala.toSeq.map(fromSDK)
    )
  }
  implicit def fromSDK(role: HealthcareEntityRelationRole): HealthcareEntityRelationRoleV4 = {
    HealthcareEntityRelationRoleV4(
      role.getEntity,
      role.getName
    )
  }
  implicit def fromSDK(entity: CategorizedEntity): NEREntityV4 = {
    NEREntityV4(
      entity.getText,
      entity.getCategory.toString,
      entity.getSubcategory,
      entity.getConfidenceScore,
      entity.getOffset)
  }
  implicit def fromSDK(entity: RecognizeEntitiesResult): NERCollectionV4 = {
    NERCollectionV4(
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntities.getWarnings.asScala.toSeq.map(fromSDK))
  }
  def unpackResult[T <: TextAnalyticsResult, U](result: T)(implicit converter: T => U):
  (Option[TAErrorV4], Option[DocumentStatistics], Option[U]) = {
    if (result.isError) {
      (Some(fromSDK(result.getError)), None, None)
    } else {
      (None, Option(result.getStatistics).map(fromSDK), Some(converter(result)))
    }
  }

  def toResponse[T <: TextAnalyticsResult, U](rc: Iterable[T])
                                             (implicit converter: T => U): TAResponseV4[U] = {
    val (errors, stats, results) = rc.map(unpackResult(_)(converter)).toSeq.unzip3
    TAResponseV4[U](results, errors, stats)
  }
}

