// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.azure.ai.textanalytics.models._
import com.microsoft.azure.synapse.ml.core.schema.SparkBindings

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object DetectLanguageResponseSDK extends SparkBindings[TAResponseSDK[DetectedLanguageSDK]]

object KeyPhraseResponseSDK extends SparkBindings[TAResponseSDK[KeyphraseSDK]]

object SentimentResponseSDK extends SparkBindings[TAResponseSDK[SentimentScoredDocumentSDK]]

object PIIResponseSDK extends SparkBindings[TAResponseSDK[PIIEntityCollectionSDK]]

object HealthcareResponseSDK extends SparkBindings[TAResponseSDK[HealthEntitiesResultSDK]]

object LinkedEntityResponseSDK extends SparkBindings[TAResponseSDK[LinkedEntityCollectionSDK]]

object NERResponseSDK extends SparkBindings[TAResponseSDK[NERCollectionSDK]]

case class TAResponseSDK[T](result: Option[T],
                            error: Option[TAErrorSDK],
                            statistics: Option[DocumentStatistics])

case class DetectedLanguageSDK(name: String,
                               iso6391Name: String,
                               confidenceScore: Double,
                               warnings: Seq[TAWarningSDK])

case class TAErrorSDK(errorCode: String, errorMessage: String, target: String)

case class TAWarningSDK(warningCode: String, message: String)

case class TextDocumentInputs(id: String, text: String)

case class KeyphraseSDK(keyPhrases: Seq[String], warnings: Seq[TAWarningSDK])

case class SentimentConfidenceScoreSDK(negative: Double, neutral: Double, positive: Double)

case class SentimentScoredDocumentSDK(sentiment: String,
                                      confidenceScores: SentimentConfidenceScoreSDK,
                                      sentences: Seq[SentimentSentenceSDK],
                                      warnings: Seq[TAWarningSDK])

case class SentimentSentenceSDK(text: String,
                                sentiment: String,
                                confidenceScores: SentimentConfidenceScoreSDK,
                                opinions: Option[Seq[OpinionSDK]],
                                offset: Int,
                                length: Int)

case class OpinionSDK(target: TargetSDK, assessments: Seq[AssessmentSDK])

case class TargetSDK(text: String,
                     sentiment: String,
                     confidenceScores: SentimentConfidenceScoreSDK,
                     offset: Int,
                     length: Int)

case class AssessmentSDK(text: String,
                         sentiment: String,
                         confidenceScores: SentimentConfidenceScoreSDK,
                         isNegated: Boolean,
                         offset: Int,
                         length: Int)

case class PIIEntityCollectionSDK(entities: Seq[PIIEntitySDK],
                                  redactedText: String,
                                  warnings: Seq[TAWarningSDK])

case class PIIEntitySDK(text: String,
                        category: String,
                        subCategory: String,
                        confidenceScore: Double,
                        offset: Int,
                        length: Int)

case class HealthEntitiesResultSDK(id: String,
                                   warnings: Seq[TAWarningSDK],
                                   entities: Seq[HealthcareEntitySDK],
                                   entityRelation: Seq[HealthcareEntityRelationSDK])

case class HealthEntitiesOperationDetailSDK(createdAt: String,
                                            expiresAt: String,
                                            lastModifiedAt: String,
                                            operationId: String)

case class EntityDataSourceSDK(name: String,
                               entityId: String)

case class HealthcareEntitySDK(assertion: Option[HealthcareEntityAssertionSDK],
                               category: String,
                               confidenceScore: Double,
                               dataSources: Seq[EntityDataSourceSDK],
                               length: Int,
                               normalizedText: String,
                               offset: Int,
                               subCategory: String,
                               text: String)

case class HealthcareEntityAssertionSDK(association: Option[String],
                                        certainty: Option[String],
                                        conditionality: Option[String])

case class HealthcareEntityRelationSDK(relationType: String,
                                       roles: Seq[HealthcareEntityRelationRoleSDK])

case class HealthcareEntityRelationRoleSDK(entity: HealthcareEntitySDK, name: String)

case class LinkedEntityCollectionSDK(entities: Seq[LinkedEntitySDK],
                                     warnings: Seq[TAWarningSDK])

case class LinkedEntitySDK(name: String,
                           matches: Seq[LinkedEntityMatchSDK],
                           language: String,
                           dataSourceEntityId: String,
                           url: String,
                           dataSource: String,
                           bingEntitySearchApiId: String)

case class LinkedEntityMatchSDK(text: String,
                                confidenceScore: Double,
                                offset: Int,
                                length: Int)

case class NERCollectionSDK(entities: Seq[NEREntitySDK], warnings: Seq[TAWarningSDK])

case class NEREntitySDK(text: String,
                        category: String,
                        subCategory: String,
                        confidenceScore: Double,
                        offset: Int,
                        length: Int)

object SDKConverters {
  implicit def fromSDK(score: SentimentConfidenceScores): SentimentConfidenceScoreSDK = {
    SentimentConfidenceScoreSDK(
      score.getNegative,
      score.getNeutral,
      score.getPositive)
  }

  implicit def fromSDK(target: TargetSentiment): TargetSDK = {
    TargetSDK(
      target.getText,
      target.getSentiment.toString,
      target.getConfidenceScores,
      target.getOffset,
      target.getLength)
  }

  implicit def fromSDK(assess: AssessmentSentiment): AssessmentSDK = {
    AssessmentSDK(
      assess.getText,
      assess.getSentiment.toString,
      assess.getConfidenceScores,
      assess.isNegated,
      assess.getOffset,
      assess.getLength)
  }

  implicit def fromSDK(op: SentenceOpinion): OpinionSDK = {
    OpinionSDK(
      op.getTarget,
      op.getAssessments.asScala.toSeq.map(fromSDK)
    )
  }

  implicit def fromSDK(ss: SentenceSentiment): SentimentSentenceSDK = {
    SentimentSentenceSDK(
      ss.getText,
      ss.getSentiment.toString,
      ss.getConfidenceScores,
      Option(ss.getOpinions).map(sentenceOpinions =>
        sentenceOpinions.asScala.toSeq.map(fromSDK)
      ),
      ss.getOffset,
      ss.getLength)
  }

  implicit def fromSDK(warning: TextAnalyticsWarning): TAWarningSDK = {
    TAWarningSDK(warning.getMessage, warning.getWarningCode.toString)
  }

  implicit def fromSDK(error: TextAnalyticsError): TAErrorSDK = {
    TAErrorSDK(
      error.getErrorCode.toString,
      error.getMessage,
      error.getTarget)
  }

  implicit def fromSDK(s: TextDocumentStatistics): DocumentStatistics = {
    DocumentStatistics(s.getCharacterCount, s.getTransactionCount)
  }

  implicit def fromSDK(doc: AnalyzeSentimentResult): SentimentScoredDocumentSDK = {
    SentimentScoredDocumentSDK(
      doc.getDocumentSentiment.getSentiment.toString,
      doc.getDocumentSentiment.getConfidenceScores,
      doc.getDocumentSentiment.getSentences.asScala.toSeq.map(fromSDK),
      doc.getDocumentSentiment.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(phrases: ExtractKeyPhraseResult): KeyphraseSDK = {
    KeyphraseSDK(
      phrases.getKeyPhrases.asScala.toSeq,
      phrases.getKeyPhrases.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(result: DetectLanguageResult): DetectedLanguageSDK = {
    DetectedLanguageSDK(
      result.getPrimaryLanguage.getName,
      result.getPrimaryLanguage.getIso6391Name,
      result.getPrimaryLanguage.getConfidenceScore,
      result.getPrimaryLanguage.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(ent: PiiEntity): PIIEntitySDK = {
    PIIEntitySDK(
      ent.getText,
      ent.getCategory.toString,
      ent.getSubcategory,
      ent.getConfidenceScore,
      ent.getOffset,
      ent.getLength)
  }

  implicit def fromSDK(entity: RecognizePiiEntitiesResult): PIIEntityCollectionSDK = {
    PIIEntityCollectionSDK(
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntities.getRedactedText,
      entity.getEntities.getWarnings.asScala.toSeq.map(fromSDK))
  }

  implicit def fromSDK(ent: EntityDataSource): EntityDataSourceSDK = {
    EntityDataSourceSDK(
      ent.getName,
      ent.getEntityId
    )
  }

  implicit def fromSDK(entity: AnalyzeHealthcareEntitiesResult): HealthEntitiesResultSDK = {
    HealthEntitiesResultSDK(
      entity.getId,
      entity.getWarnings.asScala.toSeq.map(fromSDK),
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntityRelations.asScala.toSeq.map(fromSDK)
    )
  }

  implicit def fromSDK(ent: HealthcareEntity): HealthcareEntitySDK = {
    HealthcareEntitySDK(
      Option(ent.getAssertion).map(fromSDK),
      ent.getCategory.toString,
      ent.getConfidenceScore,
      ent.getDataSources.asScala.toSeq.map(fromSDK),
      ent.getLength,
      ent.getNormalizedText,
      ent.getOffset,
      ent.getSubcategory,
      ent.getText
    )
  }

  implicit def fromSDK(entityAssertion: HealthcareEntityAssertion): HealthcareEntityAssertionSDK = {
    HealthcareEntityAssertionSDK(
      Option(entityAssertion.getAssociation).map(_.toString),
      Option(entityAssertion.getCertainty).map(_.toString),
      Option(entityAssertion.getConditionality).map(_.toString)
    )
  }

  implicit def fromSDK(rel: HealthcareEntityRelation): HealthcareEntityRelationSDK = {
    HealthcareEntityRelationSDK(
      rel.getRelationType.toString,
      rel.getRoles.asScala.toSeq.map(fromSDK)
    )
  }

  implicit def fromSDK(role: HealthcareEntityRelationRole): HealthcareEntityRelationRoleSDK = {
    HealthcareEntityRelationRoleSDK(
      role.getEntity,
      role.getName
    )
  }

  implicit def fromSDK(entity: RecognizeLinkedEntitiesResult): LinkedEntityCollectionSDK = {
    LinkedEntityCollectionSDK(
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntities.getWarnings.asScala.toSeq.map(fromSDK)
    )
  }

  implicit def fromSDK(ent: LinkedEntity): LinkedEntitySDK = {
    LinkedEntitySDK(
      ent.getName,
      ent.getMatches.asScala.toSeq.map(fromSDK),
      ent.getLanguage,
      ent.getDataSourceEntityId,
      ent.getUrl,
      ent.getDataSource,
      ent.getBingEntitySearchApiId
    )
  }

  implicit def fromSDK(ent: LinkedEntityMatch): LinkedEntityMatchSDK = {
    LinkedEntityMatchSDK(
      ent.getText,
      ent.getConfidenceScore,
      ent.getOffset,
      ent.getLength
    )
  }

  implicit def fromSDK(entity: CategorizedEntity): NEREntitySDK = {
    NEREntitySDK(
      entity.getText,
      entity.getCategory.toString,
      entity.getSubcategory,
      entity.getConfidenceScore,
      entity.getOffset,
      entity.getLength)
  }

  implicit def fromSDK(entity: RecognizeEntitiesResult): NERCollectionSDK = {
    NERCollectionSDK(
      entity.getEntities.asScala.toSeq.map(fromSDK),
      entity.getEntities.getWarnings.asScala.toSeq.map(fromSDK))
  }

  def unpackResult[T <: TextAnalyticsResult, U](result: T)(implicit converter: T => U):
  (Option[TAErrorSDK], Option[DocumentStatistics], Option[U]) = {
    if (result.isError) {
      (Some(fromSDK(result.getError)), None, None)
    } else {
      (None, Option(result.getStatistics).map(fromSDK), Some(converter(result)))
    }
  }

  def toResponse[T <: TextAnalyticsResult, U](rc: Iterable[T])(implicit converter: T => U)
  : Seq[TAResponseSDK[U]] = {
    rc.map(unpackResult(_)(converter))
      .toSeq.map(tup => new TAResponseSDK[U](tup._3, tup._1, tup._2))
  }
}

