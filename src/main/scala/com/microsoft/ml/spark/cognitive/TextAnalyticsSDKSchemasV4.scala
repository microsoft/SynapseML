package com.microsoft.ml.spark.cognitive

import com.azure.ai.textanalytics.models._
import com.azure.core.util.Context
import com.microsoft.ml.spark.core.schema.SparkBindings
import org.apache.spark.ml.ComplexParamsReadable

import java.time.OffsetDateTime

object DetectLanguageResponseV4 extends SparkBindings[TAResponseV4[DetectedLanguageV4]]

object KeyPhraseResponseV4 extends SparkBindings[TAResponseV4[KeyphraseV4]]

object SentimentResponseV4 extends SparkBindings[TAResponseV4[SentimentScoredDocumentV4]]

object HealthcareResponseV4 extends SparkBindings[TAResponseV4[HealthcareEntityV4]]

case class TAResponseV4[T](result: List[Option[T]],
                           error: List[Option[TAErrorV4]],
                           statistics: List[Option[DocumentStatistics]],
                           modelVersion: Option[String])
case class DetectedLanguageV4(name: String, iso6391Name: String, confidenceScore: Double)

case class TAErrorV4(errorCode: String, errorMessage: String, target: String)

case class TAWarningV4 (warningCode: String, message: String)
case class TextDocumentInputs (id: String, text: String)

case class TextAnalyticsRequestOptionsV4(modelVersion: String,
                                         includeStatistics: Boolean,
                                         disableServiceLogs: Boolean)

case class KeyphraseV4(keyPhrases: List[String], warnings: List[TAWarningV4])

case class SentimentConfidenceScoreV4(negative: Double, neutral: Double, positive: Double)

case class SentimentScoredDocumentV4(sentiment: String,
                                     confidenceScores: SentimentConfidenceScoreV4,
                                     sentences: List[SentimentSentenceV4],
                                     warnings: List[WarningsV4])

case class SentimentSentenceV4(text: String,
                               sentiment: String,
                               confidenceScores: SentimentConfidenceScoreV4,
                               opinion: Option[List[OpinionV4]],
                               offset: Int,
                               length: Int)

case class OpinionV4(target: TargetV4, assessment: List[AssessmentV4])

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

case class WarningsV4(text: String, warningCode: String)

case class AnalyzeHealthcareEntitiesV4(documents: List[String],
                                      language: String,
                                      options: AnalyzeHealthcareEntitiesOptions,
                                      context: Context)
case class AnalyzeHealthcareEntitiesResultCollectionV4(analyzeEntities: List[AnalyzeHealthcareEntitiesResultV4],
                                                        modelVersion: String,
                                                       statistics: List[TextDocumentBatchStatisticsV4])
case class AnalyzeHealthcareEntitiesOptionsV4(stringIndexType: StringIndexTypeV4,
                                             includeStatistics: List[AnalyzeHealthcareEntitiesOptionsV4],
                                              modelVersion: List[AnalyzeHealthcareEntitiesOptionsV4],
                                              disableServiceLogs: List[AnalyzeHealthcareEntitiesOptionsV4])

case class StringIndexTypeV4(TEXT_ELEMENT_V8: StringIndexTypeV4,
                           UNICODE_CODE_POINT: StringIndexTypeV4,
                           UTF16CODE_UNIT: StringIndexTypeV4)

case class AnalyzeHealthcareEntitiesOperationDetailV4(createdAt: OffsetDateTime,
                                                    expiresAt: OffsetDateTime,
                                                    lastModifiedAt: OffsetDateTime,
                                                    operationId: String)
case class AnalyzeHealthcareEntitiesResultV4(id: String,
                                             textDocumentStatistics: TextDocumentStatistics,
                                             error: TextAnalyticsErrorV4)
case class HealthcareEntityV4(assertion: HealthcareEntityAssertion,
                            category: String,
                            confidenceScore: Double,
                            dataSources: List[EntityDataSource],
                            length: Int,
                            normalizedText: String,
                            offset: Int,
                            subCategory: String,
                            text: String)
case class HealthcareEntityAssertionV4(association: EntityAssociation,
                                      certainty: EntityCertainty,
                                      conditionality: EntityConditionality)
case class HealthcareEntityRelationV4(relationType: HealthcareEntityRelationType,
                                      roles: HealthcareEntityRelationRole)
case class HealthcareEntityRelationRoleV4(entity: HealthcareEntityV4,
                                          name: String)
case class HealthcareEntityRelationTypeV4(name: String,
                                          entity: HealthcareEntityRelationTypeV4)
case class TextAnalyticsErrorV4(errorCode: TextAnalyticsErrorCode,
                              message: String,
                              target: String)
case class TextAnalyticsActionsV4(analyzeSentimentOptions: AnalyzeSentimentOptions,
                                  displayName: String,
                                  extractKeyPhrasesOptions: ExtractKeyPhrasesOptions,
                                  recognizeEntitiesOptions: RecognizeEntitiesOptions,
                                  recognizeLinkedEntitiesOptions: RecognizeLinkedEntitiesOptions,
                                  recognizePiiEntitiesOptions: RecognizePiiEntitiesOptions)
case class EntityDataSourceV4(name: String,
                            entityId: String)
case class TextDocumentBatchStatisticsV4(documentCount: Int,
                                       invalidDocumentCount: Int,
                                       transactionCount: Long,
                                       validDocumentCount: Int)