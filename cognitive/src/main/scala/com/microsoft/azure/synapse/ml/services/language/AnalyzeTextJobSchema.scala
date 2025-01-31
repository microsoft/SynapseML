// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json. RootJsonFormat

// scalastyle:off number.of.types
case class DocumentWarning(code: String,
                           message: String,
                           targetRef: Option[String])

object DocumentWarning extends SparkBindings[DocumentWarning]


case class SummaryContext(offset: Int,
                          length: Int)

object SummaryContext extends SparkBindings[SummaryContext]

//------------------------------------------------------------------------------------------------------
// Extractive Summarization
//------------------------------------------------------------------------------------------------------
case class ExtractiveSummarizationTaskParameters(loggingOptOut: Boolean,
                                                 modelVersion: String,
                                                 sentenceCount: Option[Int],
                                                 sortBy: Option[String],
                                                 stringIndexType: String)

case class ExtractiveSummarizationLROTask(parameters: ExtractiveSummarizationTaskParameters,
                                          taskName: Option[String],
                                          kind: String)

case class ExtractiveSummarizationJobsInput(displayName: Option[String],
                                            analysisInput: MultiLanguageAnalysisInput,
                                            tasks: Seq[ExtractiveSummarizationLROTask])

case class ExtractedSummarySentence(text: String,
                                    rankScore: Double,
                                    offset: Int,
                                    length: Int)

case class ExtractedSummaryDocumentResult(id: String,
                                          warnings: Seq[DocumentWarning],
                                          statistics: Option[RequestStatistics],
                                          sentences: Seq[ExtractedSummarySentence])

case class ExtractiveSummarizationResult(errors: Seq[ATError],
                                         statistics: Option[RequestStatistics],
                                         modelVersion: String,
                                         documents: Seq[ExtractedSummaryDocumentResult])

case class ExtractiveSummarizationLROResult(results: ExtractiveSummarizationResult,
                                            lastUpdateDateTime: String,
                                            status: String,
                                            taskName: Option[String],
                                            kind: String)

case class ExtractiveSummarizationTaskResult(completed: Int,
                                             failed: Int,
                                             inProgress: Int,
                                             total: Int,
                                             items: Option[Seq[ExtractiveSummarizationLROResult]])

case class ExtractiveSummarizationJobState(displayName: Option[String],
                                           createdDateTime: String,
                                           expirationDateTime: Option[String],
                                           jobId: String,
                                           lastUpdatedDateTime: String,
                                           status: String,
                                           errors: Option[Seq[String]],
                                           nextLink: Option[String],
                                           tasks: ExtractiveSummarizationTaskResult,
                                           statistics: Option[RequestStatistics])

object ExtractiveSummarizationJobState extends SparkBindings[ExtractiveSummarizationJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Abstractive Summarization
//------------------------------------------------------------------------------------------------------
object SummaryLength extends Enumeration {
  type SummaryLength = Value
  val Short, Medium, Long = Value
}

case class AbstractiveSummarizationTaskParameters(loggingOptOut: Boolean,
                                                  modelVersion: String,
                                                  sentenceCount: Option[Int],
                                                  stringIndexType: String,
                                                  summaryLength: Option[String])

case class AbstractiveSummarizationLROTask(parameters: AbstractiveSummarizationTaskParameters,
                                           taskName: Option[String],
                                           kind: String)

case class AbstractiveSummarizationJobsInput(displayName: Option[String],
                                             analysisInput: MultiLanguageAnalysisInput,
                                             tasks: Seq[AbstractiveSummarizationLROTask])

case class AbstractiveSummary(text: String,
                              contexts: Option[Seq[SummaryContext]])

case class AbstractiveSummaryDocumentResult(id: String,
                                            warnings: Seq[DocumentWarning],
                                            statistics: Option[RequestStatistics],
                                            summaries: Seq[AbstractiveSummary])

object AbstractiveSummaryDocumentResult extends SparkBindings[AbstractiveSummaryDocumentResult]

case class AbstractiveSummarizationResult(errors: Seq[ATError],
                                          statistics: Option[RequestStatistics],
                                          modelVersion: String,
                                          documents: Seq[AbstractiveSummaryDocumentResult])

case class AbstractiveSummarizationLROResult(results: AbstractiveSummarizationResult,
                                             lastUpdateDateTime: String,
                                             status: String,
                                             taskName: Option[String],
                                             kind: String)

case class AbstractiveSummarizationTaskResult(completed: Int,
                                              failed: Int,
                                              inProgress: Int,
                                              total: Int,
                                              items: Option[Seq[AbstractiveSummarizationLROResult]])

case class AbstractiveSummarizationJobState(displayName: Option[String],
                                            createdDateTime: String,
                                            expirationDateTime: Option[String],
                                            jobId: String,
                                            lastUpdatedDateTime: String,
                                            status: String,
                                            errors: Option[Seq[String]],
                                            nextLink: Option[String],
                                            tasks: AbstractiveSummarizationTaskResult,
                                            statistics: Option[RequestStatistics])

object AbstractiveSummarizationJobState extends SparkBindings[AbstractiveSummarizationJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// HealthCare
//------------------------------------------------------------------------------------------------------
case class HealthcareTaskParameters(loggingOptOut: Boolean,
                                    modelVersion: String,
                                    stringIndexType: String)

case class HealthcareLROTask(parameters: HealthcareTaskParameters,
                             taskName: Option[String],
                             kind: String)

case class HealthcareJobsInput(displayName: Option[String],
                               analysisInput: MultiLanguageAnalysisInput,
                               tasks: Seq[HealthcareLROTask])

case class HealthcareAssertion(conditionality: Option[String],
                               certainty: Option[String],
                               association: Option[String],
                               temporality: Option[String])

case class HealthcareEntitiesDocumentResult(id: String,
                                            warnings: Seq[DocumentWarning],
                                            statistics: Option[RequestStatistics],
                                            entities: Seq[HealthcareEntity],
                                            relations: Seq[HealthcareRelation],
                                            fhirBundle: Option[String])

case class HealthcareEntity(text: String,
                            category: String,
                            subcategory: Option[String],
                            offset: Int,
                            length: Int,
                            confidenceScore: Double,
                            assertion: Option[HealthcareAssertion],
                            name: Option[String],
                            links: Option[Seq[HealthcareEntityLink]])

case class HealthcareEntityLink(dataSource: String,
                                id: String)

case class HealthcareLROResult(results: HealthcareResult,
                               lastUpdateDateTime: String,
                               status: String,
                               taskName: Option[String],
                               kind: String)

case class HealthcareRelation(relationType: String,
                              entities: Seq[HealthcareRelationEntity],
                              confidenceScore: Option[Double])

case class HealthcareRelationEntity(ref: String,
                                    role: String)

case class HealthcareResult(errors: Seq[DocumentError],
                            statistics: Option[RequestStatistics],
                            modelVersion: String,
                            documents: Seq[HealthcareEntitiesDocumentResult])

case class HealthcareTaskResult(completed: Int,
                                failed: Int,
                                inProgress: Int,
                                total: Int,
                                items: Option[Seq[HealthcareLROResult]])

case class HealthcareJobState(displayName: Option[String],
                              createdDateTime: String,
                              expirationDateTime: Option[String],
                              jobId: String,
                              lastUpdatedDateTime: String,
                              status: String,
                              errors: Option[Seq[String]],
                              nextLink: Option[String],
                              tasks: HealthcareTaskResult,
                              statistics: Option[RequestStatistics])

object HealthcareJobState extends SparkBindings[HealthcareJobState]

//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Sentiment Analysis
//------------------------------------------------------------------------------------------------------
case class SentimentAnalysisLROTask(parameters: SentimentAnalysisTaskParameters,
                                    taskName: Option[String],
                                    kind: String)

case class SentimentAnalysisJobsInput(displayName: Option[String],
                                      analysisInput: MultiLanguageAnalysisInput,
                                      tasks: Seq[SentimentAnalysisLROTask])

case class SentimentAnalysisLROResult(results: SentimentResult,
                                      lastUpdateDateTime: String,
                                      status: String,
                                      taskName: Option[String],
                                      kind: String)

case class SentimentAnalysisTaskResult(completed: Int,
                                       failed: Int,
                                       inProgress: Int,
                                       total: Int,
                                       items: Option[Seq[SentimentAnalysisLROResult]])

case class SentimentAnalysisJobState(displayName: Option[String],
                                     createdDateTime: String,
                                     expirationDateTime: Option[String],
                                     jobId: String,
                                     lastUpdatedDateTime: String,
                                     status: String,
                                     errors: Option[Seq[String]],
                                     nextLink: Option[String],
                                     tasks: SentimentAnalysisTaskResult,
                                     statistics: Option[RequestStatistics])

object SentimentAnalysisJobState extends SparkBindings[SentimentAnalysisJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Key Phrase Extraction
//------------------------------------------------------------------------------------------------------
case class KeyPhraseExtractionLROTask(parameters: KPnLDTaskParameters,
                                      taskName: Option[String],
                                      kind: String)

case class KeyPhraseExtractionJobsInput(displayName: Option[String],
                                        analysisInput: MultiLanguageAnalysisInput,
                                        tasks: Seq[KeyPhraseExtractionLROTask])

case class KeyPhraseExtractionLROResult(results: KeyPhraseExtractionResult,
                                        lastUpdateDateTime: String,
                                        status: String,
                                        taskName: Option[String],
                                        kind: String)

case class KeyPhraseExtractionTaskResult(completed: Int,
                                         failed: Int,
                                         inProgress: Int,
                                         total: Int,
                                         items: Option[Seq[KeyPhraseExtractionLROResult]])

case class KeyPhraseExtractionJobState(displayName: Option[String],
                                       createdDateTime: String,
                                       expirationDateTime: Option[String],
                                       jobId: String,
                                       lastUpdatedDateTime: String,
                                       status: String,
                                       errors: Option[Seq[String]],
                                       nextLink: Option[String],
                                       tasks: KeyPhraseExtractionTaskResult,
                                       statistics: Option[RequestStatistics])

object KeyPhraseExtractionJobState extends SparkBindings[KeyPhraseExtractionJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// PII Entity Recognition
//------------------------------------------------------------------------------------------------------
object PiiDomain extends Enumeration {
  type PiiDomain = Value
  val None, Phi = Value
}

case class PiiEntityRecognitionLROTask(parameters: PiiTaskParameters,
                                       taskName: Option[String],
                                       kind: String)

case class PiiEntityRecognitionJobsInput(displayName: Option[String],
                                         analysisInput: MultiLanguageAnalysisInput,
                                         tasks: Seq[PiiEntityRecognitionLROTask])

case class PiiEntityRecognitionLROResult(results: PIIResult,
                                         lastUpdateDateTime: String,
                                         status: String,
                                         taskName: Option[String],
                                         kind: String)

case class PiiEntityRecognitionTaskResult(completed: Int,
                                          failed: Int,
                                          inProgress: Int,
                                          total: Int,
                                          items: Option[Seq[PiiEntityRecognitionLROResult]])

case class PiiEntityRecognitionJobState(displayName: Option[String],
                                        createdDateTime: String,
                                        expirationDateTime: Option[String],
                                        jobId: String,
                                        lastUpdatedDateTime: String,
                                        status: String,
                                        errors: Option[Seq[String]],
                                        nextLink: Option[String],
                                        tasks: PiiEntityRecognitionTaskResult,
                                        statistics: Option[RequestStatistics])

object PiiEntityRecognitionJobState extends SparkBindings[PiiEntityRecognitionJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Entity Linking
//------------------------------------------------------------------------------------------------------
case class EntityLinkingLROTask(parameters: EntityTaskParameters,
                                taskName: Option[String],
                                kind: String)


case class EntityLinkingJobsInput(displayName: Option[String],
                                  analysisInput: MultiLanguageAnalysisInput,
                                  tasks: Seq[EntityLinkingLROTask])


case class EntityLinkingLROResult(results: EntityLinkingResult,
                                  lastUpdateDateTime: String,
                                  status: String,
                                  taskName: Option[String],
                                  kind: String)

case class EntityLinkingTaskResult(completed: Int,
                                   failed: Int,
                                   inProgress: Int,
                                   total: Int,
                                   items: Option[Seq[EntityLinkingLROResult]])

case class EntityLinkingJobState(displayName: Option[String],
                                 createdDateTime: String,
                                 expirationDateTime: Option[String],
                                 jobId: String,
                                 lastUpdatedDateTime: String,
                                 status: String,
                                 errors: Option[Seq[String]],
                                 nextLink: Option[String],
                                 tasks: EntityLinkingTaskResult,
                                 statistics: Option[RequestStatistics])

object EntityLinkingJobState extends SparkBindings[EntityLinkingJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Entity Recognition
//------------------------------------------------------------------------------------------------------

case class EntityRecognitionTaskParameters(loggingOptOut: Boolean,
                                           modelVersion: String,
                                           stringIndexType: String,
                                           inclusionList: Option[Seq[String]],
                                           exclusionList: Option[Seq[String]],
                                           overlapPolicy: Option[EntityOverlapPolicy],
                                           inferenceOptions: Option[EntityInferenceOptions])

case class EntityOverlapPolicy(policyKind: String)

case class EntityInferenceOptions(excludeNormalizedValues: Boolean)

case class EntityRecognitionLROTask(parameters: EntityRecognitionTaskParameters,
                                    taskName: Option[String],
                                    kind: String)

case class EntityRecognitionJobsInput(displayName: Option[String],
                                      analysisInput: MultiLanguageAnalysisInput,
                                      tasks: Seq[EntityRecognitionLROTask])

case class EntityRecognitionLROResult(results: EntityRecognitionResult,
                                      lastUpdateDateTime: String,
                                      status: String,
                                      taskName: Option[String],
                                      kind: String)

case class EntityRecognitionTaskResult(completed: Int,
                                       failed: Int,
                                       inProgress: Int,
                                       total: Int,
                                       items: Option[Seq[EntityRecognitionLROResult]])

case class EntityRecognitionJobState(displayName: Option[String],
                                     createdDateTime: String,
                                     expirationDateTime: Option[String],
                                     jobId: String,
                                     lastUpdatedDateTime: String,
                                     status: String,
                                     errors: Option[Seq[String]],
                                     nextLink: Option[String],
                                     tasks: EntityRecognitionTaskResult,
                                     statistics: Option[RequestStatistics])

object EntityRecognitionJobState extends SparkBindings[EntityRecognitionJobState]
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Custom Entity Recognoition
//------------------------------------------------------------------------------------------------------
case class CustomEntitiesTaskParameters(loggingOptOut: Boolean,
                                        stringIndexType: String,
                                        deploymentName: String,
                                        projectName: String)

case class CustomEntityRecognitionLROTask(parameters: CustomEntitiesTaskParameters,
                                          taskName: Option[String],
                                          kind: String)

case class CustomEntitiesJobsInput(displayName: Option[String],
                                   analysisInput: MultiLanguageAnalysisInput,
                                   tasks: Seq[CustomEntityRecognitionLROTask])
//------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------
// Custom Label Classification
//------------------------------------------------------------------------------------------------------
case class CustomLabelTaskParameters(loggingOptOut: Boolean,
                                     deploymentName: String,
                                     projectName: String)

case class CustomLabelLROTask(parameters: CustomLabelTaskParameters,
                              taskName: Option[String],
                              kind: String)

case class CustomLabelJobsInput(displayName: Option[String],
                                analysisInput: MultiLanguageAnalysisInput,
                                tasks: Seq[CustomLabelLROTask])

case class ClassificationDocumentResult(id: String,
                                        warnings: Seq[DocumentWarning],
                                        statistics: Option[RequestStatistics],
                                        classifications: Seq[ClassificationResult])

//object ClassificationDocumentResult extends SparkBindings[ClassificationDocumentResult]

case class ClassificationResult(category: String,
                                confidenceScore: Double)

object ClassificationResult extends SparkBindings[ClassificationResult]

case class CustomLabelResult(errors: Seq[DocumentError],
                             statistics: Option[RequestStatistics],
                             modelVersion: String,
                             documents: Seq[ClassificationDocumentResult])

case class CustomLabelLROResult(results: CustomLabelResult,
                                lastUpdateDateTime: String,
                                status: String,
                                taskName: Option[String],
                                kind: String)

case class CustomLabelTaskResult(completed: Int,
                                 failed: Int,
                                 inProgress: Int,
                                 total: Int,
                                 items: Option[Seq[CustomLabelLROResult]])

case class CustomLabelJobState(displayName: Option[String],
                               createdDateTime: String,
                               expirationDateTime: Option[String],
                               jobId: String,
                               lastUpdatedDateTime: String,
                               status: String,
                               errors: Option[Seq[String]],
                               nextLink: Option[String],
                               tasks: CustomLabelTaskResult,
                               statistics: Option[RequestStatistics])

object CustomLabelJobState extends SparkBindings[CustomLabelJobState]
//------------------------------------------------------------------------------------------------------


object ATLROJSONFormat {

  import spray.json.DefaultJsonProtocol._
  import ATJSONFormat._

  implicit val DocumentWarningFormat: RootJsonFormat[DocumentWarning] =
    jsonFormat3(DocumentWarning.apply)

  implicit val ExtractiveSummarizationTaskParametersF: RootJsonFormat[ExtractiveSummarizationTaskParameters] =
    jsonFormat5(ExtractiveSummarizationTaskParameters.apply)

  implicit val ExtractiveSummarizationLROTaskF: RootJsonFormat[ExtractiveSummarizationLROTask] =
    jsonFormat3(ExtractiveSummarizationLROTask.apply)

  implicit val ExtractiveSummarizationJobsInputF: RootJsonFormat[ExtractiveSummarizationJobsInput] =
    jsonFormat3(ExtractiveSummarizationJobsInput.apply)

  implicit val AbstractiveSummarizationTaskParametersF: RootJsonFormat[AbstractiveSummarizationTaskParameters] =
    jsonFormat5(AbstractiveSummarizationTaskParameters.apply)

  implicit val AbstractiveSummarizationLROTaskF: RootJsonFormat[AbstractiveSummarizationLROTask] =
    jsonFormat3(AbstractiveSummarizationLROTask.apply)

  implicit val AbstractiveSummarizationJobsInputF: RootJsonFormat[AbstractiveSummarizationJobsInput] =
    jsonFormat3(AbstractiveSummarizationJobsInput.apply)

  implicit val HealthcareTaskParametersF: RootJsonFormat[HealthcareTaskParameters] =
    jsonFormat3(HealthcareTaskParameters.apply)

  implicit val HealthcareLROTaskF: RootJsonFormat[HealthcareLROTask] =
    jsonFormat3(HealthcareLROTask.apply)

  implicit val HealthcareJobsInputF: RootJsonFormat[HealthcareJobsInput] =
    jsonFormat3(HealthcareJobsInput.apply)

  implicit val SentimentAnalysisLROTaskF: RootJsonFormat[SentimentAnalysisLROTask] =
    jsonFormat3(SentimentAnalysisLROTask.apply)

  implicit val SentimentAnalysisJobsInputF: RootJsonFormat[SentimentAnalysisJobsInput] =
    jsonFormat3(SentimentAnalysisJobsInput.apply)

  implicit val KeyPhraseExtractionLROTaskF: RootJsonFormat[KeyPhraseExtractionLROTask] =
    jsonFormat3(KeyPhraseExtractionLROTask.apply)

  implicit val KeyPhraseExtractionJobsInputF: RootJsonFormat[KeyPhraseExtractionJobsInput] =
    jsonFormat3(KeyPhraseExtractionJobsInput.apply)

  implicit val PiiEntityRecognitionLROTaskF: RootJsonFormat[PiiEntityRecognitionLROTask] =
    jsonFormat3(PiiEntityRecognitionLROTask.apply)

  implicit val PiiEntityRecognitionJobsInputF: RootJsonFormat[PiiEntityRecognitionJobsInput] =
    jsonFormat3(PiiEntityRecognitionJobsInput.apply)

  implicit val EntityLinkingLROTaskF: RootJsonFormat[EntityLinkingLROTask] =
    jsonFormat3(EntityLinkingLROTask.apply)

  implicit val EntityLinkingJobsInputF: RootJsonFormat[EntityLinkingJobsInput] =
    jsonFormat3(EntityLinkingJobsInput.apply)

  implicit val EntityOverlapPolicyF: RootJsonFormat[EntityOverlapPolicy] =
    jsonFormat1(EntityOverlapPolicy.apply)

  implicit val EntityInferenceOptionsF: RootJsonFormat[EntityInferenceOptions] =
    jsonFormat1(EntityInferenceOptions.apply)

  implicit val EntityRecognitionTaskParametersF: RootJsonFormat[EntityRecognitionTaskParameters] =
    jsonFormat7(EntityRecognitionTaskParameters.apply)

  implicit val EntityRecognitionLROTaskF: RootJsonFormat[EntityRecognitionLROTask] =
    jsonFormat3(EntityRecognitionLROTask.apply)

  implicit val EntityRecognitionJobsInputF: RootJsonFormat[EntityRecognitionJobsInput] =
    jsonFormat3(EntityRecognitionJobsInput.apply)

  implicit val CustomEntitiesTaskParametersF: RootJsonFormat[CustomEntitiesTaskParameters] =
    jsonFormat4(CustomEntitiesTaskParameters.apply)

  implicit val CustomEntityRecognitionLROTaskF: RootJsonFormat[CustomEntityRecognitionLROTask] =
    jsonFormat3(CustomEntityRecognitionLROTask.apply)

  implicit val CustomEntitiesJobsInputF: RootJsonFormat[CustomEntitiesJobsInput] =
    jsonFormat3(CustomEntitiesJobsInput.apply)

  implicit val CustomSingleLabelTaskParametersF: RootJsonFormat[CustomLabelTaskParameters] =
    jsonFormat3(CustomLabelTaskParameters.apply)

  implicit val CustomSingleLabelLROTaskF: RootJsonFormat[CustomLabelLROTask] =
    jsonFormat3(CustomLabelLROTask.apply)

  implicit val CustomSingleLabelJobsInputF: RootJsonFormat[CustomLabelJobsInput] =
    jsonFormat3(CustomLabelJobsInput.apply)
}
