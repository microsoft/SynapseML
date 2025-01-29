package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.core.schema.SparkBindings
import spray.json. RootJsonFormat

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

object ExtractedSummarySentence extends SparkBindings[ExtractedSummarySentence]

case class ExtractedSummaryDocumentResult(id: String,
                                          warnings: Seq[DocumentWarning],
                                          statistics: Option[RequestStatistics],
                                          sentences: Seq[ExtractedSummarySentence])

object ExtractedSummaryDocumentResult extends SparkBindings[ExtractedSummaryDocumentResult]

case class ExtractiveSummarizationResult(errors: Seq[ATError],
                                         statistics: Option[RequestStatistics],
                                         modelVersion: String,
                                         documents: Seq[ExtractedSummaryDocumentResult])

object ExtractiveSummarizationResult extends SparkBindings[ExtractiveSummarizationResult]

case class ExtractiveSummarizationLROResult(results: ExtractiveSummarizationResult,
                                            lastUpdateDateTime: String,
                                            status: String,
                                            taskName: Option[String],
                                            kind: String)

object ExtractiveSummarizationLROResult extends SparkBindings[ExtractiveSummarizationLROResult]

case class ExtractiveSummarizationTaskResult(completed: Int,
                                             failed: Int,
                                             inProgress: Int,
                                             total: Int,
                                             items: Option[Seq[ExtractiveSummarizationLROResult]])

object ExtractiveSummarizationTaskResult extends SparkBindings[ExtractiveSummarizationTaskResult]

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

object AbstractiveSummary extends SparkBindings[AbstractiveSummary]

case class AbstractiveSummaryDocumentResult(id: String,
                                            warnings: Seq[DocumentWarning],
                                            statistics: Option[RequestStatistics],
                                            summaries: Seq[AbstractiveSummary])

object AbstractiveSummaryDocumentResult extends SparkBindings[AbstractiveSummaryDocumentResult]

case class AbstractiveSummarizationResult(errors: Seq[ATError],
                                          statistics: Option[RequestStatistics],
                                          modelVersion: String,
                                          documents: Seq[AbstractiveSummaryDocumentResult])

object AbstractiveSummarizationResult extends SparkBindings[AbstractiveSummarizationResult]

case class AbstractiveSummarizationLROResult(results: AbstractiveSummarizationResult,
                                             lastUpdateDateTime: String,
                                             status: String,
                                             taskName: Option[String],
                                             kind: String)

object AbstractiveSummarizationLROResult extends SparkBindings[AbstractiveSummarizationLROResult]


case class AbstractiveSummarizationTaskResult(completed: Int,
                                              failed: Int,
                                              inProgress: Int,
                                              total: Int,
                                              items: Option[Seq[AbstractiveSummarizationLROResult]])

object AbstractiveSummarizationTaskResult extends SparkBindings[AbstractiveSummarizationTaskResult]

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

object HealthcareAssertion extends SparkBindings[HealthcareAssertion]

case class HealthcareEntitiesDocumentResult(id: String,
                                            warnings: Seq[DocumentWarning],
                                            statistics: Option[RequestStatistics],
                                            entities: Seq[HealthcareEntity],
                                            relations: Seq[HealthcareRelation],
                                            fhirBundle: Option[String])

object HealthcareEntitiesDocumentResult extends SparkBindings[HealthcareEntitiesDocumentResult]

case class HealthcareEntity(text: String,
                            category: String,
                            subcategory: Option[String],
                            offset: Int,
                            length: Int,
                            confidenceScore: Double,
                            assertion: Option[HealthcareAssertion],
                            name: Option[String],
                            links: Option[Seq[HealthcareEntityLink]])

object HealthcareEntity extends SparkBindings[HealthcareEntity]

case class HealthcareEntityLink(dataSource: String,
                                id: String)

object HealthcareEntityLink extends SparkBindings[HealthcareEntityLink]

case class HealthcareLROResult(results: HealthcareResult,
                               lastUpdateDateTime: String,
                               status: String,
                               taskName: Option[String],
                               kind: String)

object HealthcareLROResult extends SparkBindings[HealthcareLROResult]


case class HealthcareRelation(relationType: String,
                              entities: Seq[HealthcareRelationEntity],
                              confidenceScore: Option[Double])

object HealthcareRelation extends SparkBindings[HealthcareRelation]

case class HealthcareRelationEntity(ref: String,
                                    role: String)

object HealthcareRelationEntity extends SparkBindings[HealthcareRelationEntity]

case class HealthcareResult(errors: Seq[DocumentError],
                            statistics: Option[RequestStatistics],
                            modelVersion: String,
                            documents: Seq[HealthcareEntitiesDocumentResult])

object HealthcareResult extends SparkBindings[HealthcareResult]

case class HealthcareTaskResult(completed: Int,
                                failed: Int,
                                inProgress: Int,
                                total: Int,
                                items: Option[Seq[HealthcareLROResult]])

object HealthcareTaskResult extends SparkBindings[HealthcareTaskResult]

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

object SentimentAnalysisLROResult extends SparkBindings[SentimentAnalysisLROResult]

case class SentimentAnalysisTaskResult(completed: Int,
                                       failed: Int,
                                       inProgress: Int,
                                       total: Int,
                                       items: Option[Seq[SentimentAnalysisLROResult]])

object SentimentAnalysisTaskResult extends SparkBindings[SentimentAnalysisTaskResult]

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

object KeyPhraseExtractionLROResult extends SparkBindings[KeyPhraseExtractionLROResult]

case class KeyPhraseExtractionTaskResult(completed: Int,
                                         failed: Int,
                                         inProgress: Int,
                                         total: Int,
                                         items: Option[Seq[KeyPhraseExtractionLROResult]])

object KeyPhraseExtractionTaskResult extends SparkBindings[KeyPhraseExtractionTaskResult]

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

object PiiEntityRecognitionLROResult extends SparkBindings[PiiEntityRecognitionLROResult]

case class PiiEntityRecognitionTaskResult(completed: Int,
                                          failed: Int,
                                          inProgress: Int,
                                          total: Int,
                                          items: Option[Seq[PiiEntityRecognitionLROResult]])

object PiiEntityRecognitionTaskResult extends SparkBindings[PiiEntityRecognitionTaskResult]

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

object EntityLinkingLROResult extends SparkBindings[EntityLinkingLROResult]

case class EntityLinkingTaskResult(completed: Int,
                                   failed: Int,
                                   inProgress: Int,
                                   total: Int,
                                   items: Option[Seq[EntityLinkingLROResult]])

object EntityLinkingTaskResult extends SparkBindings[EntityLinkingTaskResult]

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

object EntityRecognitionLROResult extends SparkBindings[EntityRecognitionLROResult]

case class EntityRecognitionTaskResult(completed: Int,
                                       failed: Int,
                                       inProgress: Int,
                                       total: Int,
                                       items: Option[Seq[EntityRecognitionLROResult]])

object EntityRecognitionTaskResult extends SparkBindings[EntityRecognitionTaskResult]

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
                                        classes: Seq[ClassificationResult])

object ClassificationDocumentResult extends SparkBindings[ClassificationDocumentResult]

case class ClassificationResult(category: String,
                                confidenceScore: Double)

object ClassificationResult extends SparkBindings[ClassificationResult]

case class CustomLabelResult(errors: Seq[DocumentError],
                             statistics: Option[RequestStatistics],
                             modelVersion: String,
                             documents: Seq[ClassificationDocumentResult])

object CustomLabelResult extends SparkBindings[CustomLabelResult]

case class CustomLabelLROResult(results: CustomLabelResult,
                                lastUpdateDateTime: String,
                                status: String,
                                taskName: Option[String],
                                kind: String)

object CustomLabelLROResult extends SparkBindings[CustomLabelLROResult]

case class CustomLabelTaskResult(completed: Int,
                                 failed: Int,
                                 inProgress: Int,
                                 total: Int,
                                 items: Option[Seq[CustomLabelLROResult]])

object CustomLabelTaskResult extends SparkBindings[CustomLabelTaskResult]

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
