package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.logging.{ FeatureNames, SynapseMLLogging }
import com.microsoft.azure.synapse.ml.services.{
  CognitiveServicesBaseNoHandler, HasAPIVersion, HasCognitiveServiceInput, HasInternalJsonOutputParser, HasSetLocation }
import com.microsoft.azure.synapse.ml.services.text.{ TADocument, TextAnalyticsAutoBatch }
import com.microsoft.azure.synapse.ml.services.vision.BasicAsyncReply
import com.microsoft.azure.synapse.ml.stages.{ FixedMiniBatchTransformer, FlattenBatch, HasBatchSize, UDFTransformer }
import org.apache.http.entity.{ AbstractHttpEntity, StringEntity }
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml.{ ComplexParamsReadable, NamespaceInjections, PipelineModel }
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{ ArrayType, DataType, StructType }
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.net.URI

object AnalyzeTextLongRunningOperations extends ComplexParamsReadable[AnalyzeTextLongRunningOperations]
                                                with Serializable

/**
 * <p>
 * This transformer is used to analyze text using the Azure AI Language service. It uses AI service asynchronously.
 * For more details please visit
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/concepts/use-asynchronously]]
 * For each row, it submits a job to the service and polls the service until the job is complete. Delay between
 * polling requests is controlled by the [[pollingDelay]] parameter, which is set to 1000 milliseconds by default.
 * Number of polling attempts is controlled by the [[maxPollingRetries]] parameter, which is set to 1000 by default.
 * </p>
 * <p>
 * This transformer will use the field specified as TextCol to submit the text to the service. The response from the
 * service will be stored in the field specified as OutputCol. The response will be a struct with the
 * following fields:
 * <ul>
 *   <li>statistics: A struct containing statistics about the job.</li>
 *   <li>documents: An array of structs containing the results for each document.</li>
 *   <li>errors: An array of structs containing the errors for each document.</li>
 *   <li>modelVersion: The version of the model used to analyze the text.</li>
 * </ul>
 * </p>
 * <p>
 * This transformer support only single task per row. The task to be performed is specified by the [[kind]] parameter.
 * The supported tasks are:
 * <ul>
 *   <li>ExtractiveSummarization</li>
 *   <li>AbstractiveSummarization</li>
 *   <li>Healthcare</li>
 *   <li>SentimentAnalysis</li>
 *   <li>KeyPhraseExtraction</li>
 *   <li>PiiEntityRecognition</li>
 *   <li>EntityLinking</li>
 *   <li>EntityRecognition</li>
 *   <li>CustomEntityRecognition</li>
 * </ul>
 * Each task has its own set of parameters that can be set to control the behavior of the service and response
 * schema.
 * </p>
 */
class AnalyzeTextLongRunningOperations(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
                                                                         with HasAPIVersion
                                                                         with BasicAsyncReply
                                                                         with HasCognitiveServiceInput
                                                                         with HasInternalJsonOutputParser
                                                                         with HasSetLocation
                                                                         with TextAnalyticsAutoBatch
                                                                         with SynapseMLLogging
                                                                         with HasAnalyzeTextServiceBaseParams
                                                                         with HasBatchSize
                                                                         with HandleExtractiveSummarization
                                                                         with HandleAbstractiveSummarization
                                                                         with HandleHealthcareTextAnalystics
                                                                         with HandleSentimentAnalysis
                                                                         with HandleKeyPhraseExtraction
                                                                         with HandlePiiEntityRecognition
                                                                         with HandleEntityLinking
                                                                         with HandleEntityRecognition
                                                                         with HandleCustomEntityRecognition {
  logClass(FeatureNames.AiServices.Language)
  def this() = this(Identifiable.randomUID("AnalyzeTextLongRunningOperations"))

  override private[ml] def internalServiceType: String = "textanalytics"

  override def urlPath: String = "/language/analyze-text/jobs"

  override protected def validKinds: Set[String] = responseDataTypeSchemaMap.keySet.map(_.toString)

  setDefault(
    apiVersion -> Left("2023-04-01"),
    showStats -> Left(false),
    batchSize -> 10,
    pollingDelay -> 1000
    )

  def setKind(value: AnalysisTaskKind.AnalysisTaskKind): this.type = set(kind, value.toString)

  override protected def shouldSkip(row: Row): Boolean = emptyParamData(row, text) || super.shouldSkip(row)

  /**
   * Modifies the polling URI to include the showStats parameter if enabled.
   */
  override protected def modifyPollingURI(originalURI: URI): URI = {
    if (getShowStats) {
      new URI(s"${ originalURI.toString }&showStats=true")
    } else {
      originalURI
    }
  }

  private val responseDataTypeSchemaMap: Map[AnalysisTaskKind.AnalysisTaskKind, StructType] = Map(
    AnalysisTaskKind.ExtractiveSummarization -> ExtractiveSummarizationJobState.schema,
    AnalysisTaskKind.AbstractiveSummarization -> AbstractiveSummarizationJobState.schema,
    AnalysisTaskKind.Healthcare -> HealthcareJobState.schema,
    AnalysisTaskKind.SentimentAnalysis -> SentimentAnalysisJobState.schema,
    AnalysisTaskKind.KeyPhraseExtraction -> KeyPhraseExtractionJobState.schema,
    AnalysisTaskKind.PiiEntityRecognition -> PiiEntityRecognitionJobState.schema,
    AnalysisTaskKind.EntityLinking -> EntityLinkingJobState.schema,
    AnalysisTaskKind.EntityRecognition -> EntityRecognitionJobState.schema,
    AnalysisTaskKind.CustomEntityRecognition -> EntityRecognitionJobState.schema,
    )

  override protected def responseDataType: DataType = {
    val taskKind = AnalysisTaskKind.getKindFromString(getKind)
    responseDataTypeSchemaMap(taskKind)
  }

  private val requestCreatorMap: Map[AnalysisTaskKind.AnalysisTaskKind,
    (Row, MultiLanguageAnalysisInput, String, String, Boolean) => String] = Map(
    AnalysisTaskKind.ExtractiveSummarization -> createExtractiveSummarizationRequest,
    AnalysisTaskKind.AbstractiveSummarization -> createAbstractiveSummarizationRequest,
    AnalysisTaskKind.Healthcare -> createHealthcareTextAnalyticsRequest,
    AnalysisTaskKind.SentimentAnalysis -> createSentimentAnalysisRequest,
    AnalysisTaskKind.KeyPhraseExtraction -> createKeyPhraseExtractionRequest,
    AnalysisTaskKind.PiiEntityRecognition -> createPiiEntityRecognitionRequest,
    AnalysisTaskKind.EntityLinking -> createEntityLinkingRequest,
    AnalysisTaskKind.EntityRecognition -> createEntityRecognitionRequest,
    AnalysisTaskKind.CustomEntityRecognition -> createCustomEntityRecognitionRequest
    )

  // This method is made package private for testing purposes
  override protected[language] def prepareEntity: Row => Option[AbstractHttpEntity] = row => {
    val analysisInput = createMultiLanguageAnalysisInput(row)
    val taskKind = AnalysisTaskKind.getKindFromString(getKind)
    val requestString = requestCreatorMap(taskKind)(row,
                                                    analysisInput,
                                                    getValue(row, modelVersion),
                                                    getValue(row, stringIndexType),
                                                    getValue(row, loggingOptOut))
    Some(new StringEntity(requestString, "UTF-8"))
  }

  protected def postprocessResponse(responseOpt: Row): Option[Seq[Row]] = {
    Option(responseOpt).map { response =>
      val tasks = response.getAs[Row]("tasks")
      val items = tasks.getAs[Seq[Row]]("items")
      items.flatMap(item => {
        val results = item.getAs[Row]("results")
        val stats = results.getAs[Row]("statistics")
        val docs = results.getAs[Seq[Row]]("documents").map(
          doc => (doc.getAs[String]("id"), doc)).toMap
        val errors = results.getAs[Seq[Row]]("errors").map(
          error => (error.getAs[String]("id"), error)).toMap
        val modelVersion = results.getAs[String]("modelVersion")
        (0 until (docs.size + errors.size)).map { i =>
          Row.fromSeq(Seq(
            stats,
            docs.get(i.toString),
            errors.get(i.toString),
            modelVersion
            ))
        }
      })
    }
  }

  protected def postprocessResponseUdf: UserDefinedFunction = {
    val responseType = responseDataType.asInstanceOf[StructType]
    val results = responseType("tasks").dataType.asInstanceOf[StructType]("items")
                                       .dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]("results")
                                       .dataType.asInstanceOf[StructType]
    val outputType = ArrayType(
      new StructType()
        .add("statistics", results("statistics").dataType)
        .add("documents", results("documents").dataType.asInstanceOf[ArrayType].elementType)
        .add("errors", results("errors").dataType.asInstanceOf[ArrayType].elementType)
        .add("modelVersion", results("modelVersion").dataType)
      )
    UDFUtils.oldUdf(postprocessResponse _, outputType)
  }

  override protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val batcher = if (shouldAutoBatch(schema)) {
      Some(new FixedMiniBatchTransformer().setBatchSize(getBatchSize))
    } else {
      None
    }
    val newSchema = batcher.map(_.transformSchema(schema)).getOrElse(schema)

    val pipe = super.getInternalTransformer(newSchema)

    val postprocess = new UDFTransformer()
      .setInputCol(getOutputCol)
      .setOutputCol(getOutputCol)
      .setUDF(postprocessResponseUdf)

    val flatten = if (shouldAutoBatch(schema)) {
      Some(new FlattenBatch())
    } else {
      None
    }

    NamespaceInjections.pipelineModel(
      Array(batcher, Some(pipe), Some(postprocess), flatten).flatten
      )
  }

  private def createMultiLanguageAnalysisInput(row: Row): MultiLanguageAnalysisInput = {
    val validText = getValue(row, text)
    val langs = getValueOpt(row, language).getOrElse(Seq.fill(validText.length)(""))
    val validLanguages = (if (langs.length == 1) {
      Seq.fill(validText.length)(langs.head)
    } else {
      langs
    }).map(lang => Option(lang).getOrElse(""))
    assert(validLanguages.length == validText.length)
    MultiLanguageAnalysisInput(validText.zipWithIndex.map { case (t, i) =>
      TADocument(Some(validLanguages(i)), i.toString, Option(t).getOrElse(""))
    })
  }
}
