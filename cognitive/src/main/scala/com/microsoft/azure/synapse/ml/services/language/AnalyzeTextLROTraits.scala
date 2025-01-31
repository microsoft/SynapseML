// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.language

import com.microsoft.azure.synapse.ml.io.http.{ EntityData, HTTPResponseData }
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.ServiceParam
import com.microsoft.azure.synapse.ml.services.HasServiceParams
import com.microsoft.azure.synapse.ml.services.language.ATLROJSONFormat._
import com.microsoft.azure.synapse.ml.services.language.PiiDomain.PiiDomain
import com.microsoft.azure.synapse.ml.services.language.SummaryLength.SummaryLength
import com.microsoft.azure.synapse.ml.services.vision.BasicAsyncReply
import org.apache.commons.io.IOUtils
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param.ParamValidators
import org.apache.spark.sql.Row
import spray.json.DefaultJsonProtocol._
import spray.json.enrichAny

import java.net.URI

object AnalysisTaskKind extends Enumeration {
  type AnalysisTaskKind = Value
  val SentimentAnalysis,
  EntityRecognition,
  PiiEntityRecognition,
  KeyPhraseExtraction,
  EntityLinking,
  Healthcare,
  CustomEntityRecognition,
  CustomSingleLabelClassification,
  CustomMultiLabelClassification,
  ExtractiveSummarization,
  AbstractiveSummarization = Value

  def getKindFromString(kind: String): AnalysisTaskKind = {
    AnalysisTaskKind.values.find(_.toString == kind).getOrElse(
      throw new IllegalArgumentException(s"Invalid kind: $kind")
      )
  }
}

trait HasSummarizationBaseParameter extends HasServiceParams {
  val sentenceCount = new ServiceParam[Int](
    this,
    name = "sentenceCount",
    doc = "Specifies the number of sentences in the extracted summary.",
    isValid = { case Left(value) => value >= 1 case Right(_) => true }
    )

  def getSentenceCount: Int = getScalarParam(sentenceCount)

  def setSentenceCount(value: Int): this.type = setScalarParam(sentenceCount, value)

  def getSentenceCountCol: String = getVectorParam(sentenceCount)

  def setSentenceCountCol(value: String): this.type = setVectorParam(sentenceCount, value)
}

/**
 * This trait is used to handle the extractive summarization request. It provides the necessary
 * parameters to create the request and the method to create the request. There are two
 * parameters for extractive summarization: sentenceCount and sortBy. Both of them are optional.
 * If the user does not provide any value for sentenceCount, the service will return the default
 * number of sentences in the summary. If the user does not provide any value for sortBy, the
 * service will return the summary in the order of the sentences in the input text. The possible values
 * for sortBy are "Rank" and "Offset". If the user provides an invalid value for sortBy, the service
 * will return an error. SentenceCount is an integer value, and it should be greater than 0. This parameter
 * specifies the number of sentences in the extracted summary. If the user provides an invalid value for
 * sentenceCount, the service will return an error. For more details about the parameters, please refer to
 * the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/summarization/overview]]
 */
trait HandleExtractiveSummarization extends HasServiceParams
                                            with HasSummarizationBaseParameter {
  val sortBy = new ServiceParam[String](
    this,
    name = "sortBy",
    doc = "Specifies how to sort the extracted summaries. This can be either 'Rank' or 'Offset'.",
    isValid = {
      case Left(value) => ParamValidators.inArray(Array("Rank", "Offset"))(value)
      case Right(_) => true
    })

  def getSortBy: String = getScalarParam(sortBy)

  def setSortBy(value: String): this.type = setScalarParam(sortBy, value)

  def getSortByCol: String = getVectorParam(sortBy)

  def setSortByCol(value: String): this.type = setVectorParam(sortBy, value)

  def createExtractiveSummarizationRequest(row: Row,
                                           analysisInput: MultiLanguageAnalysisInput,
                                           modelVersion: String,
                                           stringIndexType: String,
                                           loggingOptOut: Boolean): String = {
    val taskParameter = ExtractiveSummarizationLROTask(
      parameters = ExtractiveSummarizationTaskParameters(
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        sentenceCount = getValueOpt(row, sentenceCount),
        sortBy = getValueOpt(row, sortBy),
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.ExtractiveSummarization.toString
      )

    ExtractiveSummarizationJobsInput(displayName = None,
                                     analysisInput = analysisInput,
                                     tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the abstractive summarization request. It provides the necessary
 * parameters to create the request and the method to create the request. There are two
 * parameters for abstractive summarization: sentenceCount and summaryLength. Both of them are optional.
 * It is recommended to use summaryLength over sentenceCount. Service may ignore sentenceCount parameter.
 * SummaryLength is a string value, and it should be one of "short", "medium", or "long". This parameter
 * controls the approximate length of the output summaries. If the user provides an invalid value for
 * summaryLength, the service will return an error. For more details about the parameters, please refer to
 * the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/summarization/overview]]
 */
trait HandleAbstractiveSummarization extends HasServiceParams with HasSummarizationBaseParameter {
  val summaryLength = new ServiceParam[String](
    this,
    name = "summaryLength",
    doc = "(NOTE: Recommended to use summaryLength over sentenceCount) Controls the"
      + " approximate length of the output summaries.",
    isValid = {
      case Left(value) => ParamValidators.inArray(Array("short", "medium", "long"))(value)
      case Right(_) => true
    }

    )

  def getSummaryLength: String = getScalarParam(summaryLength)

  def setSummaryLength(value: String): this.type = setScalarParam(summaryLength, value)

  def setSummaryLength(value: SummaryLength): this.type = setScalarParam(summaryLength, value.toString.toLowerCase)

  def getSummaryLengthCol: String = getVectorParam(summaryLength)

  def setSummaryLengthCol(value: String): this.type = setVectorParam(summaryLength, value)

  def createAbstractiveSummarizationRequest(row: Row,
                                            analysisInput: MultiLanguageAnalysisInput,
                                            modelVersion: String,
                                            stringIndexType: String,
                                            loggingOptOut: Boolean): String = {
    val paramerter = AbstractiveSummarizationLROTask(
      parameters = AbstractiveSummarizationTaskParameters(
        sentenceCount = getValueOpt(row, sentenceCount),
        summaryLength = getValueOpt(row, summaryLength),
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        stringIndexType = stringIndexType),
      taskName = None,
      kind = AnalysisTaskKind.AbstractiveSummarization.toString
      )
    AbstractiveSummarizationJobsInput(displayName = None,
                                      analysisInput = analysisInput,
                                      tasks = Seq(paramerter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the healthcare text analytics request. It provides the necessary parameters
 * to create the request and the method to create the request. There are three parameters for healthcare text
 * analytics: modelVersion, stringIndexType, and loggingOptOut. All of them are optional. For more details about
 * the parameters, please refer to the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/text-analytics-for-health/overview]]
 */
trait HandleHealthcareTextAnalystics extends HasServiceParams {
  def createHealthcareTextAnalyticsRequest(row: Row,
                                           analysisInput: MultiLanguageAnalysisInput,
                                           modelVersion: String,
                                           stringIndexType: String,
                                           loggingOptOut: Boolean): String = {
    val taskParameter = HealthcareLROTask(
      parameters = HealthcareTaskParameters(
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.Healthcare.toString
      )
    HealthcareJobsInput(displayName = None,
                        analysisInput = analysisInput,
                        tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the text analytics request. It provides the necessary parameters to create
 * the request and the method to create the request. There are three parameters for text analytics: modelVersion,
 * stringIndexType, and loggingOptOut. All of them are optional. For more details about the parameters, please refer
 * to the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/sentiment-opinion-mining/overview]]
 */
trait HandleSentimentAnalysis extends HasServiceParams {
  val opinionMining = new ServiceParam[Boolean](
    this,
    name = "opinionMining",
    doc = "Whether to use opinion mining in the request or not."
    )

  def getOpinionMining: Boolean = getScalarParam(opinionMining)

  def setOpinionMining(value: Boolean): this.type = setScalarParam(opinionMining, value)

  def getOpinionMiningCol: String = getVectorParam(opinionMining)

  def setOpinionMiningCol(value: String): this.type = setVectorParam(opinionMining, value)

  setDefault(
    opinionMining -> Left(false)
    )

  def createSentimentAnalysisRequest(row: Row,
                                     analysisInput: MultiLanguageAnalysisInput,
                                     modelVersion: String,
                                     stringIndexType: String,
                                     loggingOptOut: Boolean): String = {
    val taskParameter = SentimentAnalysisLROTask(
      parameters = SentimentAnalysisTaskParameters(
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        opinionMining = getValue(row, opinionMining),
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.SentimentAnalysis.toString
      )
    SentimentAnalysisJobsInput(displayName = None,
                               analysisInput = analysisInput,
                               tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the key phrase extraction request. It provides the necessary parameters to create
 * the request and the method to create the request. There are two parameters for key phrase extraction: modelVersion
 * and loggingOptOut. Both of them are optional. For more details about the parameters,
 * please refer to the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/key-phrase-extraction/overview]]
 */
trait HandleKeyPhraseExtraction extends HasServiceParams {
  def createKeyPhraseExtractionRequest(row: Row,
                                       analysisInput: MultiLanguageAnalysisInput,
                                       modelVersion: String,
                                       // This parameter is not used and only exists for compatibility
                                       stringIndexType: String,
                                       loggingOptOut: Boolean): String = {
    val taskParameter = KeyPhraseExtractionLROTask(
      parameters = KPnLDTaskParameters(
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion
        ),
      taskName = None,
      kind = AnalysisTaskKind.KeyPhraseExtraction.toString
      )
    KeyPhraseExtractionJobsInput(displayName = None,
                                 analysisInput = analysisInput,
                                 tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

trait HandleEntityLinking extends HasServiceParams {
  def createEntityLinkingRequest(row: Row,
                                 analysisInput: MultiLanguageAnalysisInput,
                                 modelVersion: String,
                                 stringIndexType: String,
                                 loggingOptOut: Boolean): String = {
    val taskParameter = EntityLinkingLROTask(
      parameters = EntityTaskParameters(
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.EntityLinking.toString
      )
    EntityLinkingJobsInput(displayName = None,
                           analysisInput = analysisInput,
                           tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the PII entity recognition request. It provides the necessary parameters to create
 * the request and the method to create the request. There are three parameters for PII entity recognition: domain,
 * piiCategories, and loggingOptOut. All of them are optional. For more details about the parameters, please refer to
 * the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/personally-identifiable-information/overview]]
 */
trait HandlePiiEntityRecognition extends HasServiceParams {
  val domain = new ServiceParam[String](
    this,
    name = "domain",
    doc = "The domain of the PII entity recognition request.",
    isValid = {
      case Left(value) => PiiDomain.values.map(_.toString.toLowerCase).contains(value)
      case Right(_) => true
    }
    )

  def getDomain: String = getScalarParam(domain)

  def setDomain(value: String): this.type = setScalarParam(domain, value)

  def setDomain(value: PiiDomain): this.type = setScalarParam(domain, value.toString.toLowerCase)

  def getDomainCol: String = getVectorParam(domain)

  def setDomainCol(value: String): this.type = setVectorParam(domain, value)

  val piiCategories = new ServiceParam[Seq[String]](this, "piiCategories",
                                                    "describes the PII categories to return")

  def setPiiCategories(v: Seq[String]): this.type = setScalarParam(piiCategories, v)

  def getPiiCategories: Seq[String] = getScalarParam(piiCategories)

  def setPiiCategoriesCol(v: String): this.type = setVectorParam(piiCategories, v)

  def getPiiCategoriesCol: String = getVectorParam(piiCategories)

  setDefault(
    domain -> Left("none")
    )

  def createPiiEntityRecognitionRequest(row: Row,
                                        analysisInput: MultiLanguageAnalysisInput,
                                        modelVersion: String,
                                        stringIndexType: String,
                                        loggingOptOut: Boolean): String = {
    val taskParameter = PiiEntityRecognitionLROTask(
      parameters = PiiTaskParameters(
        domain = getValue(row, domain),
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        piiCategories = getValueOpt(row, piiCategories),
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.PiiEntityRecognition.toString
      )
    PiiEntityRecognitionJobsInput(displayName = None,
                                  analysisInput = analysisInput,
                                  tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * This trait is used to handle the entity recognition request. It provides the necessary parameters to create
 * the request and the method to create the request. There are five parameters for entity recognition: inclusionList,
 * exclusionList, overlapPolicy, excludeNormalizedValues, and loggingOptOut. All of them are optional. For more details
 * about the parameters, please refer to the documentation.
 * [[https://learn.microsoft.com/en-us/azure/ai-services/language-service/named-entity-recognition/overview]]
 */
trait HandleEntityRecognition extends HasServiceParams {
  val inclusionList = new ServiceParam[Seq[String]](
    this,
    name = "inclusionList",
    doc = "(Optional) request parameter that limits the output to the requested entity"
      + " types included in this list. We will apply inclusionList before"
      + " exclusionList"
    )

  def getInclusionList: Seq[String] = getScalarParam(inclusionList)

  def setInclusionList(value: Seq[String]): this.type = setScalarParam(inclusionList, value)

  def getInclusionListCol: String = getVectorParam(inclusionList)

  def setInclusionListCol(value: String): this.type = setVectorParam(inclusionList, value)

  val exclusionList = new ServiceParam[Seq[String]](
    this,
    name = "exclusionList",
    doc = "(Optional) request parameter that filters out any entities that are"
      + " included the excludeList. When a user specifies an excludeList, they cannot"
      + " get a prediction returned with an entity in that list. We will apply"
      + " inclusionList before exclusionList"
    )

  def getExclusionList: Seq[String] = getScalarParam(exclusionList)

  def setExclusionList(value: Seq[String]): this.type = setScalarParam(exclusionList, value)

  def getExclusionListCol: String = getVectorParam(exclusionList)

  def setExclusionListCol(value: String): this.type = setVectorParam(exclusionList, value)

  val overlapPolicy = new ServiceParam[String](
    this,
    name = "overlapPolicy",
    doc = "(Optional) describes the type of overlap policy to apply to the ner output.",
    isValid = {
      case Left(value) => value == "matchLongest" || value == "allowOverlap"
      case Right(_) => true
    })

  def getOverlapPolicy: String = getScalarParam(overlapPolicy)

  def setOverlapPolicy(value: String): this.type = setScalarParam(overlapPolicy, value)

  def getOverlapPolicyCol: String = getVectorParam(overlapPolicy)

  def setOverlapPolicyCol(value: String): this.type = setVectorParam(overlapPolicy, value)

  val excludeNormalizedValues = new ServiceParam[Boolean](
    this,
    name = "excludeNormalizedValues",
    doc = "(Optional) request parameter that allows the user to provide settings for"
      + " running the inference. If set to true, the service will exclude normalized"
    )

  def getExcludeNormalizedValues: Boolean = getScalarParam(excludeNormalizedValues)

  def setExcludeNormalizedValues(value: Boolean): this.type = setScalarParam(excludeNormalizedValues, value)

  def getExcludeNormalizedValuesCol: String = getVectorParam(excludeNormalizedValues)

  def setexcludeNormalizedValuesCol(value: String): this.type = setVectorParam(excludeNormalizedValues, value)

  def createEntityRecognitionRequest(row: Row,
                                     analysisInput: MultiLanguageAnalysisInput,
                                     modelVersion: String,
                                     stringIndexType: String,
                                     loggingOptOut: Boolean): String = {
    val serviceOverlapPolicy: Option[EntityOverlapPolicy] = getValueOpt(row, overlapPolicy) match {
      case Some(policy) => Some(new EntityOverlapPolicy(policy))
      case None => None
    }

    val inferenceOptions: Option[EntityInferenceOptions] = getValueOpt(row, excludeNormalizedValues) match {
      case Some(value) => Some(new EntityInferenceOptions(value))
      case None => None
    }
    val taskParameter = EntityRecognitionLROTask(
      parameters = EntityRecognitionTaskParameters(
        exclusionList = getValueOpt(row, exclusionList),
        inclusionList = getValueOpt(row, inclusionList),
        loggingOptOut = loggingOptOut,
        modelVersion = modelVersion,
        overlapPolicy = serviceOverlapPolicy,
        stringIndexType = stringIndexType,
        inferenceOptions = inferenceOptions
        ),
      taskName = None,
      kind = AnalysisTaskKind.EntityRecognition.toString
    )
    EntityRecognitionJobsInput(displayName = None,
                               analysisInput = analysisInput,
                               tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

trait HasCustomLanguageModelParam extends HasServiceParams {
  val projectName = new ServiceParam[String](
    this,
    name = "projectName",
    doc = "This field indicates the project name for the model. This is a required field"
    )

  def getProjectName: String = getScalarParam(projectName)

  def setProjectName(value: String): this.type = setScalarParam(projectName, value)

  def getProjectNameCol: String = getVectorParam(projectName)

  def setProjectNameCol(value: String): this.type = setVectorParam(projectName, value)

  val deploymentName = new ServiceParam[String](
    this,
    name = "deploymentName",
    doc = "This field indicates the deployment name for the model. This is a required field."
    )

  def getDeploymentName: String = getScalarParam(deploymentName)

  def setDeploymentName(value: String): this.type = setScalarParam(deploymentName, value)

  def getDeploymentNameCol: String = getVectorParam(deploymentName)

  def setDeploymentNameCol(value: String): this.type = setVectorParam(deploymentName, value)
}

trait HandleCustomEntityRecognition extends HasServiceParams
                                            with HasCustomLanguageModelParam {

  def createCustomEntityRecognitionRequest(row: Row,
                                           analysisInput: MultiLanguageAnalysisInput,
                                           // This paremeter is not used and only exists for compatibility
                                           modelVersion: String,
                                           stringIndexType: String,
                                           loggingOptOut: Boolean): String = {
    val taskParameter = CustomEntityRecognitionLROTask(
      parameters = CustomEntitiesTaskParameters(
        loggingOptOut = loggingOptOut,
        projectName = getValue(row, projectName),
        deploymentName = getValue(row, deploymentName),
        stringIndexType = stringIndexType
        ),
      taskName = None,
      kind = AnalysisTaskKind.CustomEntityRecognition.toString)
    CustomEntitiesJobsInput(displayName = None,
                            analysisInput = analysisInput,
                            tasks = Seq(taskParameter)).toJson.compactPrint
  }
}

/**
 * Trait `ModifiableAsyncReply` extends `BasicAsyncReply` and provides a mechanism to modify the HTTP response
 * received from an asynchronous service call. This trait is designed to be mixed into classes that require
 * custom handling of the response data.
 *
 * The primary purpose of this trait is to allow modification of the response before it is processed further.
 * This is particularly useful in scenarios where the response needs to be transformed or certain fields need
 * to be renamed to comply with specific requirements or constraints.
 *
 * In this implementation, the `queryForResult` method is overridden and marked as `final` to prevent further
 * overriding. This ensures that the response modification logic is consistently applied across all subclasses.
 *
 * @note This trait is designed to be used with the `SynapseMLLogging` trait for consistent logging.
 */
trait ModifiableAsyncReply extends BasicAsyncReply {
  self: SynapseMLLogging =>

  protected def modifyResponse(response: Option[HTTPResponseData]): Option[HTTPResponseData] = response

  /**
   * Queries for the result of an asynchronous service call and applies the response modification logic.
   */
  override final protected def queryForResult(key: Option[String],
                                              client: CloseableHttpClient,
                                              location: URI): Option[HTTPResponseData] = {
    val originalResponse = super.queryForResult(key, client, location)
    logDebug(s"Original response: ${ originalResponse }")
    modifyResponse(originalResponse)
  }
}


/**
 * Trait `HandleCustomLabelClassification` extends `HasServiceParams` and `HasCustomLanguageModelParam` to handle
 * custom label classification tasks. This trait provides the necessary methods to create requests for custom
 * multi-label classification and to modify the response to comply with specific requirements.
 *
 * The primary purpose of this trait is to address the limitation in Spark where fields named "class" cannot be
 * directly bound. To work around this limitation, the response is modified to rename the "class" field to
 * "classifications".
 *
 * This trait is designed to be mixed into classes that require custom label classification functionality and
 * response modification logic.
 *
 * @note This trait is designed to be used with the `ModifiableAsyncReply` and `SynapseMLLogging` traits for
 *       consistent response handling and logging.
 */
trait HandleCustomLabelClassification extends HasServiceParams
                                              with HasCustomLanguageModelParam {
  self: ModifiableAsyncReply
    with SynapseMLLogging =>

  private def isCustomLabelClassification: Boolean = {
    val kind = getKind
    kind == AnalysisTaskKind.CustomSingleLabelClassification.toString ||
      kind == AnalysisTaskKind.CustomMultiLabelClassification.toString
  }

  /**
   * Modifies the entity in the HTTP response to rename the "class" field to "classifications".
   *
   * @param response The original HTTP response.
   * @return The modified HTTP response with the "class" field renamed to "classifications".
   */
  private def modifyEntity(response: HTTPResponseData): HTTPResponseData = {
    val modifiedEntity = response.entity.flatMap { entity =>
      val strEntity = IOUtils.toString(entity.content, "UTF-8")
      val modifiedEntity = strEntity.replace("\"class\":", "\"classifications\":")
      logDebug(s"Original entity: $strEntity\t Modified entity: $modifiedEntity")
      Some(new EntityData(
        content = modifiedEntity.getBytes,
        contentEncoding = entity.contentEncoding,
        contentLength = Some(strEntity.length),
        contentType = entity.contentType,
        isChunked = entity.isChunked,
        isRepeatable = entity.isRepeatable,
        isStreaming = entity.isStreaming
        ))
    }
    new HTTPResponseData(response.headers, modifiedEntity, response.statusLine, response.locale)
  }

  /**
   * Modifies the HTTP response if the task kind is custom label classification.
   */
  override def modifyResponse(response: Option[HTTPResponseData]): Option[HTTPResponseData] = {
    if (!isCustomLabelClassification) {
      logDebug(s"Kind is not CustomSingleLabelClassification or CustomMultiLabelClassification. Kind: $getKind")
      response
    } else {
      response.map(modifyEntity)
    }
  }


  def getKind: String

  def createCustomMultiLabelRequest(row: Row,
                                    analysisInput: MultiLanguageAnalysisInput,
                                    // This paremeter is not used and only exists for compatibility
                                    modelVersion: String,
                                    // This paremeter is not used and only exists for compatibility
                                    stringIndexType: String,
                                    loggingOptOut: Boolean): String = {
    val taskParameter = CustomLabelLROTask(
      parameters = CustomLabelTaskParameters(
        loggingOptOut = loggingOptOut,
        projectName = getValue(row, projectName),
        deploymentName = getValue(row, deploymentName)
        ),
      taskName = None,
      kind = getKind
      )
    CustomLabelJobsInput(displayName = None,
                         analysisInput = analysisInput,
                         tasks = Seq(taskParameter)).toJson.compactPrint
  }
}
