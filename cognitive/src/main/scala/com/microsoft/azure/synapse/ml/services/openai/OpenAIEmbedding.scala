// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.io.http.JSONOutputParser
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.{GlobalParams, ServiceParam}
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.util._
import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import scala.language.existentials
import org.apache.spark.sql.functions.{col, element_at, struct}
import spray.json.DefaultJsonProtocol._
import spray.json._
import HasReturnUsage.UsageFieldMapping

object OpenAIEmbedding extends ComplexParamsReadable[OpenAIEmbedding]

class OpenAIEmbedding (override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAIEmbeddingParams with HasCognitiveServiceInput with SynapseMLLogging
  with HasReturnUsage {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIEmbedding"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  // Default API version aligned with Responses and Chat Completions
  setDefault(apiVersion -> Left("2025-04-01-preview"))

  val text: ServiceParam[String] = new ServiceParam[String](
    this, "text", "Input text to get embeddings for.", isRequired = true)

  def getText: String = getScalarParam(text)

  def setText(value: String): this.type = setScalarParam(text, value)

  def getTextCol: String = getVectorParam(text)

  def setTextCol(value: String): this.type = setVectorParam(text, value)

  override protected def getInternalOutputParser(schema: StructType): JSONOutputParser =
    new JSONOutputParser().setDataType(EmbeddingResponse.schema)

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    val globalEmbeddingDeployment =
      GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey).flatMap(_.left.toOption)

    val dep = globalEmbeddingDeployment.orElse {
      // If embedding-specific deployment is not set, check instance param
      if (isSet(deploymentName)) {
        getValueOpt(row, deploymentName)
      } else {
        None
      }
    }.getOrElse(throw new IllegalArgumentException(
      "No embedding deployment name provided. Set the 'deploymentName' param or call " +
      "OpenAIDefaults.setEmbeddingDeploymentName('<your-embedding-deployment>') to set a global default."))

    s"${getUrl}openai/deployments/$dep/embeddings"
  }

  private[this] def getStringEntity[A](text: A, optionalParams: Map[String, Any]): StringEntity = {
    val fullPayload = optionalParams.updated("input", text)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      getValueOpt(r, text)
        .map(text => getStringEntity(text, optionalParams))
        .orElse(throw new IllegalArgumentException(
          "No input text provided. Set the 'text' param or 'textCol' with the column containing input text."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  override def transform(dataset: Dataset[_]): DataFrame = {
    val parsed = super.transform(dataset)
    val responseCol = col(getOutputCol)
    val embeddingArrayCol = element_at(responseCol.getField("data"), 1).getField("embedding")
    val vectorCol = array_to_vector(embeddingArrayCol)
    if (getReturnUsage) {
      val usageCol = normalizeUsageColumn(
        responseCol.getField("usage"),
        UsageFieldMapping(
          inputTokens = Some("prompt_tokens"),
          outputTokens = None,
          totalTokens = Some("total_tokens"),
          inputDetails = None,
          outputDetails = None
        )
      )
      parsed.withColumn(
        getOutputCol,
        struct(
          vectorCol.alias("response"),
          usageCol.alias("usage")
        )
      )
    } else {
      parsed.withColumn(getOutputCol, vectorCol)
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    val baseSchema = super.transformSchema(schema)
    val fieldsWithoutOutput = baseSchema.fields.filterNot(_.name == getOutputCol)
    val outputField = if (getReturnUsage) {
      StructField(
        getOutputCol,
        StructType(Seq(
          StructField("response", VectorType, nullable = true),
          StructField("usage", usageStructType, nullable = true)
        )),
        nullable = true
      )
    } else {
      StructField(getOutputCol, VectorType, nullable = true)
    }
    StructType(fieldsWithoutOutput :+ outputField)
  }
}
