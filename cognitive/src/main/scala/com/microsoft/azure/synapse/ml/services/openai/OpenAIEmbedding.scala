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
import org.apache.spark.sql.functions.{col, element_at, struct, when}
import spray.json.DefaultJsonProtocol._
import spray.json._
import UsageUtils.{UsageFieldMapping, UsageMappings, UsageStructType}
import org.apache.spark.ml.param.Param

object OpenAIEmbedding extends ComplexParamsReadable[OpenAIEmbedding]

class OpenAIEmbedding (override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAIEmbeddingParams with HasCognitiveServiceInput with SynapseMLLogging {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIEmbedding"))

  val usageCol: Param[String] = new Param[String](
    this, "usageCol",
    "Column to hold usage statistics. Set this parameter to enable usage tracking.")

  def getUsageCol: String = $(usageCol)

  def setUsageCol(value: String): this.type = set(usageCol, value)

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
    val serviceOutputCol = getOutputCol
    val tempCol = s"__${serviceOutputCol}_response"

    val withTemp = parsed.withColumnRenamed(serviceOutputCol, tempCol)
    val response = col(tempCol)

    val embedding = element_at(response.getField("data"), 1).getField("embedding")
    var result = withTemp.withColumn(serviceOutputCol, when(response.isNotNull, array_to_vector(embedding)))

    if (isSet(usageCol)) {
      val usage = UsageUtils.normalize(response.getField("usage"), UsageMappings.Embeddings)
      result = result.withColumn(getUsageCol, when(response.isNotNull, usage))
    }

    result.drop(tempCol)
  }

  override def transformSchema(schema: StructType): StructType = {
    val baseSchema = super.transformSchema(schema)
    val fieldsWithoutOutput = baseSchema.fields.filterNot(_.name == getOutputCol)

    val outputField = StructField(getOutputCol, VectorType, nullable = true)
    var resultSchema = StructType(fieldsWithoutOutput :+ outputField)

    if (isSet(usageCol)) {
      resultSchema = resultSchema.add(getUsageCol, UsageStructType)
    }

    resultSchema
  }
}
