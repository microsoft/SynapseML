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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.BooleanParam
import org.apache.spark.ml.util._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import scala.language.existentials
import org.apache.spark.sql.functions.{col, lit, map => sqlMap, struct, typedLit, udf, when}
import spray.json.DefaultJsonProtocol._
import spray.json._

object OpenAIEmbedding extends ComplexParamsReadable[OpenAIEmbedding]

class OpenAIEmbedding (override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAIEmbeddingParams with HasCognitiveServiceInput with SynapseMLLogging {
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

  val returnUsage = new BooleanParam(
    this, "returnUsage", "Whether to include embedding usage statistics alongside the vector output.")

  def getReturnUsage: Boolean = $(returnUsage)

  def setReturnUsage(value: Boolean): this.type = set(returnUsage, value)

  setDefault(returnUsage -> false)

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

  private val usageMapType: MapType = MapType(StringType, LongType, valueContainsNull = false)

  private val extractVectorUDF = udf { row: Row =>
    Option(row)
      .flatMap(r => Option(r.getAs[Seq[Row]]("data")))
      .flatMap(_.headOption)
      .map(dataRow => Vectors.dense(dataRow.getAs[Seq[Double]]("embedding").toArray))
      .orNull // scalastyle:ignore null
  }

  private def usageMapColumn(usageStruct: Column): Column = {
    val promptTokens = usageStruct.getField("prompt_tokens").cast(LongType)
    val totalTokens = usageStruct.getField("total_tokens").cast(LongType)
    val mapExpr = sqlMap(
      lit("prompt_tokens"), promptTokens,
      lit("total_tokens"), totalTokens
    )
    when(usageStruct.isNotNull, mapExpr).otherwise(typedLit(Map.empty[String, Long]))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val parsed = super.transform(dataset)
    val responseCol = col(getOutputCol)
    val vectorCol = extractVectorUDF(responseCol)
    if (getReturnUsage) {
      val usageCol = usageMapColumn(responseCol.getField("usage"))
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
          StructField("usage", usageMapType, nullable = true)
        )),
        nullable = true
      )
    } else {
      StructField(getOutputCol, VectorType, nullable = true)
    }
    StructType(fieldsWithoutOutput :+ outputField)
  }
}
