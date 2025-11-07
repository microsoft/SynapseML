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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.language.existentials

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

  override protected def getInternalOutputParser(schema: StructType): JSONOutputParser = {
    def extractVector(row: Row): Option[Vector] = {
      Option(row)
        .flatMap(r => Option(r.getAs[Seq[Row]]("data")))
        .flatMap(_.headOption)
        .map(dataRow => Vectors.dense(dataRow.getAs[Seq[Double]]("embedding").toArray))
    }

    val parser = new JSONOutputParser()
      .setDataType(EmbeddingResponse.schema)

    if (getReturnUsage) {
      parser.setPostProcessFunc[Row, String]({ r: Row =>
        if (r == null) {
          JsNull.compactPrint
        } else {
          val vectorOpt = extractVector(r)
          val usageMapOpt = Option(r.getAs[Row]("usage")).map { usageRow =>
            Map(
              "prompt_tokens" -> usageRow.getAs[Long]("prompt_tokens"),
              "total_tokens" -> usageRow.getAs[Long]("total_tokens")
            )
          }

          val responseJson = vectorOpt match {
            case Some(vec) => vec.toArray.toSeq.toJson
            case None => JsNull
          }

          val usageJson: JsValue = usageMapOpt.map(_.toJson).getOrElse(JsNull)

          JsObject("response" -> responseJson, "usage" -> usageJson).compactPrint
        }
      }, StringType)
    } else {
      parser.setPostProcessFunc[Row, Option[Vector]](extractVector, VectorType)
    }

    parser
  }

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

}
