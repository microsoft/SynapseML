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

  val text: ServiceParam[String] = new ServiceParam[String](
    this, "text", "Input text to get embeddings for.", isRequired = true)

  def getText: String = getScalarParam(text)

  def setText(value: String): this.type = setScalarParam(text, value)

  def getTextCol: String = getVectorParam(text)

  def setTextCol(value: String): this.type = setVectorParam(text, value)

  override protected def getInternalOutputParser(schema: StructType): JSONOutputParser = {
    def responseToVector(r: Row) =
      if (r == null)
        None
      else
        Some(Vectors.dense(r.getAs[Seq[Row]]("data").head.getAs[Seq[Double]]("embedding").toArray))

    new JSONOutputParser()
      .setDataType(EmbeddingResponse.schema)
      .setPostProcessFunc[Row, Option[Vector]](responseToVector, VectorType)
  }

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    // Resolve deployment name with strict precedence:
    // 1) explicit instance/row value
    // 2) global embedding deployment name
    // 3) global (general) deployment name
    // We never override an explicit per-instance deployment.
    val instanceDep = getValueOpt(row, deploymentName)
    val instanceParamSetting = get(deploymentName) // Left(scalar) or Right(colName) if set on instance
    val globalEmbeddingDeployment =
      GlobalParams.getGlobalParam(OpenAIEmbeddingDeploymentNameKey).flatMap(_.left.toOption)
    val globalDeployment =
      GlobalParams.getGlobalParam(OpenAIDeploymentNameKey).flatMap(_.left.toOption)

    val dep = if (instanceParamSetting.exists(_.isRight)) {
      // Explicit per-row column provided; always honor it
      instanceDep.get
    } else if (instanceParamSetting.exists(_.isLeft) && this.isDeploymentNameExplicitlySet && instanceDep.nonEmpty) {
      // Scalar set explicitly on instance; honor it
      instanceDep.get
    } else {
      // Either unset on instance or populated from global default; prefer embedding-global, then instance/global
      globalEmbeddingDeployment
        .orElse(instanceDep)
        .orElse(globalDeployment)
        .getOrElse(throw new IllegalArgumentException("Please set deploymentName parameter or global defaults"))
    }

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
      .orElse(throw new IllegalArgumentException("Please set textCol."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

}
