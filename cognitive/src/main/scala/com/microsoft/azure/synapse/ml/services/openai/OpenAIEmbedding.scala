// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.services.{CognitiveServicesBase, HasCognitiveServiceInput, HasServiceParams}
import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.io.http.JSONOutputParser
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.language.existentials

object OpenAIEmbedding extends ComplexParamsReadable[OpenAIEmbedding]

class OpenAIEmbedding (override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAISharedParams with HasCognitiveServiceInput  with SynapseMLLogging {
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
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/embeddings"
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, text)
        .map(text => new StringEntity(Map("input" -> text).toJson.compactPrint, ContentType.APPLICATION_JSON))
      .orElse(throw new IllegalArgumentException("Please set textCol."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

}
