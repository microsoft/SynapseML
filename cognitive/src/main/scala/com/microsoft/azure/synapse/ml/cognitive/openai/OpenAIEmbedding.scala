// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.openai

import com.microsoft.azure.synapse.ml.cognitive.{CognitiveServicesBase, HasCognitiveServiceInput, HasServiceParams}
import com.microsoft.azure.synapse.ml.core.contracts.HasInputCol
import com.microsoft.azure.synapse.ml.io.http.JSONOutputParser
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
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

class OpenAIEmbedding (override val uid: String) extends CognitiveServicesBase(uid)
  with HasServiceParams with HasAPIVersion with HasDeploymentName
  with HasCognitiveServiceInput  with SynapseMLLogging {
  logClass()

  def this() = this(Identifiable.randomUID("OpenAIEmbedding"))

  def urlPath: String = ""

  val inputCol: ServiceParam[String] = new ServiceParam[String](
    this, "inputCol", "Input text to get embeddings for.", isRequired = true)

  def getInputCol: String = getVectorParam(inputCol)

  def setInputCol(value: String): this.type = setVectorParam(inputCol, value)

  override protected def getInternalOutputParser(schema: StructType): JSONOutputParser = {
    def responseToVector(r: Row) =
      if (r == null)
        Vectors.zeros(0)
      else
        Vectors.dense(r.getAs[Seq[Row]]("data").head.getAs[Seq[Double]]("embedding").toArray)

    new JSONOutputParser()
      .setDataType(EmbeddingResponse.schema)
      .setPostProcessFunc[Row, Vector](responseToVector, VectorType)
  }

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/embeddings"
  }

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, inputCol)
        .map(input => new StringEntity(Map("input" -> input).toJson.compactPrint, ContentType.APPLICATION_JSON))
      .orElse(throw new IllegalArgumentException("Please set inputCol."))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

}