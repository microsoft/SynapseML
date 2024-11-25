// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import com.microsoft.azure.synapse.ml.services.{HasCognitiveServiceInput, HasInternalJsonOutputParser}
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.language.existentials

object OpenAIChatCompletion extends ComplexParamsReadable[OpenAIChatCompletion]

class OpenAIChatCompletion(override val uid: String) extends OpenAIServicesBase(uid)
  with HasOpenAITextParams with HasMessagesInput with HasCognitiveServiceInput
  with HasInternalJsonOutputParser with SynapseMLLogging {
  logClass(FeatureNames.AiServices.OpenAI)

  def this() = this(Identifiable.randomUID("OpenAIChatCompletion"))

  def urlPath: String = ""

  override private[ml] def internalServiceType: String = "openai"

  override def setCustomServiceName(v: String): this.type = {
    setUrl(s"https://$v.openai.azure.com/" + urlPath.stripPrefix("/"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    s"${getUrl}openai/deployments/${getValue(row, deploymentName)}/chat/completions"
  }

  override protected[openai] def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      lazy val optionalParams: Map[String, Any] = getOptionalParams(r)
      val messages = r.getAs[Seq[Row]](getMessagesCol)
      Some(getStringEntity(messages, optionalParams))
  }

  override val subscriptionKeyHeaderName: String = "api-key"

  /**
   * Check if the row should be skipped. A row should be skipped if the messages column is empty or if any of the
   * messages are empty or if there not exists a single message with "role" as "user" and "content" as non-empty.
   */
  override def shouldSkip(row: Row): Boolean ={
    super.shouldSkip(row) || {
      val messages = Option(row.getAs[Seq[Row]](getMessagesCol)).getOrElse(Seq.empty)
      messages.isEmpty || !messages
        // filter out messages system messages
        .filter(m => Option(m.getAs[String]("role")).getOrElse("").equalsIgnoreCase("user"))
        // check if there exists a user message with non-empty content
        .exists(m => Option(m.getAs[String]("content")).getOrElse("").nonEmpty)
    }
  }

  override protected def getVectorParamMap: Map[String, String] = super.getVectorParamMap
    .updated("messages", getMessagesCol)

  override def responseDataType: DataType = ChatCompletionResponse.schema

  private[this] def getStringEntity(messages: Seq[Row], optionalParams: Map[String, Any]): StringEntity = {
    val mappedMessages: Seq[Map[String, String]] = messages.map { m =>
      Seq("role", "content", "name").map(n =>
        n -> Option(m.getAs[String](n))
      ).toMap.filter(_._2.isDefined).mapValues(_.get)
    }
    val fullPayload = optionalParams.updated("messages", mappedMessages)
    new StringEntity(fullPayload.toJson.compactPrint, ContentType.APPLICATION_JSON)
  }

}



