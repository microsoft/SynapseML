// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml

import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.IOException
import java.time.Instant
import scala.sys.process._
import scala.util.Try

object Secrets {
  private[ml] case class ExpiringAccessToken(value: String, expiresAt: Instant)

  private val KvName = "mmlspark-build-keys"
  private[ml] val SubscriptionID = "e342c2c0-f844-4b18-9208-52c8c234c30e"

  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => (Seq("cmd", "/C") ++ Seq(command)).!!
      case _ => command.!!
    }
  }

  private def safeExec(command: String): Option[String] = {
    try {
      Some(exec(command))
    } catch {
      case e: java.lang.RuntimeException =>
        println(s"Secret fetch error: ${e.toString}")
        None
      case e: IOException =>
        println(s"Secret fetch error: ${e.toString}")
        None
    }
  }

  private def extractField(json: String, field: String): String = {
    json.parseJson.asJsObject().fields(field).convertTo[String]
  }

  // Keep overhead of setting account down
  lazy val AccountString: String = {
    try {
      exec(s"az account set -s $SubscriptionID")
    } catch {
      case e: java.lang.RuntimeException =>
        println(s"Secret fetch error: ${e.toString}")
      case e: IOException =>
        println(s"Secret fetch error: ${e.toString}")
    }
    SubscriptionID
  }

  def getSynapseExtensionSecret(envName: String, secretType: String): String = {
    val secretKey = s"synapse-extension-$envName-$secretType"
    println(s"[info] fetching secret: $secretKey from $AccountString")
    safeExec(s"az keyvault secret show --vault-name $KvName --name $secretKey")
      .map(extractField(_, "value"))
      .getOrElse("")
  }

  private def getSecret(secretName: String): String = {
    println(s"[info] fetching secret: $secretName from $AccountString")
    safeExec(s"az keyvault secret show --vault-name $KvName --name $secretName")
      .map(extractField(_, "value"))
      .getOrElse("")
  }

  private def getAccessTokenFields(reqResource: String): Map[String, JsValue] = {
    println(s"[info] token for perms: $reqResource from $AccountString")
    val json = exec(s"az account get-access-token --resource $reqResource --output json")
    json.parseJson.asJsObject().fields
  }

  private def getAccessTokenFields(reqResource: String): Map[String, JsValue] = {
    println(s"[info] token for perms: $reqResource from $AccountString")
    val json = exec(s"az account get-access-token --resource $reqResource --output json")
    json.parseJson.asJsObject().fields
  }

  def getAccessToken(reqResource: String): String = {
    getAccessTokenFields(reqResource)("accessToken").convertTo[String]
  }

  private[ml] def parseExpiringAccessToken(fields: Map[String, JsValue]): ExpiringAccessToken = {
    val expiresOn = fields.get("expires_on").flatMap {
      case JsNumber(value) => Try(value.toLongExact).toOption
      case JsString(value) => Try(value.toLong).toOption
      case _ => None
    }.getOrElse {
      throw new IllegalStateException("Azure CLI access token response did not include a valid expires_on epoch value")
    }
    ExpiringAccessToken(
      fields("accessToken").convertTo[String],
      Instant.ofEpochSecond(expiresOn)
    )
  }

  private[ml] def getAccessTokenWithExpiry(reqResource: String): ExpiringAccessToken = {
    parseExpiringAccessToken(getAccessTokenFields(reqResource))
  }

  lazy val CognitiveApiKey: String = getSecret("cognitive-api-key")
  lazy val OpenAIApiKey: String = getSecret("openai-api-key-3")
  lazy val AIFoundryApiKey: String = getSecret("synapseml-ai-foundry-resource-key")

  lazy val CustomSpeechApiKey: String = getSecret("custom-speech-api-key")
  lazy val ConversationTranscriptionUrl: String = getSecret("conversation-transcription-url")
  lazy val ConversationTranscriptionKey: String = getSecret("conversation-transcription-key")

  lazy val AzureSearchKey: String = getSecret("azure-search-key")
  lazy val TranslatorKey: String = getSecret("translator-key")
  lazy val AzureMapsKey: String = getSecret("azuremaps-api-key")
  lazy val PowerbiURL: String = getSecret("powerbi-url")
  lazy val AdbToken: String = getSecret("adb-token")

  lazy val ArtifactStore: String = getSecret("synapse-artifact-store")
  lazy val Platform: String = getSecret("synapse-platform")
  lazy val AadResource: String = getSecret("synapse-internal-aad-resource")

  lazy val LanguageApiKey: String = getSecret("language-api-key")
}
