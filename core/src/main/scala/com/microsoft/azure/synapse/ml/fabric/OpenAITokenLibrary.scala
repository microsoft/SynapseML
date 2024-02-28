// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import spray.json.DefaultJsonProtocol.StringJsonFormat

object OpenAITokenLibrary extends SynapseMLLogging with AuthHeaderProvider {
  private var MLToken: Option[String] = None;
  private var IsMWCTokenEnabled: Boolean = true;
  val BackgroundRefreshExpiryCushionInMillis: Long = 5 * 60 * 1000L
  val OpenAIFeatureName = "SparkCodeFirst"

  private def buildAuthHeader: String = {
    if(IsMWCTokenEnabled) {
      "MwcToken " + MLToken.getOrElse("")
    } else {
      "Bearer " + TokenLibrary.getAccessToken;
    }
  }

  def getAuthHeader: String = {
    if (isTokenExpired(MLToken, BackgroundRefreshExpiryCushionInMillis)) {
      val artifactId = FabricClient.ArtifactID
      val payload =
        s"""{
           |"artifactObjectId": "${artifactId.getOrElse("")}",
           |"openAIFeatureName": "$OpenAIFeatureName",
           |}""".stripMargin

      val url: String = FabricClient.MLWorkloadEndpointML + "cognitive/openai/generatemwctoken";
      try {
        val token = FabricClient.usagePost(url, payload).asJsObject.fields("Token").convertTo[String];
        MLToken = Some(token)
        IsMWCTokenEnabled = true
      } catch {
        case e: Throwable =>
          IsMWCTokenEnabled = false
      }
    }
    buildAuthHeader
  }

  private def getExpiryTime(accessToken: String): Long = {
    //Extract expiry time
    val parser = new FabricTokenParser(accessToken);
    parser.getExpiry
  }

  private def isTokenExpired(accessToken: Option[String], expiryCushionInMillis: Long = 0): Boolean = {
    accessToken match {
      case Some(accessToken) =>
        try {
          val expiry: Long = getExpiryTime(accessToken)
          val currentTime: Long = System.currentTimeMillis()
          currentTime > expiry - expiryCushionInMillis
        }
        catch {
          case t: Throwable =>
            logInfo("Error while getting token expiry time", t)
            true
        }
      case None =>
        true
    }

  }

  // scalastyle:off
  override val uid: String = "OpenAITokenLibrary";
}
