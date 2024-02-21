// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import spray.json.DefaultJsonProtocol.StringJsonFormat

object OpenAITokenLibrary extends SynapseMLLogging with AuthHeaderProvider {
  var MLMWCToken = "";
  val BackgroundRefreshExpiryCushionInMillis: Long = 5 * 60 * 1000L
  val OpenAIFeatureName = "SparkCodeFirst"

  def getAuthHeader: String = {
    if (MLMWCToken != "" && !isTokenExpired(MLMWCToken)) {
      logInfo("using cached openai mwc token")
      "MwcToken " + MLMWCToken
    }
    else {
      val artifactId = FabricClient.ArtifactID
      val payload =
        s"""{
           |"artifactObjectId": "$artifactId",
           |"openAIFeatureName": "$OpenAIFeatureName",
           |}""".stripMargin

      val url: String = FabricClient.MLWorkloadEndpointML + "cognitive/openai/generatemwctoken";

      try {
        val token = FabricClient.usagePost(url, payload).asJsObject.fields("Token").convertTo[String];
        logInfo("successfully fetch openai mwc token");
        MLMWCToken = token;
        "MwcToken " + token
      } catch {
        case e: Throwable =>
          logInfo("openai mwc token not available, using aad token", e)
          "Bearer " + TokenLibrary.getAccessToken;
      }
    }
  }

  def getExpiryTime(accessToken: String): Long = {
    //Extract expiry time
    val parser = new FabricTokenParser(accessToken);
    parser.getExpiry
  }

  def isTokenExpired(accessToken: String, expiryCushionInMillis: Long = 0): Boolean = {
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
  }

  // scalastyle:off
  override val uid: String = "OpenAITokenLibrary";
}
