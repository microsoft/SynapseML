package com.microsoft.azure.synapse.ml.fabric

import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.json.JSONObject
import pdi.jwt.{Jwt, JwtOptions}
import spray.json.DefaultJsonProtocol.StringJsonFormat

import java.util.Date
import scala.util.{Failure, Success, Try}

object OpenAITokenLibrary extends SynapseMLLogging{
  var MLMWCToken = "";
  val BackgroundRefreshExpiryCushionInMillis: Long = 5 * 60 * 1000L
  val OpenAIFeatureName = "SparkCodeFirst"

  def getAccessToken: String = {
    if (MLMWCToken != "" && !isTokenExpired(MLMWCToken)) {
      logInfo("using cached openai mwc token")
      MLMWCToken
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
        var token = FabricClient.usagePost(url, payload).asJsObject.fields("Token").convertTo[String];
        logInfo("successfully fetch openai mwc token")
        token
      } catch {
        case e: Throwable =>
          logInfo("openai mwc token not available, using aad token", e)
          TokenLibrary.getAccessToken;
      }
    }
  }

  def getExpiryTime(accessToken: String): Date = {
    //Extract expiry time
    val jwtOptions = new JwtOptions(false, false, false, 0)
    val jwtTokenDecoded: Try[(String, String, String)] = Jwt.decodeRawAll(accessToken, jwtOptions)
    jwtTokenDecoded match {
      case Success((_, payload, _)) =>
        val jsonPayload: JSONObject = new JSONObject(payload)
        val expiry = jsonPayload.get("exp").toString
        new Date(expiry.toLong * 1000)
      case Failure(t) =>
        throw t
    }
  }

  def isTokenExpired(accessToken: String, expiryCushionInMillis: Long = 0): Boolean = {
    try {
      val expiry: Date = getExpiryTime(accessToken)
      val currentTime: Long = System.currentTimeMillis()
      val expiryTimeMillis: Long = expiry.getTime()
      currentTime > expiryTimeMillis - expiryCushionInMillis
    }
    catch {
      case t: Throwable =>
        logInfo("Error while getting token expiry time", t)
        true
    }
  }

  //noinspection ScalaStyle
  override val uid: String = "OpenAITokenLibrary";
}
