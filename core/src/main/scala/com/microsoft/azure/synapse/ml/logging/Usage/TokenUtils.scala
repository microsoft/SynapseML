// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import java.time.Instant
import org.apache.spark.SparkContext
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat3}
import java.util.UUID
import com.microsoft.azure.synapse.ml.logging.common.WebUtils._
import spray.json.{DeserializationException, RootJsonFormat}
import spray.json.JsonParser.ParsingException

case class MwcToken (TargetUriHost: String, CapacityObjectId: String, Token: String)
object TokenUtils {
  var AADToken: String = ""
  val MwcWorkloadTypeMl = "ML"

  def getAccessToken: String = {
    if (checkTokenValid(this.AADToken))
      this.AADToken
    else {
      refreshAccessToken()
      this.AADToken
    }
  }

  def getAccessToken(tokenType: String): String = {

    val objectName = "com.microsoft.azure.trident.tokenlibrary.TokenLibrary"
    val mirror = currentMirror
    val module = mirror.staticModule(objectName)
    val obj = mirror.reflectModule(module).instance
    val objType = mirror.reflect(obj).symbol.toType
    val methodName = "getAccessToken"
    val methodSymbols = objType.decl(TermName(methodName)).asTerm.alternatives
    val argType = typeOf[String]

    val selectedMethodSymbol = methodSymbols.find { m =>
      m.asMethod.paramLists match {
        case List(List(param)) => param.typeSignature =:= argType
        case _ => false
      }
    }.getOrElse(throw new NoSuchMethodException(s"Method $methodName with argument type $argType not found"))

    val methodMirror = mirror.reflect(obj).reflectMethod(selectedMethodSymbol.asMethod)
    methodMirror(tokenType).asInstanceOf[String]
  }

  def checkTokenValid(token: String): Boolean = {
    if (token == null || token.isEmpty) {
      false
    }
    try{
      val tokenParser =   new FabricTokenParser(token)
      val expiryEpoch = tokenParser.getExpiry
      val now = Instant.now().getEpochSecond
      now < expiryEpoch - 60
    } catch {
      case e: InvalidJwtTokenException =>
        SynapseMLLogging.logMessage(s"TokenUtils::checkTokenValid: Token used to trigger telemetry " +
          s"endpoint is invalid. Exception = $e")
        false
      case e: JwtTokenExpiryMissingException =>
        SynapseMLLogging.logMessage(s"TokenUtils::checkTokenValid: Token misses expiry. " +
          s"Exception = $e")
        false
    }
  }

  private def refreshAccessToken(): Unit = {
    try {
      if (SparkContext.getOrCreate() != null) {
        val token = getAccessToken("pbi")
        AADToken = token
      } else {
        val token = new FabricTokenServiceClient().getAccessToken("pbi")
        AADToken = token
      }
    } catch {
      case e: Exception =>
        SynapseMLLogging.logMessage(s"refreshAccessTok: failed to refresh pbi tok. Exception: {$e}. (usage test)")
    }
  }

  def getMWCToken(shared_host: String, WorkspaceId: String, capacity_id: String,
                    workload_type: String): MwcToken = {
    val url: String = shared_host + "/metadata/v201606/generatemwctokenv2"

    val payLoad = s"""{
      |"capacityObjectId": "$capacity_id",
      |"workspaceObjectId": "$WorkspaceId",
      |"workloadType": "$workload_type"
    }""".stripMargin

    val driverAADToken = getAccessToken

    val headers = Map(
      "Content-Type" -> "application/json",
      "Authorization" -> s"""Bearer $driverAADToken""".stripMargin,
      "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
    )

    try{
      val response = usagePost(url, payLoad, headers)
      /*if (response.asJsObject.fields("status_code").convertTo[String] != 200
        || response.asJsObject.fields("content").convertTo[String].isEmpty) {
        throw new Exception("Fetch access token error")
      }*/
      var targetUriHost = response.asJsObject.fields("TargetUriHost").convertTo[String]
      targetUriHost = s"https://$targetUriHost"
      response.asJsObject.fields.updated("TargetUriHost", targetUriHost)

      implicit val mwcTokenFormat: RootJsonFormat[MwcToken] = jsonFormat3(MwcToken)
      response.convertTo[MwcToken]
    }
    catch {
      case e: NoSuchElementException =>
        SynapseMLLogging.logMessage(s"TokenUtils.getMWCToken: Cannot retrieve targetUriHost from MWC Token.")
        throw e
      case e: DeserializationException =>
        SynapseMLLogging.logMessage(s"TokenUtils.getMWCToken: The structure of response is not of type MwcToken.")
        throw e
      case e: ParsingException =>
        SynapseMLLogging.logMessage(s"TokenUtils.getMWCToken: The structure of json response is formed correctly.")
        throw e
      case e: Exception =>
        SynapseMLLogging.logMessage(s"getMWCTok: Failed to fetch MWC token that is required to " +
          s"get cluster details: $e.")
        throw e
    }
  }
}
