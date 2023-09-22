// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.logging.common.WebUtils._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import java.time.Instant
import java.util.UUID
import org.apache.spark.SparkContext
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat3}
import spray.json.RootJsonFormat

case class MwcToken (TargetUriHost: String, CapacityObjectId: String, Token: String)
object TokenUtils {
  var AADToken: Option[String] = None
  val MwcWorkloadTypeMl = "ML"

  def getAccessToken: String = {
    if (isTokenValid(AADToken))
      this.AADToken.get
    else {
      refreshAccessToken()
      this.AADToken.get
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

  private def isTokenValid(tokenOption: Option[String]): Boolean = {
    tokenOption match {
      case Some(token) if token.nonEmpty =>
        try {
          val tokenParser = new FabricTokenParser(token)
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
      case _ =>
        false // No value is present or the value is empty
    }
  }

  private def refreshAccessToken(): Unit = {
    if (SparkContext.getOrCreate() != null) {
      val token = getAccessToken("pbi")
      AADToken = Some(token)
    } else {
      val token = new FabricTokenServiceClient().getAccessToken("pbi")
      AADToken = Some(token)
    }
  }

  def getMwcToken(shared_host: String, WorkspaceId: String, capacity_id: String,
                    workload_type: String): Option[MwcToken]= {
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

    val response = usagePost(url, payLoad, headers)
    val targetUriHost = s"https://${response.asJsObject.fields("TargetUriHost").convertTo[String]}"
    response.asJsObject.fields.updated("TargetUriHost", targetUriHost)

    implicit val mwcTokenFormat: RootJsonFormat[MwcToken] = jsonFormat3(MwcToken)
    val mwcToken = response.convertTo[MwcToken]
    Some(mwcToken)
  }
}
