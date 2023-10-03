// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.fabric

import java.util.UUID
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import spray.json.DefaultJsonProtocol.{StringJsonFormat, jsonFormat3}
import spray.json.RootJsonFormat

case class MwcToken(TargetUriHost: String, CapacityObjectId: String, Token: String)

object TokenUtils extends WebUtils {
  private var AADToken: Option[String] = None
  val MwcWorkloadTypeMl = "ML"

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

  def getMwcToken(sharedHost: String,
                  workspaceId: String,
                  capacityId: String,
                  workloadType: String): Option[MwcToken] = {
    val url: String = sharedHost + "/metadata/v201606/generatemwctokenv2"

    val payLoad =
      s"""{
         |"capacityObjectId": "$capacityId",
         |"workspaceObjectId": "$workspaceId",
         |"workloadType": "$workloadType"
    }""".stripMargin

    val driverAADToken = getAccessToken("pbi")

    val headers = Map(
      "Content-Type" -> "application/json",
      "Authorization" -> s"""Bearer $driverAADToken""".stripMargin,
      "x-ms-workload-resource-moniker" -> UUID.randomUUID().toString
    )

    val response = usagePost(url, payLoad, headers)
    val targetUriHost = s"https://${response.asJsObject.fields("TargetUriHost").convertTo[String]}"
    response.asJsObject.fields.updated("TargetUriHost", targetUriHost)

    implicit val mwcTokenFormat: RootJsonFormat[MwcToken] = jsonFormat3(MwcToken)
    Some(response.convertTo[MwcToken])
  }
}
