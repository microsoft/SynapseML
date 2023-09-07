// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import spray.json._
import scala.util.matching.Regex
import scala.io.Source

case class TokenServiceConfig(tokenServiceEndpoint: String,
                              clusterType: String,
                              clusterName: String,
                              sessionToken: String)

object TokenServiceConfigProtocol extends DefaultJsonProtocol {
  implicit val TokenServiceConfigFormat: RootJsonFormat[TokenServiceConfig] = jsonFormat4(TokenServiceConfig)
}

import TokenServiceConfigProtocol._

object FabricUtils {
  //private var TridentContext: Map[String, String] = Map[String, String]()

  def getFabricContext: Map[String, String] = {
    try {
      val linesContextFile = StreamUtilities.usingSource(scala.io.Source.fromFile(FabricConstants.ContextFilePath)) {
        source => source.getLines().toList
      }.get

      val tokenServiceConfig = StreamUtilities.usingSource(scala.io.Source.fromFile
      (FabricConstants.TokenServiceFilePath)) {
        source => cleanJson(source.mkString).parseJson.convertTo[TokenServiceConfig]
      }.get

      val tridentContext: Map[String, String] = linesContextFile
        .filter(line => line.split('=').length == 2)
        .map { line =>
          val Array(k, v) = line.split('=')
          (k.trim, v.trim)
        }.toMap
        .++(Seq(
          (FabricConstants.SynapseTokenServiceEndpoint, tokenServiceConfig.tokenServiceEndpoint),
          (FabricConstants.SynapseClusterType, tokenServiceConfig.clusterType),
          (FabricConstants.SynapseClusterIdentifier, tokenServiceConfig.clusterName),
          (FabricConstants.TridentSessionToken, tokenServiceConfig.sessionToken)
        ).toMap)

      tridentContext
    } catch {
      case e: NullPointerException =>
        SynapseMLLogging.logMessage(s"Error reading Fabric context file: Trident context file path is missing. $e")
        throw e
    }
  }

  private def cleanJson(s: String): String = {
    val pattern: Regex = ",[ \t\r\n]+}".r
    val cleanedJson = pattern.replaceAllIn(s, "}")
    cleanedJson
  }
}
