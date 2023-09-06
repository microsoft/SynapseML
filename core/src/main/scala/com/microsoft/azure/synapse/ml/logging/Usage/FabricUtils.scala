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
  private var TridentContext: Map[String, String] = Map[String, String]()

  def getFabricContext: Map[String, String] = {
    if (TridentContext.nonEmpty) {
      TridentContext
    } else {
      try {
        val linesContextFile = StreamUtilities.usingSource(scala.io.Source.fromFile(FabricConstants.ContextFilePath)){
          source => source.getLines().toList
        }.get
        for (line <- linesContextFile) {
          if (line.split('=').length == 2) {
            val Array(k, v) = line.split('=')
            TridentContext += (k.trim -> v.trim)
          }
        }

        val tokenServiceConfig = StreamUtilities.usingSource(scala.io.Source.fromFile(
          FabricConstants.TokenServiceFilePath)) {
          source => cleanJson(source.mkString).parseJson.convertTo[TokenServiceConfig]
        }.get

        TridentContext += (FabricConstants.SynapseTokenServiceEndpoint -> tokenServiceConfig.tokenServiceEndpoint)
        TridentContext += (FabricConstants.SynapseClusterType -> tokenServiceConfig.clusterType)
        TridentContext += (FabricConstants.SynapseClusterIdentifier -> tokenServiceConfig.clusterName)
        TridentContext += (FabricConstants.TridentSessionToken -> tokenServiceConfig.sessionToken)

        TridentContext
      } catch {
        case e: NullPointerException =>
          SynapseMLLogging.logMessage(s"Error reading Fabric context file: Trident context file path is missing. $e")
          throw e
      }
    }
  }

  private def cleanJson(s: String): String = {
    val pattern: Regex = ",[ \t\r\n]+}".r
    val cleanedJson = pattern.replaceAllIn(s, "}")
    cleanedJson
  }
}
