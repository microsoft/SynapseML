// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import spray.json._
import spray.json.DefaultJsonProtocol._
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
  var TridentContext = Map[String, String]()

  def getFabricContext(): Map[String, String] = {
    if (TridentContext.nonEmpty) {
      TridentContext
    } else {
      try {
        val lines = scala.io.Source.fromFile(FabricConstants.ContextFilePath).getLines().toList
        for (line <- lines) {
          if (line.split('=').length == 2) {
            val Array(k, v) = line.split('=')
            TridentContext += (k.trim -> v.trim)
          }
        }

        var fileContent: String = Source.fromFile(FabricConstants.TokenServiceFilePath).mkString
        fileContent = cleanJson(fileContent)
        val tokenServiceConfigJson = fileContent.parseJson

        // Extract the values from the JSON using Spray JSON's automatic JSON-to-case-class conversion
        val tokenServiceConfig = tokenServiceConfigJson.convertTo[TokenServiceConfig]
        // Populate the TridentContext map
        TridentContext += (FabricConstants.SynapseTokenServiceEndpoint -> tokenServiceConfig.tokenServiceEndpoint)
        TridentContext += (FabricConstants.SynapseClusterType -> tokenServiceConfig.clusterType)
        TridentContext += (FabricConstants.SynapseClusterIdentifier -> tokenServiceConfig.clusterName)
        TridentContext += (FabricConstants.TridentSessionToken -> tokenServiceConfig.sessionToken)
      } catch {
        case e: Exception =>
          SynapseMLLogging.logMessage(s"Error reading Fabric context file: $e")
          throw e
      }
    }
    TridentContext
  }

  def cleanJson(s: String): String = {
    val pattern: Regex = ",[ \t\r\n]+}".r
    val cleanedJson = pattern.replaceAllIn(s, "}")
    cleanedJson
  }
}
