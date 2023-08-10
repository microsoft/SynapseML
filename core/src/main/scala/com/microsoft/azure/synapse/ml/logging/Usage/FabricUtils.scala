// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

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
        val contextFile = scala.io.Source.fromFile(FabricConstants.ContextFilePath)
        try {
          val lines = contextFile.getLines().toList
          for (line <- lines) {
            if (line.split('=').length == 2) {
              val Array(k, v) = line.split('=')
              TridentContext += (k.trim -> v.trim)
            }
          }
        }
        finally {
          contextFile.close()
        }

        val tokenServiceFile = Source.fromFile(FabricConstants.TokenServiceFilePath)
        try {
          var fileContent: String = tokenServiceFile.mkString
          fileContent = cleanJson(fileContent)
          val tokenServiceConfigJson = fileContent.parseJson

          val tokenServiceConfig = tokenServiceConfigJson.convertTo[TokenServiceConfig]
          TridentContext += (FabricConstants.SynapseTokenServiceEndpoint -> tokenServiceConfig.tokenServiceEndpoint)
          TridentContext += (FabricConstants.SynapseClusterType -> tokenServiceConfig.clusterType)
          TridentContext += (FabricConstants.SynapseClusterIdentifier -> tokenServiceConfig.clusterName)
          TridentContext += (FabricConstants.TridentSessionToken -> tokenServiceConfig.sessionToken)
        }
        finally {
          tokenServiceFile.close()
        }
        TridentContext
      } catch {
        case e: Exception =>
          SynapseMLLogging.logMessage(s"Error reading Fabric context file: $e")
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
