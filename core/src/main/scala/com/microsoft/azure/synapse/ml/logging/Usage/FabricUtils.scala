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
  implicit val tokenServiceConfigFormat: RootJsonFormat[TokenServiceConfig] = jsonFormat4(TokenServiceConfig)
}

import TokenServiceConfigProtocol._

object FabricUtils {
  var trident_context = Map[String, String]()

  def getFabricContext(): Map[String, String] = {
    if (trident_context.nonEmpty) {
      trident_context
    } else {
      try {
        val lines = scala.io.Source.fromFile(FabricConstants.CONTEXT_FILE_PATH).getLines().toList
        for (line <- lines) {
          if (line.split('=').length == 2) {
            val Array(k, v) = line.split('=')
            trident_context += (k.trim -> v.trim)
          }
        }

        var file_content: String = Source.fromFile(FabricConstants.TOKEN_SERVICE_FILE_PATH).mkString
        file_content = cleanJson(file_content)
        val tokenServiceConfigJson = file_content.parseJson

        // Extract the values from the JSON using Spray JSON's automatic JSON-to-case-class conversion
        val tokenServiceConfig = tokenServiceConfigJson.convertTo[TokenServiceConfig]
        // Populate the trident_context map
        trident_context += (FabricConstants.SYNAPSE_TOKEN_SERVICE_ENDPOINT -> tokenServiceConfig.tokenServiceEndpoint)
        trident_context += (FabricConstants.SYNAPSE_CLUSTER_TYPE -> tokenServiceConfig.clusterType)
        trident_context += (FabricConstants.SYNAPSE_CLUSTER_IDENTIFIER -> tokenServiceConfig.clusterName)
        trident_context += (FabricConstants.TRIDENT_SESSION_TOKEN -> tokenServiceConfig.sessionToken)
      } catch {
        case e: Exception =>
          SynapseMLLogging.logMessage(s"Error reading Fabric context file: $e")
          throw e
      }
    }
    trident_context
  }

  def cleanJson(s: String): String = {
    val pattern: Regex = ",[ \t\r\n]+}".r
    val cleanedJson = pattern.replaceAllIn(s, "}")
    cleanedJson
  }
}
