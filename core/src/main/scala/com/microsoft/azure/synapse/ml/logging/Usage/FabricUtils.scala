// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import spray.json._

case class TokenServiceConfig(tokenServiceEndpoint: String,
                              clusterType: String,
                              clusterName: String,
                              sessionToken: String)

object TokenServiceConfigProtocol extends DefaultJsonProtocol {
  implicit val TokenServiceConfigFormat: RootJsonFormat[TokenServiceConfig] = jsonFormat4(TokenServiceConfig)
}

import TokenServiceConfigProtocol._

object FabricUtils extends FabricConstants {
  lazy val FabricContext: Map[String, String] = {
    val linesContextFile = StreamUtilities.usingSource(scala.io.Source.fromFile(contextFilePath)) {
      source => source.getLines().toList
    }.get

    val tokenServiceConfig = StreamUtilities.usingSource(scala.io.Source.fromFile
    (tokenServiceFilePath)) {
      source => source.mkString.parseJson.convertTo[TokenServiceConfig]
    }.get

    val tridentContext: Map[String, String] = linesContextFile
      .filter(line => line.split('=').length == 2)
      .map { line =>
        val Array(k, v) = line.split('=')
        (k.trim, v.trim)
      }.toMap
      .++(Seq(
        (synapseTokenServiceEndpoint, tokenServiceConfig.tokenServiceEndpoint),
        (synapseClusterType, tokenServiceConfig.clusterType),
        (synapseClusterIdentifier, tokenServiceConfig.clusterName),
        (tridentSessionToken, tokenServiceConfig.sessionToken)
      ).toMap)

    tridentContext
  }
}
