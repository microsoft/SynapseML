// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.fabric

import com.microsoft.azure.synapse.ml.fabric.FabricClient
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails.runningOnFabric
import spray.json.DefaultJsonProtocol.{StringJsonFormat, _}
import spray.json._

import java.time.Instant

object CertifiedEventClient {
  private lazy val CertifiedEventUri = getCertifiedEventUri

  private[ml] def getCertifiedEventUri: String = {
    s"${FabricClient.MLWorkloadEndpointAdmin}/telemetry"
  }


  private[ml] def logToCertifiedEvents(featureName: String,
                                       activityName: String): Unit = {
    if (runningOnFabric) {
      val payload =
        s"""{
           |"timestamp":${Instant.now().getEpochSecond},
           |"feature_name":"$featureName",
           |"activity_name":"$activityName",
           |"attributes":{}
           |}""".stripMargin

      FabricClient.usagePost(CertifiedEventUri, payload)
    }
  }
}
