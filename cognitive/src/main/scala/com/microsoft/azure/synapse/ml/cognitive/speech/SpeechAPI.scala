// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive.speech

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpPost}
import org.apache.http.entity.ByteArrayEntity
import spray.json._

import java.io.File
import java.nio.file.Files

object SpeechAPI {

  def getSpeakerProfile(data: File, key: String, region: String): String = {
    retry(List(100, 500, 1000), { () => //scalastyle:ignore magic.number
      val httpsURL = s"https://signature.$region.cts.speech.microsoft.com" +
        s"/api/v1/Signature/GenerateVoiceSignatureFromByteArray"
      val voiceSampleData = Files.readAllBytes(data.toPath)

      val request = new HttpPost(httpsURL)
      request.setEntity(new ByteArrayEntity(voiceSampleData))
      request.addHeader("Ocp-Apim-Subscription-Key", key)

      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(
            s"Failed: response: $response " +
              s"requestUrl: ${request.getURI}" +
              s"requestBody: $bodyOpt")
        }
        IOUtils.toString(response.getEntity.getContent, "UTF-8")
          .parseJson.asJsObject().fields("Signature").compactPrint
      }.get
    })
  }


}
