// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import java.io.File

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, RequestBuilder}
import org.apache.http.entity.mime.content.FileBody
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import spray.json._


object SpeechAPI {

  import RESTHelpers._

  def getSpeakerProfile(data: File, key: String): String = {
    retry(List(100, 500, 1000), { () =>
      val request = RequestBuilder
        .post("https://signature.centralus.cts.speech.microsoft.com" +
          "/api/v1/Signature/GenerateVoiceSignatureFromFormData")
        .setEntity(MultipartEntityBuilder.create()
          .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
          .addPart("file", new FileBody(data))
          .build())
        .addHeader("Ocp-Apim-Subscription-Key", key)
        .build()

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
