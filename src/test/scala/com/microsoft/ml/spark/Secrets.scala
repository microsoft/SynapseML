// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.sys.process._

import sys.process._
import spray.json._
import DefaultJsonProtocol._

object Secrets {
  private val kvName = "mmlspark-keys"
  private val subscriptionID = "ca9d21ff-2a46-4e8b-bf06-8d65242342e5"

  protected def exec(command: String): String = {
    val os = sys.props("os.name").toLowerCase
    os match {
      case x if x contains "windows" => Seq("cmd", "/C") ++ Seq(command) !!
      case _ => command !!
    }
  }

  private def getSecret(secretName: String): String = {
    println(s"fetching secret: $secretName")
    exec(s"az account set -s $subscriptionID")
    val secretJson = exec(s"az keyvault secret show --vault-name $kvName --name $secretName")
    secretJson.parseJson.asJsObject().fields("value").convertTo[String]
  }

  lazy val textApiKey: String = getSecret("text-api-key")
  lazy val anomalyApiKey: String = getSecret("anomaly-api-key")
  lazy val azureSearchKey: String = getSecret("azure-search-key")
  lazy val bingImageSearchKey: String = getSecret("bing-image-search-key")
  lazy val faceApiKey: String = getSecret("face-api-key")
  lazy val powerbiURL: String = getSecret("powerbi-url")
  lazy val speechApiKey: String = getSecret("speech-api-key")
  lazy val visionApiKey: String = getSecret("vision-api-key")

}
