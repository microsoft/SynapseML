// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.logging.common.WebUtils._
import scala.util.{Success, Try}
import spray.json.{JsValue}
import spray.json.DefaultJsonProtocol.IntJsonFormat
import spray.json.DefaultJsonProtocol.StringJsonFormat

object CommonUtils {
  def requestGet(url: String, headers: Map[String, String], property: String): JsValue = {
    val response: JsValue = usageGet(url, headers)

    val statusCode = Try(response.asJsObject.fields("status_code").convertTo[Int])
    val propertyValue = Try(response.asJsObject.fields(property).convertTo[String])

    (statusCode, propertyValue) match {
      case (Success(code), Success(value)) if code == 200 && !value.isEmpty => response.asJsObject.fields(property)
      case _ => throw new Exception(s"CommonUtils.requestGet: Failed with " +
        s"code=$statusCode. Property looked for was = $property")
    }
  }
}
