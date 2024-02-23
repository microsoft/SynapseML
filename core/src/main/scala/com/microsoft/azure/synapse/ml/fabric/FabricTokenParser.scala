// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import spray.json._

class InvalidJwtTokenException(message: String) extends Exception(message)

class JwtTokenExpiryMissingException(message: String) extends Exception(message)

class FabricTokenParser(JWToken: String) {
  private val tokens: Array[String] = JWToken.split("\\.")
  private val parsedToken: JsValue = tokenCheckAndDecode(Some(tokens))

  def getExpiry: Long = {
    val exp: Option[Long] = parsedToken.asJsObject.fields.get("exp").collect {
      case JsNumber(value) => value.toLong * 1000L
    }
    exp match {
      case Some(expValue) =>
        expValue
      case None =>
        throw new JwtTokenExpiryMissingException(s"JWT token does not have expiration set. " +
          s"Here is the token = {$JWToken}")
    }
  }

  private def tokenCheckAndDecode(tokens: Option[Array[String]]): JsValue = {
    tokens match {
      case Some(tokens) =>
        if (tokens.length == 3) {
          // Getting the JWT payload which is second member of [header].[payload].[signature]
          val payload = tokens(1)
          // Removing whitespace and url safe characters encoded that might have been added to token
          val sanitizedPayload = payload.replace('-', '+').replace('_', '/').replaceAll("\\.", "").replaceAll("\\s", "")
          val decodedPayload = java.util.Base64.getDecoder.decode(sanitizedPayload)
          val decodedJson = new String(decodedPayload)
          decodedJson.parseJson
        }
        else {
          throw new InvalidJwtTokenException(s"Invalid JWT token. Here is the token = {$JWToken}")
        }
      case None =>
        throw new NullPointerException("Invalid JWT token used for reporting usage data.")
    }
  }
}
