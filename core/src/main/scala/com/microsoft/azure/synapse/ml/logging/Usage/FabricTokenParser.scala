// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

import spray.json._

class InvalidJwtTokenException(message: String) extends Exception(message)
class JwtTokenExpiryMissingException(message: String) extends Exception(message)
class FabricTokenParser(JWToken: String) {
  val tokens = JWToken.split("\\.")
  var parsedToken: JsValue = JsObject.empty

  if (tokens.length == 3) {
    val payload = tokens(1)
    val sanitizedPayload = payload.replace('-', '+').replace('_', '/').replaceAll("\\.", "").replaceAll("\\s", "")
    val decodedPayload = java.util.Base64.getDecoder.decode(sanitizedPayload)
    val decodedJson = new String(decodedPayload)
    parsedToken = decodedJson.parseJson
  }
  else {
    throw new InvalidJwtTokenException(s"Invalid JWT token. Here is the token = {$JWToken}")
    println("Invalid JWT token input.")
  }

  def getExpiry(): Long ={
    val exp: Option[Long] = parsedToken.asJsObject.fields.get("exp").collect { case JsNumber(value) => value.toLong }
    exp match {
      case Some(expValue) =>
        expValue
      case None =>
        throw new JwtTokenExpiryMissingException(s"JWT token does not have expiration set. " +
          s"Here is the token = {$JWToken}")
    }
  }
}
