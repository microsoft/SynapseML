// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.Usage.{FabricTokenParser, InvalidJwtTokenException}

import scala.io.Source
import spray.json._

class FabricTokenParserTests extends TestBase {

  case class Token(valid: String, payload: String)

  object TokenJsonProtocol extends DefaultJsonProtocol {
    implicit val TokenFormat: RootJsonFormat[Token] = jsonFormat2(Token)
  }

  import TokenJsonProtocol._

  test("JWT Token Expiry Check"){
      val fabricTokenParser = new FabricTokenParser(createDummyToken(true))
      val exp: Long = fabricTokenParser.getExpiry
      assert(exp > 0L)
  }

  test("Invalid JWT Token Check."){
    assertThrows[InvalidJwtTokenException]{
      val fabricTokenParser = new FabricTokenParser(createDummyToken(false))
    }
  }

  def createDummyToken(createValidToken: Boolean): String = {
    val claims = """{
          "iss": "issuer",
          "sub": "subject",
          "aud": "audience",
          "exp": 1691171109,
          "userId": "123456789"
        }"""

    val header = encodeBase64URLSafeString ("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes ("UTF-8") )
    val payload = encodeBase64URLSafeString (claims.getBytes ("UTF-8") )
    val dummySignature = "dummy-signature" // You can replace this with an actual signature if needed

    if(createValidToken)
      s"$header.$payload.$dummySignature"
    else
      s"$header.$payload"
  }

  def encodeBase64URLSafeString(bytes: Array[Byte]): String = {
    java.util.Base64.getUrlEncoder.encodeToString(bytes)
  }
}
