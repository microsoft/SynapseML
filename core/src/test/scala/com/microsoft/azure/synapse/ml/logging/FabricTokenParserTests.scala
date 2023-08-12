// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.Usage.{FabricTokenParser, InvalidJwtTokenException}

import scala.io.Source
import spray.json._

class FabricTokenParserTests extends TestBase {

  case class Token(name: String, payload: String)

  object TokenJsonProtocol extends DefaultJsonProtocol {
    implicit val TokenFormat: RootJsonFormat[Token] = jsonFormat2(Token)
  }

  import TokenJsonProtocol._

  test("JWT Token Expiry Check"){
    val source = Source.fromFile("../resources/UsageTestData.json")
    try {
      val jsonString = source.mkString
      val parsedTokens = jsonString.parseJson
      val tokens = parsedTokens.convertTo[Seq[Token]]
      val token = tokens(0)
      val fabricTokenParser = new FabricTokenParser(token.payload)
      val exp: Long = fabricTokenParser.getExpiry
      assert(exp > 0L)
    } finally {
      source.close()
    }
  }

  test("Invalid JWT Token Check."){
    val source = Source.fromFile("../resources/UsageTestData.json")
    try {
      val jsonString = source.mkString
      val parsedTokens = jsonString.parseJson
      val tokens = parsedTokens.convertTo[Seq[Token]]
      val token = tokens(1)

      var exceptionThrown = false
      try {
        val fabricTokenParser = new FabricTokenParser(token.payload)
      } catch {
        case _: InvalidJwtTokenException => exceptionThrown = true
      }
      assert(exceptionThrown, "InvalidJwtTokenException was thrown.")
    } finally {
      source.close()
    }
  }
}
