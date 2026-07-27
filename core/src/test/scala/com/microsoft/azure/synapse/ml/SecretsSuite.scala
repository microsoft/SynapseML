// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml

import org.scalatest.funsuite.AnyFunSuite
import spray.json.{JsNumber, JsString}

import java.time.Instant

class SecretsSuite extends AnyFunSuite {

  test("Parse Azure CLI access token expiry") {
    Seq(JsNumber(1777000000L), JsString("1777000000")).foreach { expiry =>
      val token = Secrets.parseExpiringAccessToken(Map(
        "accessToken" -> JsString("test-token"),
        "expires_on" -> expiry
      ))

      assert(token.value === "test-token")
      assert(token.expiresAt === Instant.ofEpochSecond(1777000000L))
    }
  }

  test("Reject Azure CLI access token without a valid numeric expiry") {
    Seq(None, Some(JsString("not-an-epoch"))).foreach { expiry =>
      val fields = Map("accessToken" -> JsString("test-token")) ++ expiry.map("expires_on" -> _)
      val error = intercept[IllegalStateException] {
        Secrets.parseExpiringAccessToken(fields)
      }

      assert(error.getMessage ===
        "Azure CLI access token response did not include a valid expires_on epoch value")
    }
  }
}
