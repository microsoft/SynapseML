// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

// The setter-time normalization in setCustomHeaders only runs through that setter. The generic
// ServiceParam setting paths -- the typed setScalarParam(customHeaders, v), the string-name
// setScalarParam("customHeaders", v), and the raw Params.set(customHeaders, Left(v)) -- bypass it and
// store the value verbatim, so a null map/key/value survives to Param.jsonEncode (the exact step
// ComplexParamsWritable.getMetadataToSave runs) and would NPE / trip spray-json require(x ne null) at
// save time. The customHeaders ServiceParam normalizes at its own JSON encode/decode boundary
// (exercised end to end by persistCustomHeaders), so every generic setting/persistence path is safe by
// construction while build() still sanitizes at assembly time. These pure tests complement the real
// AddDocuments save/load round-trip in AddDocumentsHeaderPersistenceSuite.
class AzureSearchGenericParamPersistenceSuite extends AnyFunSuite {

  // Reproduces the exact per-param persistence step ComplexParamsWritable.getMetadataToSave runs
  // (Param.jsonEncode) followed by the load-time Param.jsonDecode: a null map, header name, or header
  // value in the stored param throws here unless the param normalizes at its own JSON boundary.
  private def persistCustomHeaders(stage: AddDocuments): Map[String, String] =
    stage.customHeaders.jsonDecode(
      stage.customHeaders.jsonEncode(stage.getOrDefault(stage.customHeaders))).left.get

  // scalastyle:off null
  test("generic typed setScalarParam(customHeaders, null map) persists and builds without an NPE") {
    val nullMap: Map[String, String] = null
    val stage = new AddDocuments().setSubscriptionKey("resolved-key")
    stage.setScalarParam(stage.customHeaders, nullMap)
    assert(persistCustomHeaders(stage).isEmpty)
    val headers = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(headers("api-key") == "resolved-key")
  }

  test("generic string-name setScalarParam(\"customHeaders\", null map) persists without an NPE") {
    val nullMap: Map[String, String] = null
    val stage = new AddDocuments().setSubscriptionKey("resolved-key")
    stage.setScalarParam("customHeaders", nullMap)
    assert(persistCustomHeaders(stage).isEmpty)
    val headers = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(headers("api-key") == "resolved-key")
  }

  test("generic Params.set(customHeaders, Left(null map)) persists and preserves auth precedence") {
    val nullMap: Map[String, String] = null
    val stage = new AddDocuments().setAADToken("aad-token")
    stage.set(stage.customHeaders, Left(nullMap))
    assert(persistCustomHeaders(stage).isEmpty)
    val headers = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(headers.contains("Authorization"))
    assert(!headers.contains("api-key"))
  }

  test("generic Params.set null key/value entries persist as only the valid headers") {
    val stage = new AddDocuments().setSubscriptionKey("resolved-key")
    stage.set(stage.customHeaders, Left(Map(
      (null: String) -> "orphan-value", "x-null-value" -> (null: String), "x-generic" -> "generic-value")))
    assert(persistCustomHeaders(stage) == Map("x-generic" -> "generic-value"))
    val headers = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(headers("api-key") == "resolved-key")
    assert(headers("x-generic") == "generic-value")
    assert(headers.keySet.forall(name => name != null))
    assert(!headers.values.exists(value => value == null || value.contains("orphan-value")))
  }

  test("generic-path embedded credential precedence survives boundary normalization and persistence") {
    val stage = new AddDocuments()
    stage.setScalarParam(stage.customHeaders, Map(
      "api-key" -> "embedded-key", "x-null-value" -> (null: String), "x-generic" -> "generic-value"))
    assert(persistCustomHeaders(stage) == Map("api-key" -> "embedded-key", "x-generic" -> "generic-value"))
    val embedded = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(embedded("api-key") == "embedded-key")
    assert(embedded.keys.count(name => name.equalsIgnoreCase("api-key")) == 1)
    assert(embedded("x-generic") == "generic-value")

    stage.setSubscriptionKey("resolved-key")
    val outranked = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(outranked("api-key") == "resolved-key")
  }

  test("generic-path custom header values are preserved verbatim and the boundary never leaks them") {
    val canary = "canary-9d13-not-a-real-secret"
    val stage = new AddDocuments().setSubscriptionKey("resolved-key")
    stage.set(stage.customHeaders, Left(Map("x-canary" -> canary, "x-null-value" -> (null: String))))
    // Normalization silently drops the null entry and keeps the real value: no rejection is raised, so
    // no rendered exception can leak the value, and persistence/header assembly stay NPE-free.
    assert(persistCustomHeaders(stage) == Map("x-canary" -> canary))
    val headers = stage.buildServiceAuthHeaders(Row.empty, addContentType = false, None)
    assert(headers("x-canary") == canary)
    assert(headers("api-key") == "resolved-key")
  }
  // scalastyle:on null
}
