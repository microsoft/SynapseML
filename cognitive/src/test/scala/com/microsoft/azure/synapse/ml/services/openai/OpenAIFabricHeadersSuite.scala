// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.sql.Row
import spray.json._

class OpenAIFabricHeadersSuite extends TestBase {

  private class InspectableOpenAIChatCompletion(
      isFabric: Boolean,
      usesDefaultEndpoint: Boolean) extends OpenAIChatCompletion {

    override protected[openai] def runningOnFabric: Boolean = isFabric

    override protected[openai] def usingDefaultOpenAIEndpoint: Boolean = usesDefaultEndpoint

    override protected[openai] def fabricRuntime: String = "synapse_internal"

    def requestHeaders: Map[String, String] = {
      buildServiceAuthHeaders(
        Row.empty,
        addContentType = false,
        fabricFallbackAuthHeader = None
      )
    }
  }

  test("default Fabric OpenAI requests include SynapseML extended properties") {
    val transformer = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = true
    ).setCustomHeaders(Map(
      "X-Taxonomy-ExtendedProperties" -> """{"feature":"caller"}"""
    ))
    val headers = transformer.requestHeaders

    assert(
      headers("X-Taxonomy-ExtendedProperties").parseJson ==
        JsObject(
          "feature" -> JsString("synapseml"),
          "runtime" -> JsString("synapse_internal")
        )
    )
  }

  test("non-Fabric OpenAI requests omit SynapseML extended properties") {
    val headers = new InspectableOpenAIChatCompletion(
      isFabric = false,
      usesDefaultEndpoint = true
    ).requestHeaders

    assert(!headers.contains("X-Taxonomy-ExtendedProperties"))
  }

  test("explicit OpenAI endpoints omit SynapseML extended properties") {
    val headers = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = false
    ).requestHeaders

    assert(!headers.contains("X-Taxonomy-ExtendedProperties"))
  }

  test("custom OpenAI URL roots omit SynapseML extended properties") {
    val transformer = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = true
    ).setCustomUrlRoot("https://example.openai.azure.com/")

    assert(!transformer.requestHeaders.contains("X-Taxonomy-ExtendedProperties"))
  }
}
