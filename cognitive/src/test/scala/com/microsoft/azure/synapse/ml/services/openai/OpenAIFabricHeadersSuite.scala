// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import org.apache.spark.sql.Row
import spray.json._

class OpenAIFabricHeadersSuite extends TestBase {

  private trait InspectableFabricHeaders {
    self: OpenAIServicesBase with HasCognitiveServiceInput =>

    protected def isFabric: Boolean

    protected def usesDefaultEndpoint: Boolean

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

    def withCustomUrlRoot(value: String): this.type = setCustomUrlRoot(value)

    def withRawCustomHeaders(value: Map[String, String]): this.type = {
      set(customHeaders, Left(value))
    }
  }

  private class InspectableOpenAIChatCompletion(
      override protected val isFabric: Boolean,
      override protected val usesDefaultEndpoint: Boolean)
    extends OpenAIChatCompletion with InspectableFabricHeaders

  private class InspectableOpenAIEmbedding(
      override protected val isFabric: Boolean,
      override protected val usesDefaultEndpoint: Boolean)
    extends OpenAIEmbedding with InspectableFabricHeaders

  private class InspectableOpenAIResponses(
      override protected val isFabric: Boolean,
      override protected val usesDefaultEndpoint: Boolean)
    extends OpenAIResponses with InspectableFabricHeaders

  private def transformers(
      isFabric: Boolean,
      usesDefaultEndpoint: Boolean): Seq[InspectableFabricHeaders] = {
    Seq(
      new InspectableOpenAIChatCompletion(isFabric, usesDefaultEndpoint),
      new InspectableOpenAIEmbedding(isFabric, usesDefaultEndpoint),
      new InspectableOpenAIResponses(isFabric, usesDefaultEndpoint)
    )
  }

  test("default Fabric OpenAI requests include SynapseML extended properties") {
    val transformer = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = true
    ).setCustomHeaders(Map(
      "x-taxonomy-extendedproperties" -> """{"feature":"caller"}"""
    ))
    val headers = transformer.requestHeaders

    assert(
      headers("X-Taxonomy-ExtendedProperties").parseJson ==
        JsObject(
          "feature" -> JsString("synapseml"),
          "runtime" -> JsString("synapse_internal")
        )
    )
    assert(headers.keys.count(_.equalsIgnoreCase("X-Taxonomy-ExtendedProperties")) == 1)
  }

  test("all OpenAI stages include SynapseML extended properties") {
    transformers(isFabric = true, usesDefaultEndpoint = true).foreach { transformer =>
      assert(
        transformer.requestHeaders("X-Taxonomy-ExtendedProperties").parseJson ==
          JsObject(
            "feature" -> JsString("synapseml"),
            "runtime" -> JsString("synapse_internal")
          )
      )
    }
  }

  test("non-Fabric OpenAI requests omit SynapseML extended properties") {
    transformers(isFabric = false, usesDefaultEndpoint = true).foreach { transformer =>
      assert(!transformer.requestHeaders.contains("X-Taxonomy-ExtendedProperties"))
    }
  }

  test("explicit OpenAI endpoints omit SynapseML extended properties") {
    transformers(isFabric = true, usesDefaultEndpoint = false).foreach { transformer =>
      assert(!transformer.requestHeaders.contains("X-Taxonomy-ExtendedProperties"))
    }
  }

  test("custom OpenAI URL roots omit SynapseML extended properties") {
    Seq("https://example.openai.azure.com/", " ").foreach { customUrlRoot =>
      transformers(isFabric = true, usesDefaultEndpoint = true).foreach { transformer =>
        transformer.withCustomUrlRoot(customUrlRoot)
        assert(!transformer.requestHeaders.contains("X-Taxonomy-ExtendedProperties"))
      }
    }
  }

  test("raw null custom headers are sanitized before Fabric attribution") {
    val nullString = Option.empty[String].orNull
    val transformer = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = true
    ).withRawCustomHeaders(Map(
      nullString -> "value",
      "Other" -> nullString,
      "x-taxonomy-extendedproperties" -> """{"feature":"caller"}"""
    ))
    val headers = transformer.requestHeaders

    assert(headers.keys.count(_.equalsIgnoreCase("X-Taxonomy-ExtendedProperties")) == 1)
    assert(headers.keys.forall(_ != null))
    assert(headers.values.forall(_ != null))
    assert(!headers.contains("Other"))
  }
}
