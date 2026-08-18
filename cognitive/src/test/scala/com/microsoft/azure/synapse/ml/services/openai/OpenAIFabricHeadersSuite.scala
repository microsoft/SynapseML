// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.common.PlatformDetails
import com.microsoft.azure.synapse.ml.services.HasCognitiveServiceInput
import org.apache.spark.sql.Row
import spray.json._

class OpenAIFabricHeadersSuite extends TestBase {

  private val fabricHeaderNames = Seq(
    "X-Taxonomy-TrafficType",
    "x-llm-service-tier",
    "X-Taxonomy-ExtendedProperties"
  )

  private trait InspectableFabricHeaders {
    self: OpenAIServicesBase with HasCognitiveServiceInput =>

    protected def isFabric: Boolean

    protected def usesDefaultEndpoint: Boolean

    override protected[openai] def runningOnFabric: Boolean = isFabric

    override protected[openai] def usingDefaultOpenAIEndpoint: Boolean = usesDefaultEndpoint

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

  private def assertFabricHeaders(
      headers: Map[String, String],
      expectedRuntime: String = PlatformDetails.FabricRuntime): Unit = {
    assert(headers("X-Taxonomy-TrafficType") == "Background")
    assert(headers("x-llm-service-tier") == "flex")
    assert(
      headers("X-Taxonomy-ExtendedProperties").parseJson ==
        JsObject(
          "feature" -> JsString("synapseml"),
          "runtime" -> JsString(expectedRuntime)
        )
    )
    fabricHeaderNames.foreach { headerName =>
      assert(headers.keys.count(_.equalsIgnoreCase(headerName)) == 1)
    }
  }

  private def assertNoFabricHeaders(headers: Map[String, String]): Unit = {
    fabricHeaderNames.foreach { headerName =>
      assert(!headers.keys.exists(_.equalsIgnoreCase(headerName)))
    }
  }

  test("Fabric classification headers are initialized once per runtime") {
    val first = OpenAIFabricHeaders.Values
    val second = OpenAIFabricHeaders.Values

    assert(first eq second)
    assertFabricHeaders(
      OpenAIFabricHeaders.build("fabric_spark_3.5.4"),
      expectedRuntime = "fabric_spark_3.5.4")
  }

  test("default Fabric OpenAI requests include SynapseML classification headers") {
    val transformer = new InspectableOpenAIChatCompletion(
      isFabric = true,
      usesDefaultEndpoint = true
    ).setCustomHeaders(Map(
      "x-taxonomy-traffictype" -> "caller",
      "X-LLM-SERVICE-TIER" -> "caller",
      "x-taxonomy-extendedproperties" -> """{"feature":"caller"}"""
    ))

    assertFabricHeaders(transformer.requestHeaders)
  }

  test("all OpenAI stages include SynapseML classification headers") {
    transformers(isFabric = true, usesDefaultEndpoint = true).foreach { transformer =>
      assertFabricHeaders(transformer.requestHeaders)
    }
  }

  test("non-Fabric OpenAI requests omit SynapseML classification headers") {
    transformers(isFabric = false, usesDefaultEndpoint = true).foreach { transformer =>
      assertNoFabricHeaders(transformer.requestHeaders)
    }
  }

  test("explicit OpenAI endpoints omit SynapseML classification headers") {
    transformers(isFabric = true, usesDefaultEndpoint = false).foreach { transformer =>
      assertNoFabricHeaders(transformer.requestHeaders)
    }
  }

  test("custom OpenAI URL roots omit SynapseML classification headers") {
    Seq("https://example.openai.azure.com/", " ").foreach { customUrlRoot =>
      transformers(isFabric = true, usesDefaultEndpoint = true).foreach { transformer =>
        transformer.withCustomUrlRoot(customUrlRoot)
        assertNoFabricHeaders(transformer.requestHeaders)
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
      "x-taxonomy-traffictype" -> "caller",
      "X-LLM-SERVICE-TIER" -> "caller",
      "x-taxonomy-extendedproperties" -> """{"feature":"caller"}"""
    ))
    val headers = transformer.requestHeaders

    assertFabricHeaders(headers)
    assert(headers.keys.forall(_ != null))
    assert(headers.values.forall(_ != null))
    assert(!headers.contains("Other"))
  }
}
