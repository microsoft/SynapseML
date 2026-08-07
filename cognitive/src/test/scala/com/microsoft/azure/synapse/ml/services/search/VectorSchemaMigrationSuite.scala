// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import org.apache.http.{HttpEntity, HttpVersion}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.entity.{BasicHttpEntity, StringEntity}
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

import java.io.{IOException, InputStream}

/** Secret-free coverage for the Azure AI Search vector schema migration. */
class VectorSchemaMigrationSuite extends AnyFunSuite {

  private val legacyIndexJson =
    """
      |{
      |  "name": "legacy-index",
      |  "fields": [
      |    { "name": "id", "type": "Edm.String", "key": true },
      |    {
      |      "name": "vectorCol",
      |      "type": "Collection(Edm.Single)",
      |      "dimensions": 3,
      |      "vectorSearchConfiguration": "hnswConfig",
      |      "futureFieldOption": { "mode": "kept" }
      |    }
      |  ],
      |  "vectorSearch": {
      |    "algorithmConfigurations": [
      |      {
      |        "name": "hnswConfig",
      |        "kind": "hnsw",
      |        "parameters": { "m": 4, "efConstruction": 400, "efSearch": 500, "metric": "cosine" },
      |        "futureAlgorithmOption": "kept"
      |      },
      |      {
      |        "name": "exhaustiveConfig",
      |        "kind": "exhaustiveKnn",
      |        "parameters": { "metric": "euclidean" }
      |      }
      |    ],
      |    "futureVectorSearchOption": { "enabled": true }
      |  },
      |  "futureRootOption": [1, 2, 3]
      |}
    """.stripMargin

  private val modernIndexJson =
    """
      |{
      |  "name": "modern-index",
      |  "fields": [
      |    { "name": "id", "type": "Edm.String", "key": true },
      |    {
      |      "name": "vectorCol",
      |      "type": "Collection(Edm.Single)",
      |      "searchable": true,
      |      "dimensions": 3,
      |      "vectorSearchProfile": "vectorProfile",
      |      "futureFieldOption": { "mode": "kept" }
      |    }
      |  ],
      |  "vectorSearch": {
      |    "algorithms": [
      |      {
      |        "name": "hnswConfig",
      |        "kind": "hnsw",
      |        "parameters": { "m": 8, "efConstruction": 500, "efSearch": 700, "metric": "cosine" },
      |        "futureAlgorithmOption": "kept"
      |      },
      |      {
      |        "name": "exhaustiveConfig",
      |        "kind": "exhaustiveKnn",
      |        "parameters": { "metric": "dotProduct" }
      |      }
      |    ],
      |    "profiles": [
      |      {
      |        "name": "vectorProfile",
      |        "algorithm": "hnswConfig",
      |        "vectorizer": "aoaiVectorizer",
      |        "compression": "scalarCompression",
      |        "futureProfileOption": 7
      |      }
      |    ],
      |    "vectorizers": [
      |      {
      |        "name": "aoaiVectorizer",
      |        "kind": "azureOpenAI",
      |        "azureOpenAIParameters": {
      |          "resourceUri": "https://example.openai.azure.com",
      |          "deploymentId": "embedding",
      |          "modelName": "text-embedding-3-small"
      |        },
      |        "futureVectorizerOption": true
      |      }
      |    ],
      |    "compressions": [
      |      {
      |        "name": "scalarCompression",
      |        "kind": "scalarQuantization",
      |        "rescoringOptions": { "enableRescoring": true, "defaultOversampling": 4.0 },
      |        "futureCompressionOption": "kept"
      |      }
      |    ],
      |    "futureVectorSearchOption": { "enabled": true }
      |  },
      |  "futureRootOption": { "mode": "kept" }
      |}
    """.stripMargin

  private def parse(json: String): JsValue = json.parseJson

  private def objectAt(value: JsValue, key: String): JsObject =
    value.asJsObject.fields(key).asJsObject

  private def vectorField(value: JsValue): JsObject =
    value.asJsObject.fields("fields").asInstanceOf[JsArray].elements
      .map(_.asJsObject)
      .find(_.fields.get("name").contains(JsString("vectorCol")))
      .get

  private class RecordingSearchIndexClient(existingIndexName: String,
                                           remoteIndexJson: String) extends SearchIndexClient {
    var listCalls = 0
    var getCalls = 0
    var createCalls = 0

    override def getExisting(auth: AzureSearchAuth,
                             serviceName: String,
                             apiVersion: String): Seq[String] = {
      listCalls += 1
      Seq(existingIndexName)
    }

    override def getIndexJson(auth: AzureSearchAuth,
                              serviceName: String,
                              indexName: String,
                              apiVersion: String): String = {
      getCalls += 1
      remoteIndexJson
    }

    override def createIndex(auth: AzureSearchAuth,
                             serviceName: String,
                             indexJson: String,
                             apiVersion: String): Int = {
      createCalls += 1
      201 // scalastyle:ignore magic.number
    }
  }

  private class TrackingResponse(responseEntity: HttpEntity)
    extends BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"))
      with CloseableHttpResponse { // scalastyle:ignore magic.number

    @volatile var closed = false
    setEntity(responseEntity)

    override def close(): Unit = closed = true
  }

  test("api version gate uses the 2023-10-01-Preview boundary") {
    Seq("2023-07-01-Preview", "2023-09-30", "2020-06-30")
      .foreach(v => assert(!AzureSearchAPIConstants.supportsVectorProfiles(v), s"$v should use legacy schema"))

    Seq("2023-10-01-Preview", "2023-10-01", "2023-11-01", "2024-03-01-preview", "2026-04-01")
      .foreach(v => assert(AzureSearchAPIConstants.supportsVectorProfiles(v), s"$v should use profiles"))

    assert(AzureSearchAPIConstants.supportsVectorProfiles(AzureSearchAPIConstants.DefaultAPIVersion))
  }

  test("invalid api versions fail explicitly") {
    Seq("", "not-a-version", "2023-13-01", "2023-10").foreach { version =>
      val error = intercept[IllegalArgumentException] {
        AzureSearchAPIConstants.supportsVectorProfiles(version)
      }
      assert(error.getMessage.contains("apiVersion"))
    }
  }

  test("published VectorSearch and IndexField case class shapes remain source compatible") {
    val algorithms = Seq(AlgorithmConfigs("vectorConfig", "hnsw"))
    val vectorSearch = VectorSearch(algorithms)
    val VectorSearch(extractedAlgorithms) = vectorSearch
    assert(extractedAlgorithms == algorithms)
    assert(vectorSearch.productArity == 1)

    val field = IndexField(
      "vectorCol", "Collection(Edm.Single)", None, None, None, None, None, None,
      None, None, None, None, None, Some(3), Some("vectorConfig"))
    val IndexField(name, fieldType, searchable, filterable, sortable, facetable, retrievable, key,
      analyzer, searchAnalyzer, indexAnalyzer, synonymMap, fields, dimensions, vectorConfiguration) = field

    assert(name == "vectorCol")
    assert(fieldType == "Collection(Edm.Single)")
    assert(Seq(searchable, filterable, sortable, facetable, retrievable, key).forall(_.isEmpty))
    assert(Seq(analyzer, searchAnalyzer, indexAnalyzer).forall(_.isEmpty))
    assert(synonymMap.isEmpty && fields.isEmpty)
    assert(dimensions.contains(3) && vectorConfiguration.contains("vectorConfig"))
    assert(field.copy(name = "copy").productArity == 15)
  }

  test("public parsers accept modern aliases without changing the published model") {
    val parsed = new IndexParser {}.parseIndexJson(modernIndexJson)
    val field = parsed.fields.find(_.name == "vectorCol").get

    assert(field.vectorSearchConfiguration.contains("vectorProfile"))
    assert(parsed.vectorSearch.get.algorithmConfigurations.map(_.name) ==
      Seq("hnswConfig", "exhaustiveConfig"))

    val publicJson = parsed.toJson.asJsObject
    assert(objectAt(publicJson, "vectorSearch").fields.contains("algorithmConfigurations"))
    assert(vectorField(publicJson).fields.contains("vectorSearchConfiguration"))
  }

  test("legacy JSON is modernized by renaming only required keys") {
    val original = parse(legacyIndexJson)
    val aligned = VectorSchema.align(original, "2023-10-01-Preview")
    val originalVectorSearch = objectAt(original, "vectorSearch")
    val alignedVectorSearch = objectAt(aligned, "vectorSearch")
    val alignedField = vectorField(aligned)

    assert(alignedVectorSearch.fields("algorithms") ==
      originalVectorSearch.fields("algorithmConfigurations"))
    assert(alignedVectorSearch.fields("futureVectorSearchOption") ==
      originalVectorSearch.fields("futureVectorSearchOption"))
    assert(alignedVectorSearch.fields("profiles") == JsArray(Vector(
      JsObject("name" -> JsString("hnswConfig"), "algorithm" -> JsString("hnswConfig")),
      JsObject("name" -> JsString("exhaustiveConfig"), "algorithm" -> JsString("exhaustiveConfig")))))
    assert(!alignedVectorSearch.fields.contains("algorithmConfigurations"))

    assert(alignedField.fields("vectorSearchProfile") == JsString("hnswConfig"))
    assert(alignedField.fields("searchable").convertTo[Boolean])
    assert(alignedField.fields("futureFieldOption") == vectorField(original).fields("futureFieldOption"))
    assert(!alignedField.fields.contains("vectorSearchConfiguration"))
    assert(aligned.asJsObject.fields("futureRootOption") == original.asJsObject.fields("futureRootOption"))
  }

  test("the actual REST entity preserves complete modern vector JSON") {
    val original = parse(modernIndexJson)
    val entity = parse(SearchIndex.prepareEntity(modernIndexJson, "2026-04-01"))

    assert(entity == original)
    val vectorSearchKeys = objectAt(entity, "vectorSearch").fields.keySet
    assert(Seq("algorithms", "profiles", "vectorizers", "compressions").forall(vectorSearchKeys))
  }

  test("legacy REST preparation preserves parameters and unknown fields") {
    val prepared = parse(SearchIndex.prepareEntity(legacyIndexJson, "2026-04-01"))
    val originalVectorSearch = objectAt(parse(legacyIndexJson), "vectorSearch")
    val preparedVectorSearch = objectAt(prepared, "vectorSearch")

    assert(preparedVectorSearch.fields("algorithms") ==
      originalVectorSearch.fields("algorithmConfigurations"))
    assert(preparedVectorSearch.fields("futureVectorSearchOption") ==
      originalVectorSearch.fields("futureVectorSearchOption"))
    assert(prepared.asJsObject.fields("futureRootOption") ==
      parse(legacyIndexJson).asJsObject.fields("futureRootOption"))
  }

  test("legacy api path is explicit and refuses lossy modern downgrades") {
    val legacy = parse(legacyIndexJson)
    assert(VectorSchema.align(legacy, "2023-07-01-Preview") == legacy)

    val error = intercept[IllegalArgumentException] {
      VectorSchema.align(parse(modernIndexJson), "2023-07-01-Preview")
    }
    assert(error.getMessage.contains("cannot be losslessly sent"))
    assert(error.getMessage.contains("2023-10-01-Preview"))
  }

  test("existing legacy indexes fail early instead of implying an automatic migration") {
    val error = intercept[IllegalArgumentException] {
      VectorSchema.requireCompatibleExistingIndex(parse(legacyIndexJson), "2026-04-01")
    }
    assert(error.getMessage.contains("createIfNoneExists does not update or migrate"))
    assert(error.getMessage.contains("apiVersion=2023-07-01-Preview"))
    assert(error.getMessage.contains("Create or Update Index"))

    VectorSchema.requireCompatibleExistingIndex(parse(legacyIndexJson), "2023-07-01-Preview")
    VectorSchema.requireCompatibleExistingIndex(parse(modernIndexJson), "2026-04-01")
  }

  test("modern API responses that normalize legacy indexes still fail early") {
    val normalizedLegacyIndex =
      """
        |{
        |  "name": "legacy-index",
        |  "fields": [
        |    { "name": "id", "type": "Edm.String", "key": true, "dimensions": null },
        |    {
        |      "name": "vectorCol",
        |      "type": "Collection(Edm.Single)",
        |      "dimensions": 3,
        |      "vectorSearchProfile": null
        |    }
        |  ],
        |  "vectorSearch": {
        |    "algorithms": [ { "name": "hnswConfig", "kind": "hnsw" } ],
        |    "profiles": []
        |  }
        |}
      """.stripMargin

    val error = intercept[IllegalArgumentException] {
      VectorSchema.requireCompatibleExistingIndex(parse(normalizedLegacyIndex), "2026-04-01")
    }

    assert(error.getMessage.contains("legacy vector schema"))
    assert(error.getMessage.contains("apiVersion=2023-07-01-Preview"))
  }

  test("existing modern indexes reject a legacy api version") {
    val error = intercept[IllegalArgumentException] {
      VectorSchema.requireCompatibleExistingIndex(parse(modernIndexJson), "2023-07-01-Preview")
    }
    assert(error.getMessage.contains("profile-based vector schema"))
    assert(error.getMessage.contains("2023-10-01-Preview or later"))
  }

  test("createIfNoneExists validates the actual remote index with one list and one GET") {
    val remoteLegacyJson = legacyIndexJson.replace("\"legacy-index\"", "\"modern-index\"")
    val client = new RecordingSearchIndexClient("modern-index", remoteLegacyJson)

    val error = intercept[IllegalArgumentException] {
      SearchIndex.createIfNoneExists(
        AzureSearchAuth(), "service", modernIndexJson, "2026-04-01", client)
    }

    assert(error.getMessage.contains("legacy vector schema"))
    assert(client.listCalls == 1)
    assert(client.getCalls == 1)
    assert(client.createCalls == 0)
  }

  test("internal index GET closes responses after successful and failed reads") {
    val successResponse = new TrackingResponse(new StringEntity("""{"name":"index"}"""))
    val successJson = IndexJsonReader.read(
      new HttpGet("https://example.test/index"), _ => successResponse)

    assert(successJson == """{"name":"index"}""")
    assert(successResponse.closed)

    val failingEntity = new BasicHttpEntity()
    failingEntity.setContent(new InputStream {
      override def read(): Int = throw new IOException("expected read failure")
    })
    val failingResponse = new TrackingResponse(failingEntity)

    assertThrows[IOException] {
      IndexJsonReader.read(new HttpGet("https://example.test/failing-index"), _ => failingResponse)
    }
    assert(failingResponse.closed)
  }

  test("nested fields are modernized without dropping unrelated content") {
    val nested =
      """
        |{
        |  "name": "nested-index",
        |  "fields": [
        |    {
        |      "name": "parent",
        |      "type": "Edm.ComplexType",
        |      "futureParentOption": true,
        |      "fields": [
        |        {
        |          "name": "vectorCol",
        |          "type": "Collection(Edm.Single)",
        |          "dimensions": 3,
        |          "vectorSearchConfiguration": "vectorConfig",
        |          "futureChildOption": "kept"
        |        }
        |      ]
        |    }
        |  ],
        |  "vectorSearch": {
        |    "algorithmConfigurations": [ { "name": "vectorConfig", "kind": "hnsw" } ]
        |  }
        |}
      """.stripMargin

    val aligned = VectorSchema.align(parse(nested), "2026-04-01")
    val parent = aligned.asJsObject.fields("fields").asInstanceOf[JsArray].elements.head.asJsObject
    val child = parent.fields("fields").asInstanceOf[JsArray].elements.head.asJsObject

    assert(parent.fields("futureParentOption").convertTo[Boolean])
    assert(child.fields("futureChildOption") == JsString("kept"))
    assert(child.fields("vectorSearchProfile") == JsString("vectorConfig"))
    assert(child.fields("searchable").convertTo[Boolean])
  }

  test("non-vector indexes are unchanged for both schema generations") {
    val plain = parse(
      """{"name":"plain-index","fields":[{"name":"id","type":"Edm.String","key":true}],"future":7}""")

    assert(VectorSchema.align(plain, "2026-04-01") == plain)
    assert(VectorSchema.align(plain, "2023-07-01-Preview") == plain)
  }
}
