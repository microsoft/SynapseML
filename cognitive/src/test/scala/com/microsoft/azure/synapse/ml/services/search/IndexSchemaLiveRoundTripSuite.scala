// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import org.apache.http.client.methods.HttpDelete
import spray.json._

import java.util.UUID

/** End-to-end companion to [[IndexSchemaParsingSuite]].
  *
  * The offline suite proves the parser accepts the payload shapes reported in issue #2143, but it
  * asserts against hand-written JSON. This suite creates a real index that exercises every schema
  * feature the service returns as an object, reads it back through the same production code path
  * the writer uses, and parses it. That closes the gap where a hand-written fixture could drift
  * from what the Search service actually emits.
  */
class IndexSchemaLiveRoundTripSuite extends TestBase with IndexJsonGetter with IndexParser {

  private lazy val azureSearchKey: String = sys.env.getOrElse("AZURE_SEARCH_KEY", Secrets.AzureSearchKey)
  private val testServiceName = "mmlspark-azure-search"

  private def indexDefinition(name: String): String =
    s"""
       |{
       |  "name": "$name",
       |  "fields": [
       |    { "name": "id", "type": "Edm.String", "key": true, "searchable": true },
       |    { "name": "text", "type": "Edm.String", "searchable": true, "analyzer": "sml_custom_analyzer" },
       |    { "name": "title", "type": "Edm.String", "searchable": true }
       |  ],
       |  "corsOptions": { "allowedOrigins": ["*"], "maxAgeInSeconds": 300 },
       |  "suggesters": [
       |    { "name": "sml_suggester", "searchMode": "analyzingInfixMatching", "sourceFields": ["title"] }
       |  ],
       |  "analyzers": [
       |    {
       |      "@odata.type": "#Microsoft.Azure.Search.CustomAnalyzer",
       |      "name": "sml_custom_analyzer",
       |      "tokenizer": "sml_tokenizer",
       |      "tokenFilters": ["sml_asciifolding"],
       |      "charFilters": ["sml_mapping"]
       |    }
       |  ],
       |  "tokenizers": [
       |    { "@odata.type": "#Microsoft.Azure.Search.KeywordTokenizerV2", "name": "sml_tokenizer" }
       |  ],
       |  "tokenFilters": [
       |    { "@odata.type": "#Microsoft.Azure.Search.AsciiFoldingTokenFilter",
       |      "name": "sml_asciifolding", "preserveOriginal": true }
       |  ],
       |  "charFilters": [
       |    { "@odata.type": "#Microsoft.Azure.Search.MappingCharFilter",
       |      "name": "sml_mapping", "mappings": ["a=>b"] }
       |  ]
       |}
       |""".stripMargin

  private def deleteIndex(name: String): Unit = {
    val apiVersion = AzureSearchAPIConstants.DefaultAPIVersion
    val deleteRequest = new HttpDelete(
      s"https://$testServiceName.search.windows.net/indexes/$name?api-version=$apiVersion")
    deleteRequest.setHeader("api-key", azureSearchKey)
    safeSend(deleteRequest)
    ()
  }

  test("An index using object-valued schema features round-trips through the live service") {
    val indexName = s"test-schema-${UUID.randomUUID().toString.take(8)}"
    SearchIndex.createIfNoneExists(azureSearchKey, testServiceName, indexDefinition(indexName))
    try {
      // Read back through the same path AzureSearchWriter uses, so this fails if the service
      // emits a shape the production parser rejects.
      val liveJson = getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, indexName)
      val info = parseIndexJson(liveJson)

      assert(info.name.contains(indexName))
      // The service returns these as objects; typing them as strings is what broke issue #2143.
      assert(info.analyzers.exists(_.length == 1))
      assert(info.analyzers.get.head.asJsObject.fields("name") == JsString("sml_custom_analyzer"))
      assert(info.tokenizers.exists(_.length == 1))
      assert(info.tokenFilters.exists(_.length == 1))
      assert(info.charFilters.exists(_.length == 1))
      assert(info.suggesters.exists(_.length == 1))
      // corsOptions is a single object rather than an array.
      assert(info.corsOptions.exists(_.asJsObject.fields("maxAgeInSeconds") == JsNumber(300)))
    } finally {
      deleteIndex(indexName)
    }
  }

}
