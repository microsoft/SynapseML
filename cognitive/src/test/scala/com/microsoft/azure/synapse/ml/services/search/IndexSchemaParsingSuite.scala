// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import spray.json._

/** Regression coverage for index definitions that use the schema features the Search service
  * returns as objects rather than strings. See issue #2143: any index carrying a custom analyzer
  * failed to deserialize with "Expected String as JsString" before the writer ever sent a document.
  */
class IndexSchemaParsingSuite extends TestBase with IndexParser {

  private val keyField =
    """{"name": "Id", "type": "Edm.String", "key": true, "searchable": false}"""

  private def indexJson(extraMembers: String): String =
    s"""{"name": "test-index", "fields": [$keyField]${if (extraMembers.isEmpty) "" else ", " + extraMembers}}"""

  // Verbatim from the issue #2143 report.
  private val customAnalyzer =
    """"analyzers": [{
      |  "@odata.type": "#Microsoft.Azure.Search.CustomAnalyzer",
      |  "name": "keyword_analyzer",
      |  "tokenizer": "keyword_v2",
      |  "charFilters": [],
      |  "tokenFilters": ["lowercase"]
      |}]""".stripMargin

  test("parseIndexJson accepts an index with a custom analyzer") {
    val info = parseIndexJson(indexJson(customAnalyzer))
    assert(info.name.contains("test-index"))
    assert(info.analyzers.exists(_.length == 1))
    val analyzer = info.analyzers.get.head.asJsObject
    assert(analyzer.fields("name") == JsString("keyword_analyzer"))
    assert(analyzer.fields("@odata.type") == JsString("#Microsoft.Azure.Search.CustomAnalyzer"))
    assert(analyzer.fields("tokenFilters") == JsArray(JsString("lowercase")))
  }

  test("a custom analyzer survives a parse and re-serialize round trip") {
    val original = indexJson(customAnalyzer)
    val roundTripped = parseIndexJson(original).toJson.asJsObject
    // The service rejects an analyzer it cannot identify, so every member has to come back intact.
    assert(roundTripped.fields("analyzers") == original.parseJson.asJsObject.fields("analyzers"))
  }

  test("parseIndexJson accepts object-valued charFilters, tokenizers and tokenFilters") {
    val members =
      """"charFilters": [{"@odata.type": "#Microsoft.Azure.Search.MappingCharFilter",
        |  "name": "cf", "mappings": ["a=>b"]}],
        |"tokenizers": [{"@odata.type": "#Microsoft.Azure.Search.KeywordTokenizerV2", "name": "kw"}],
        |"tokenFilters": [{"@odata.type": "#Microsoft.Azure.Search.AsciiFoldingTokenFilter",
        |  "name": "af", "preserveOriginal": true}]""".stripMargin
    val info = parseIndexJson(indexJson(members))
    assert(info.charFilters.exists(_.length == 1))
    assert(info.tokenizers.exists(_.length == 1))
    assert(info.tokenFilters.exists(_.length == 1))
    assert(info.tokenFilters.get.head.asJsObject.fields("preserveOriginal") == JsTrue)
  }

  test("parseIndexJson accepts object-valued suggesters") {
    val members =
      """"suggesters": [{"name": "sg", "searchMode": "analyzingInfixMatching", "sourceFields": ["Id"]}]"""
    val info = parseIndexJson(indexJson(members))
    assert(info.suggesters.exists(_.length == 1))
    assert(info.suggesters.get.head.asJsObject.fields("name") == JsString("sg"))
  }

  test("parseIndexJson accepts corsOptions, which the service returns as an object not an array") {
    val members = """"corsOptions": {"allowedOrigins": ["*"], "maxAgeInSeconds": 300}"""
    val info = parseIndexJson(indexJson(members))
    assert(info.corsOptions.exists(_.asJsObject.fields("maxAgeInSeconds") == JsNumber(300)))
  }

  test("an index using every object-valued feature at once still parses") {
    val members = Seq(
      customAnalyzer,
      """"charFilters": [{"@odata.type": "#Microsoft.Azure.Search.MappingCharFilter",
        |  "name": "cf", "mappings": ["a=>b"]}]""".stripMargin,
      """"tokenizers": [{"@odata.type": "#Microsoft.Azure.Search.KeywordTokenizerV2", "name": "kw"}]""",
      """"tokenFilters": [{"@odata.type": "#Microsoft.Azure.Search.AsciiFoldingTokenFilter", "name": "af"}]""",
      """"suggesters": [{"name": "sg", "searchMode": "analyzingInfixMatching", "sourceFields": ["Id"]}]""",
      """"corsOptions": {"allowedOrigins": ["*"]}"""
    ).mkString(", ")
    val info = parseIndexJson(indexJson(members))
    assert(info.fields.length == 1)
    assert(info.analyzers.isDefined && info.charFilters.isDefined && info.tokenizers.isDefined)
    assert(info.tokenFilters.isDefined && info.suggesters.isDefined && info.corsOptions.isDefined)
  }

  test("omitting the optional members leaves them empty rather than failing") {
    val info = parseIndexJson(indexJson(""))
    assert(info.analyzers.isEmpty)
    assert(info.charFilters.isEmpty)
    assert(info.tokenizers.isEmpty)
    assert(info.tokenFilters.isEmpty)
    assert(info.suggesters.isEmpty)
    assert(info.corsOptions.isEmpty)
  }
}
