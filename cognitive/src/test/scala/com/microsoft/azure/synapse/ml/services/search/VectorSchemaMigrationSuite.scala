// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.services.search.AzureSearchProtocol._
import org.scalatest.funsuite.AnyFunSuite
import spray.json._

/** Secret-free coverage for the Azure AI Search vector schema migration.
  *
  * `2023-11-01` renamed `vectorSearch.algorithmConfigurations` to `vectorSearch.algorithms`, added
  * `vectorSearch.profiles`, and replaced field-level `vectorSearchConfiguration` with
  * `vectorSearchProfile`. These tests pin both directions of that translation plus the api-version
  * gate that selects it.
  */
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
      |      "vectorSearchConfiguration": "vectorConfig"
      |    }
      |  ],
      |  "vectorSearch": {
      |    "algorithmConfigurations": [ { "name": "vectorConfig", "kind": "hnsw" } ]
      |  }
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
      |      "vectorSearchProfile": "vectorProfile"
      |    }
      |  ],
      |  "vectorSearch": {
      |    "algorithms": [ { "name": "vectorConfig", "kind": "hnsw" } ],
      |    "profiles": [ { "name": "vectorProfile", "algorithm": "vectorConfig" } ]
      |  }
      |}
    """.stripMargin

  private def parse(json: String): IndexInfo = json.parseJson.convertTo[IndexInfo]

  private def vectorField(index: IndexInfo): IndexField =
    index.fields.find(_.name == "vectorCol").get

  test("default api version is a supported, non-deprecated version") {
    // 2023-07-01-Preview was deprecated 2024-04-08 and unsupported from 2024-07-08.
    assert(AzureSearchAPIConstants.DefaultAPIVersion != "2023-07-01-Preview")
    assert(AzureSearchAPIConstants.supportsVectorProfiles(AzureSearchAPIConstants.DefaultAPIVersion))
  }

  test("api version gate selects the correct vector schema generation") {
    Seq("2023-11-01", "2024-07-01", "2025-09-01", "2026-04-01", "2024-03-01-preview")
      .foreach(v => assert(AzureSearchAPIConstants.supportsVectorProfiles(v), s"$v should use profiles"))

    Seq("2023-07-01-Preview", "2023-10-01-Preview", "2020-06-30", "2019-05-06")
      .foreach(v => assert(!AzureSearchAPIConstants.supportsVectorProfiles(v), s"$v should use legacy schema"))
  }

  test("legacy index json is upgraded to the profile based schema") {
    val aligned = VectorSchema.align(parse(legacyIndexJson), "2026-04-01")
    val field = vectorField(aligned)

    assert(field.vectorSearchProfile.contains("vectorConfig"))
    assert(field.vectorSearchConfiguration.isEmpty)
    // Vector fields must be searchable in 2023-11-01 and later.
    assert(field.searchable.contains(true))

    val vectorSearch = aligned.vectorSearch.get
    assert(vectorSearch.algorithms.get.map(_.name) == Seq("vectorConfig"))
    assert(vectorSearch.algorithmConfigurations.isEmpty)
    // A same-named profile keeps the pre-existing field reference resolvable.
    assert(vectorSearch.profiles.get == Seq(VectorProfile("vectorConfig", "vectorConfig")))
  }

  test("modern index json is downgraded when an older api version is pinned") {
    val aligned = VectorSchema.align(parse(modernIndexJson), "2023-07-01-Preview")
    val field = vectorField(aligned)

    // The field referenced a profile; the legacy schema needs the underlying algorithm name.
    assert(field.vectorSearchConfiguration.contains("vectorConfig"))
    assert(field.vectorSearchProfile.isEmpty)
    // Pre-2023-11-01 rejects searchable vector fields.
    assert(field.searchable.isEmpty)

    val vectorSearch = aligned.vectorSearch.get
    assert(vectorSearch.algorithmConfigurations.get.map(_.name) == Seq("vectorConfig"))
    assert(vectorSearch.algorithms.isEmpty)
    assert(vectorSearch.profiles.isEmpty)
  }

  test("alignment is idempotent in both directions") {
    val modern = VectorSchema.align(parse(modernIndexJson), "2026-04-01")
    assert(VectorSchema.align(modern, "2026-04-01") == modern)

    val legacy = VectorSchema.align(parse(legacyIndexJson), "2023-07-01-Preview")
    assert(VectorSchema.align(legacy, "2023-07-01-Preview") == legacy)
  }

  test("round tripping legacy json through the modern schema preserves the vector binding") {
    val modern = VectorSchema.align(parse(legacyIndexJson), "2026-04-01")
    val backToLegacy = VectorSchema.align(modern, "2023-07-01-Preview")

    assert(vectorField(backToLegacy).vectorSearchConfiguration.contains("vectorConfig"))
    assert(backToLegacy.vectorSearch.get.algorithmConfigurations.get.map(_.name) == Seq("vectorConfig"))
  }

  test("both schema generations parse and expose a single vector reference") {
    assert(vectorField(parse(legacyIndexJson)).vectorReference.contains("vectorConfig"))
    assert(vectorField(parse(modernIndexJson)).vectorReference.contains("vectorProfile"))
    assert(vectorField(parse(legacyIndexJson)).isVectorField)
    assert(vectorField(parse(modernIndexJson)).isVectorField)
  }

  test("non vector indexes are unchanged by alignment") {
    val json =
      """
        |{
        |  "name": "plain-index",
        |  "fields": [ { "name": "id", "type": "Edm.String", "key": true } ]
        |}
      """.stripMargin
    val parsed = parse(json)

    assert(VectorSchema.align(parsed, "2026-04-01") == parsed)
    assert(VectorSchema.align(parsed, "2019-05-06") == parsed)
    assert(!parsed.fields.head.isVectorField)
  }

  test("serialized modern index omits legacy vector keys") {
    val serialized = VectorSchema.align(parse(legacyIndexJson), "2026-04-01").toJson.compactPrint

    assert(serialized.contains("\"vectorSearchProfile\""))
    assert(serialized.contains("\"algorithms\""))
    assert(serialized.contains("\"profiles\""))
    assert(!serialized.contains("\"vectorSearchConfiguration\""))
    assert(!serialized.contains("\"algorithmConfigurations\""))
  }

  test("nested vector fields are translated as well") {
    val json =
      """
        |{
        |  "name": "nested-index",
        |  "fields": [
        |    { "name": "id", "type": "Edm.String", "key": true },
        |    {
        |      "name": "parent",
        |      "type": "Edm.ComplexType",
        |      "fields": [
        |        {
        |          "name": "childVector",
        |          "type": "Collection(Edm.Single)",
        |          "dimensions": 3,
        |          "vectorSearchConfiguration": "vectorConfig"
        |        }
        |      ]
        |    }
        |  ],
        |  "vectorSearch": {
        |    "algorithmConfigurations": [ { "name": "vectorConfig", "kind": "hnsw" } ]
        |  }
        |}
      """.stripMargin

    val aligned = VectorSchema.align(parse(json), "2026-04-01")
    val child = aligned.fields.find(_.name == "parent").get.fields.get.head

    assert(child.vectorSearchProfile.contains("vectorConfig"))
    assert(child.vectorSearchConfiguration.isEmpty)
  }
}
