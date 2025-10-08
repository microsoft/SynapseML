// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search.split2

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.services.openai.OpenAIEmbedding
import com.microsoft.azure.synapse.ml.services.search._
import com.microsoft.azure.synapse.ml.services.search.split1.SearchWriterSuiteUtilities
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait AzureSearchKey {
  lazy val azureSearchKey: String = sys.env.getOrElse("AZURE_SEARCH_KEY", Secrets.AzureSearchKey)
}

//scalastyle:off null
class SearchWriterSuite extends SearchWriterSuiteUtilities {

  import spark.implicits._

  test("pipeline with openai embedding") {
    val in = generateIndexName()

    val df = Seq(
      ("upload", "0", "this is the first sentence"),
      ("upload", "1", "this is the second sentence")
    ).toDF("searchAction", "id", "content")

    val tdf = new OpenAIEmbedding()
      .setSubscriptionKey(openAIAPIKey)
      .setDeploymentName("text-embedding-ada-002")
      .setCustomServiceName(openAIServiceName)
      .setTextCol("content")
      .setErrorCol("error")
      .setOutputCol("vectorContent")
      .transform(df)
      .drop("error")

    AzureSearchWriter.write(tdf,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexName" -> in,
        "keyCol" -> "id",
        "vectorCols" -> """[{"name": "vectorContent", "dimension": 1536}]"""
      ))

    retryWithBackoff(assertSize(in, 2))
    val indexJson = retryWithBackoff(getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, in))
    assert(parseIndexJson(indexJson).fields.find(_.name == "vectorContent").get.vectorSearchConfiguration.nonEmpty)
  }

  test("Handle Azure Search index with scoring profiles") {
    val indexJsonWithScoringProfile = """{"name": "test-index", "fields": [
      {"name": "id", "type": "Edm.String", "key": true},
      {"name": "date", "type": "Edm.DateTimeOffset"}
    ], "scoringProfiles": [{
      "name": "default_profiler", "functionAggregation": "sum",
      "functions": [{"type": "freshness", "boost": 2.0, "fieldName": "date",
        "interpolation": "constant", "freshness": {"boostingDuration": "P1D"}}]
    }]}"""
    val parsedIndex = parseIndexJson(indexJsonWithScoringProfile)
    assert(parsedIndex.scoringProfiles.isDefined)
    assert(parsedIndex.scoringProfiles.get.length == 1)
    val scoringProfile = parsedIndex.scoringProfiles.get.head
    assert(scoringProfile.name == "default_profiler")
    assert(scoringProfile.functionAggregation.contains("sum"))
    assert(scoringProfile.functions.isDefined)
    assert(scoringProfile.functions.get.length == 1)
    val function = scoringProfile.functions.get.head
    assert(function.`type` == "freshness")
    assert(function.boost.contains(2.0))
    assert(function.fieldName == "date")
    assert(function.freshness.isDefined)
    assert(function.freshness.get.boostingDuration == "P1D")
  }

  test("Handle date fields correctly") {
    val in = generateIndexName()
    import java.sql.{Date, Timestamp}
    val dateDF = Seq(
      ("upload", "0", "item0", Date.valueOf("2025-05-20"), Timestamp.valueOf("2025-05-20 10:30:00")),
      ("upload", "1", "item1", Date.valueOf("2025-05-21"), Timestamp.valueOf("2025-05-21 14:45:30")),
      ("upload", "2", "item2", null, null)
    ).toDF("searchAction", "id", "name", "createdDate", "lastModified")
    val indexJson = s"""{"name": "$in", "fields": [
      {"name": "id", "type": "Edm.String", "key": true, "facetable": false},
      {"name": "name", "type": "Edm.String", "searchable": true, "facetable": false},
      {"name": "createdDate", "type": "Edm.DateTimeOffset", "filterable": true, "sortable": true, "facetable": false},
      {"name": "lastModified", "type": "Edm.DateTimeOffset", "filterable": true, "sortable": true, "facetable": false}
    ]}"""
    AzureSearchWriter.write(dateDF, Map(
      "subscriptionKey" -> azureSearchKey, "actionCol" -> "searchAction",
      "serviceName" -> testServiceName, "indexJson" -> indexJson))
    retryWithBackoff(assertSize(in, 3))
    val createdIndexJson = retryWithBackoff(getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, in))
    val parsedIndex = parseIndexJson(createdIndexJson)
    assert(parsedIndex.fields.find(_.name == "createdDate").get.`type` == "Edm.DateTimeOffset")
    assert(parsedIndex.fields.find(_.name == "lastModified").get.`type` == "Edm.DateTimeOffset")
  }

  test("Date field conversion handles different timezones correctly") {
    val in = generateIndexName()
    import org.apache.spark.sql.functions._
    val dateDF = spark.createDataFrame(Seq(
      ("upload", "0", "2025-05-20", "2025-05-20 10:30:00"),
      ("upload", "1", "2025-05-21", "2025-05-21 14:45:30.123")
    )).toDF("searchAction", "id", "dateStr", "timestampStr")
      .withColumn("createdDate", to_date(col("dateStr")))
      .withColumn("lastModified", to_timestamp(col("timestampStr")))
      .drop("dateStr", "timestampStr")
    val indexJson = s"""{"name": "$in", "fields": [
      {"name": "id", "type": "Edm.String", "key": true},
      {"name": "createdDate", "type": "Edm.DateTimeOffset"},
      {"name": "lastModified", "type": "Edm.DateTimeOffset"}
    ]}"""
    AzureSearchWriter.write(dateDF, Map(
      "subscriptionKey" -> azureSearchKey, "actionCol" -> "searchAction",
      "serviceName" -> testServiceName, "indexJson" -> indexJson))
    retryWithBackoff(assertSize(in, 2))
  }

  test("Handle GeoJSON GeographyPoint fields") {

    val in = generateIndexName()
    val schema = StructType(Seq(
      StructField("searchAction", StringType),
      StructField("id", StringType),
      StructField("location", StructType(Seq(
        StructField("type", StringType, nullable = false),
        StructField("coordinates", ArrayType(DoubleType, containsNull = false), nullable = false)
      )))
    ))

    val rows = Seq(
      Row("upload", "0", Row("Point", Seq(-122.3493, 47.6205))),
      Row("upload", "1", Row("Point", Seq(-122.3351, 47.6080)))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val indexJson =
      s"""
         |{
         |  "name": "$in",
         |  "fields": [
         |    { "name": "id", "type": "Edm.String", "key": true, "searchable": true, "retrievable": true },
         |    { "name": "location", "type": "Edm.GeographyPoint", "searchable": false,
         |     "filterable": true, "retrievable": true, "sortable": true }
         |  ]
         |}
         |""".stripMargin

    AzureSearchWriter.write(df,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> indexJson
      )
    )

    // Test parser
    val createdIndexJson = retryWithBackoff(getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, in))
    val parsedIndex = parseIndexJson(createdIndexJson)
    assert(parsedIndex.fields.find(_.name == "location").get.`type` == "Edm.GeographyPoint")

    // Test writer
    retryWithBackoff(assertSize(in, 2))

  }

}
