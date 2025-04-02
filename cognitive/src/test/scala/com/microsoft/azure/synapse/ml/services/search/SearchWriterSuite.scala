// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.search

import com.microsoft.azure.synapse.ml.Secrets
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{ TestObject, TransformerFuzzing }
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.openai.{ OpenAIAPIKey, OpenAIEmbedding }
import com.microsoft.azure.synapse.ml.services.vision.AnalyzeImage
import org.apache.http.client.methods.HttpDelete
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.scalatest.{ Outcome, TestData }

import java.time.LocalDateTime
import java.time.format.{ DateTimeFormatter, DateTimeFormatterBuilder, DateTimeParseException, SignStyle }
import java.time.temporal.ChronoField
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.blocking

trait AzureSearchKey {
  lazy val azureSearchKey: String = sys.env.getOrElse("AZURE_SEARCH_KEY", Secrets.AzureSearchKey)
}

//scalastyle:off null
class SearchWriterSuite extends TestBase with AzureSearchKey with IndexJsonGetter with IndexParser
  with TransformerFuzzing[AddDocuments] with CognitiveKey with OpenAIAPIKey {

  import spark.implicits._

  private val testServiceName = "mmlspark-azure-search"

  // When a date pattern starts with 'yyyy' and has no separator following, the parser can sometimes decide
  // to take the whole string to match the year, which results in an exception. The following is a hackaround.
  val formatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendValue(ChronoField.YEAR_OF_ERA, 4, 4, SignStyle.EXCEEDS_PAD)
    .appendPattern("MMddHHmmssSSS").toFormatter()

  private def createTestData(numDocs: Int): DataFrame = {
    (0 until numDocs)
      .map(i => ("upload", s"$i", s"file$i", s"text$i"))
      .toDF("searchAction", "id", "fileName", "text")
  }

  private def createTestDataWithVector(numDocs: Int): DataFrame = {
    (0 until numDocs)
      .map(i => ("upload", s"$i", s"file$i", Array(0.001, 0.002, 0.003).map(_ * i)))
      .toDF("searchAction", "id", "fileName", "vectorCol")
  }

  private def createSimpleIndexJson(indexName: String): String = {
    s"""
       |{
       |    "name": "$indexName",
       |    "fields": [
       |      {
       |        "name": "id",
       |        "type": "Edm.String",
       |        "key": true,
       |        "facetable": false
       |      },
       |    {
       |      "name": "fileName",
       |      "type": "Edm.String",
       |      "searchable": false,
       |      "sortable": false,
       |      "facetable": false
       |    },
       |    {
       |      "name": "text",
       |      "type": "Edm.String",
       |      "filterable": false,
       |      "sortable": false,
       |      "facetable": false
       |    }
       |    ]
       |  }
    """.stripMargin
  }

  private def createSimpleIndexJsonWithVector(indexName: String): String = {
    s"""
       |{
       |    "name": "$indexName",
       |    "fields": [
       |      {
       |        "name": "id",
       |        "type": "Edm.String",
       |        "key": true,
       |        "facetable": false
       |      },
       |      {
       |        "name": "fileName",
       |        "type": "Edm.String",
       |        "searchable": false,
       |        "sortable": false,
       |        "facetable": false
       |      },
       |      {
       |        "name": "vectorCol",
       |        "type": "Collection(Edm.Single)",
       |        "dimensions": 3,
       |        "vectorSearchConfiguration": "vectorConfig"
       |      }
       |    ],
       |    "vectorSearch": {
       |         "algorithmConfigurations": [
       |           {
       |             "name": "vectorConfig",
       |             "kind": "hnsw"
       |           }
       |         ]
       |       }
       |  }
    """.stripMargin
  }

  private val createdIndexes = new mutable.HashMap[String, mutable.Set[String]]

  private val currentTestData = new ThreadLocal[TestData]

  override def withFixture(test: NoArgTest): Outcome = {
    currentTestData.set(test)
    try {
      super.withFixture(test)
    } finally {
      currentTestData.remove()
    }
  }

  private def generateIndexName(testName: Option[String] = None): String = {
    val testNameNormalized = if (testName.isEmpty || testName.get.isEmpty) {
      currentTestData.get().name
    } else {
      testName.get
    }

    val date = formatter.format(LocalDateTime.now())
    val name = s"test-${UUID.randomUUID().hashCode()}-${date}"
    createdIndexes.getOrElseUpdate(testNameNormalized, mutable.HashSet[String]()).+=(name)
    name
  }

  lazy val indexName: String = generateIndexName(Some("global"))

  override def beforeAll(): Unit = {
    // to ensure that we clean up old indexes at the start of the test
    // so we have enough capacity to create new ones
    cleanOldIndexes()
    println("WARNING CREATING SEARCH ENGINE!")
    SearchIndex.createIfNoneExists(azureSearchKey,
      testServiceName,
      createSimpleIndexJson(indexName))
  }

  def deleteIndex(indexName: String): Int = {
    val deleteRequest = new HttpDelete(
      s"https://$testServiceName.search.windows.net/indexes/$indexName?api-version=2017-11-11")
    deleteRequest.setHeader("api-key", azureSearchKey)
    val response = safeSend(deleteRequest, backoffs = List.fill(10)(1000 * 10)) // 10 seconds for each 10 retries
    response.getStatusLine.getStatusCode
  }

  override def afterAll(): Unit = {
    //TODO make this existing search indices when multiple builds are allowed
    println("Cleaning up services")
    val indexNames = this.createdIndexes.values.flatten
    println(s"Remaining indices: ${indexNames.mkString(",")}")
    cleanTestIndices(indexNames)
    cleanOldIndexes()
    super.afterAll()
    ()
  }

  override def afterEach(td: TestData): Unit = {
    val testName = td.name
    println(s"Cleaning up local test indices for test $testName")
    val indices = createdIndexes.get(testName)

    if (indices.isDefined) {
      println(s"Deleting indices ${indices.get.mkString(",")}")
      cleanTestIndices(indices.get)
      createdIndexes.remove(testName)
    } else {
      println(s"No indices found for test $testName")
    }
    super.afterEach(td)
  }

  private def cleanTestIndices(indices: Iterable[String]): Unit = {
    val successfulCleanup = getExisting(azureSearchKey, testServiceName)
      .intersect(indices.toSeq).map { n =>
        println(s"Deleting index $n")
        deleteIndex(n)
      }.forall(_ == 204)
    assert(successfulCleanup)
    ()
  }

  def cleanOldIndexes(): Unit = {
    import scala.util.matching.Regex

    val twoDaysAgo = LocalDateTime.now().minusDays(2)
    val endingDatePattern: Regex = "^.*-(\\d{17})$".r
    val e = getExisting(azureSearchKey, testServiceName)
    e.foreach {
      case name@endingDatePattern(dateString) =>
        try {
          val date = LocalDateTime.parse(dateString, formatter)
          if (date.isBefore(twoDaysAgo)) {
            deleteIndex(name)
          }
        } catch {
          case _: DateTimeParseException => {}
          case t: Throwable => throw t
        }
      case _ => {}
    }
  }

  private def retryWithBackoff[T](f: => T,
                                  timeouts: List[Long] =
                                  List(5000, 10000, 50000, 100000, 200000, 200000)): T = {
    try {
      f
    } catch {
      case _: Exception if timeouts.nonEmpty =>
        println(s"Sleeping for ${timeouts.head}")
        blocking {
          Thread.sleep(timeouts.head)
        }
        retryWithBackoff(f, timeouts.tail)
    }
  }

  lazy val df4: DataFrame = createTestData(4)
  lazy val df10: DataFrame = createTestData(10)
  lazy val bigDF: DataFrame = createTestData(10000)

  override val sortInDataframeEquality: Boolean = true

  lazy val ad: AddDocuments = {
    new AddDocuments()
      .setSubscriptionKey(azureSearchKey)
      .setServiceName(testServiceName)
      .setOutputCol("out").setErrorCol("err")
      .setIndexName(indexName)
      .setActionCol("searchAction")
  }

  override def testObjects(): Seq[TestObject[AddDocuments]] =
    Seq(new TestObject(ad, df4))

  override def reader: MLReadable[_] = AddDocuments

  def writeHelper(df: DataFrame,
                  indexName: String,
                  isVectorField: Boolean,
                  extraParams: Map[String, String] = Map()): Unit = {
    val indexJson = if (isVectorField) createSimpleIndexJsonWithVector(indexName) else createSimpleIndexJson(indexName)
    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> indexJson)
        ++ extraParams)
  }

  def assertSize(indexName: String, size: Int): Unit = {
    assert(SearchIndex.getStatistics(indexName, azureSearchKey, testServiceName)._1 == size)
    ()
  }

  ignore("clean up all search indexes") {
    getExisting(azureSearchKey, testServiceName)
      .foreach { n =>
        val deleteRequest = new HttpDelete(
          s"https://$testServiceName.search.windows.net/indexes/$n?api-version=2017-11-11")
        deleteRequest.setHeader("api-key", azureSearchKey)
        val response = safeSend(deleteRequest)
        println(s"Deleted index $n, status code ${response.getStatusLine.getStatusCode}")
      }
  }

  test("Run azure-search tests with waits") {
    val testsToRun = Set(1, 2) //, 3)

    def dependsOn(testNumber: Int, f: => Unit): Unit = {
      if (testsToRun(testNumber)) {
        println(s"Running code for test $testNumber")
        f
      }
    }

    //create new index and add docs
    lazy val in1 = generateIndexName()
    dependsOn(1, writeHelper(df4, in1, isVectorField=false))

    //push docs to existing index
    lazy val in2 = generateIndexName()
    lazy val dfA = df10.limit(4)
    lazy val dfB = df10.except(dfA)
    dependsOn(2, writeHelper(dfA, in2, isVectorField=false))

    dependsOn(2, retryWithBackoff({
      if (getExisting(azureSearchKey, testServiceName).contains(in2)) {
        writeHelper(dfB, in2, isVectorField=false)
      } else {
        throw new RuntimeException("No existing service found")
      }
    }))

    //push docs with custom batch size
    lazy val in3 = generateIndexName()
    dependsOn(3, writeHelper(bigDF, in3, isVectorField=false, Map("batchSize" -> "2000")))

    dependsOn(1, retryWithBackoff(assertSize(in1, 4)))
    dependsOn(2, retryWithBackoff(assertSize(in2, 10)))
    dependsOn(3, retryWithBackoff(assertSize(in3, 10000)))

  }

  test("Throw useful error when given badly formatted json") {
    val in = generateIndexName()
    val badJson =
      s"""
         |{
         |    "name": "$in",
         |    "fields": [
         |      {
         |        "name": "id",
         |        "type": "Edm.String",
         |        "key": true,
         |        "facetable": false
         |      },
         |    {
         |      "name": "someCollection",
         |      "type": "Collection(Edm.String)",
         |      "searchable": false,
         |      "sortable": true,
         |      "facetable": false
         |    },
         |    {
         |      "name": "text",
         |      "type": "Edm.String",
         |      "filterable": false,
         |      "sortable": false,
         |      "facetable": false
         |    }
         |    ]
         |  }
    """.stripMargin

    assertThrows[IllegalArgumentException] {
      SearchIndex.createIfNoneExists(azureSearchKey, testServiceName, badJson)
    }
  }

  test("Throw useful error when given mismatched schema and document fields") {
    val mismatchDF = (0 until 4)
      .map { i => ("upload", s"$i", s"file$i", s"text$i") }
      .toDF("searchAction", "badkeyname", "fileName", "text")
    assertThrows[IllegalArgumentException] {
      writeHelper(mismatchDF, generateIndexName(), isVectorField = false)
    }
  }

  /**
   * All the Edm Types are nullable in Azure Search except for Collection(Edm.String).
   * Because it is not possible to store a null value in a Collection(Edm.String) field,
   * there is an option to set a boolean flag, filterNulls, that will remove null values
   * from the dataset in the Collection(Edm.String) fields before writing the data to the search index.
   * The default value for this boolean flag is False.
   */
  test("Handle null values for Collection(Edm.String) fields") {
    val in = generateIndexName()
    val phraseIndex =
      s"""
         |{
         |    "name": "$in",
         |    "fields": [
         |      {
         |        "name": "id",
         |        "type": "Edm.String",
         |        "key": true,
         |        "facetable": false
         |      },
         |    {
         |      "name": "fileName",
         |      "type": "Edm.String",
         |      "searchable": false,
         |      "sortable": false,
         |      "facetable": false
         |    },
         |    {
         |      "name": "phrases",
         |      "type": "Collection(Edm.String)",
         |      "filterable": false,
         |      "sortable": false,
         |      "facetable": false
         |    }
         |    ]
         |  }
        """.stripMargin
    val phraseDF = Seq(
      ("upload", "0", "file0", Array("p1", "p2", "p3")),
      ("upload", "1", "file1", Array("p4", null, "p6")))
      .toDF("searchAction", "id", "fileName", "phrases")

    SearchIndex.createIfNoneExists(azureSearchKey,
      testServiceName,
      phraseIndex)

    AzureSearchWriter.write(phraseDF,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> phraseIndex,
        "filterNulls" -> "true"))

    retryWithBackoff(assertSize(in, 2))
  }

  test("Infer the structure of the index from the dataframe") {
    val in = generateIndexName()
    val phraseDF = Seq(
      ("upload", "0", "file0", Array("p1", "p2", "p3")),
      ("upload", "1", "file1", Array("p4", null, "p6")))
      .toDF("searchAction", "id", "fileName", "phrases")
    AzureSearchWriter.write(phraseDF,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "filterNulls" -> "true",
        "indexName" -> in,
        "keyCol" -> "id"
      ))

    retryWithBackoff(assertSize(in, 2))
  }

  test("pipeline with analyze image") {
    val in = generateIndexName()
    val df = Seq(
      ("upload", "0", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"),
      ("upload", "1", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg")
    ).toDF("searchAction", "id", "url")

    val tdf = new AnalyzeImage()
      .setSubscriptionKey(cognitiveKey)
      .setLocation("eastus")
      .setImageUrlCol("url")
      .setOutputCol("analyzed")
      .setErrorCol("errors")
      .setVisualFeatures(List("Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"))
      .transform(df)
      .select("*", "analyzed.*").drop("errors", "analyzed")

    AzureSearchWriter.write(tdf,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "filterNulls" -> "true",
        "indexName" -> in,
        "keyCol" -> "id"
      ))

    retryWithBackoff(assertSize(in, 2))
  }

  test("Run azure-search tests with vector fields") {
    val in1 = generateIndexName()
    val vectorDF4 = createTestDataWithVector(4)

    writeHelper(vectorDF4, in1, isVectorField=true)

    val in2 = generateIndexName()
    val vectorDF10 = createTestDataWithVector(10)
    val dfA = vectorDF10.limit(4)
    val dfB = vectorDF10.except(dfA)

    writeHelper(dfA, in2, isVectorField=true)

    retryWithBackoff({
      if (getExisting(azureSearchKey, testServiceName).contains(in2)) {
        writeHelper(dfB, in2, isVectorField=true)
      } else {
        throw new RuntimeException("No existing service found")
      }
    })

    retryWithBackoff(assertSize(in1, 4))
    retryWithBackoff(assertSize(in2, 10))

    val indexJson = retryWithBackoff(getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, in1))
    // assert if vectorCol is a vector field
    assert(parseIndexJson(indexJson).fields.find(_.name == "vectorCol").get.vectorSearchConfiguration.nonEmpty)
  }

  test("Infer the structure of the index from the dataframe with vector columns") {
    val in = generateIndexName()
    val phraseDF = Seq(
      ("upload", "0", "file0", Array(1.1, 2.1, 3.1), Vectors.dense(0.11, 0.21, 0.31),
        Vectors.sparse(3, Array(0, 1, 2), Array(0.11, 0.21, 0.31))),
      ("upload", "1", "file1", Array(1.2, 2.2, 3.2), Vectors.dense(0.12, 0.22, 0.32),
        Vectors.sparse(3, Array(0, 1, 2), Array(0.11, 0.21, 0.31))))
      .toDF("searchAction", "id", "fileName", "vectorCol1", "vectorCol2", "vectorCol3")

    val vectorCols =
      """
        |[
        |  {"name": "vectorCol1", "dimension": 3},
        |  {"name": "vectorCol2", "dimension": 3},
        |  {"name": "vectorCol3", "dimension": 3}
        |]
        |""".stripMargin

    AzureSearchWriter.write(phraseDF,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "filterNulls" -> "true",
        "indexName" -> in,
        "keyCol" -> "id",
        "vectorCols" -> vectorCols
      ))

    retryWithBackoff(assertSize(in, 2))

    // assert if vectorCols are a vector field
    val indexJson = retryWithBackoff(getIndexJsonFromExistingIndex(azureSearchKey, testServiceName, in))
    assert(parseIndexJson(indexJson).fields.find(_.name == "vectorCol1").get.vectorSearchConfiguration.nonEmpty)
    assert(parseIndexJson(indexJson).fields.find(_.name == "vectorCol2").get.vectorSearchConfiguration.nonEmpty)
    assert(parseIndexJson(indexJson).fields.find(_.name == "vectorCol3").get.vectorSearchConfiguration.nonEmpty)
  }

  test("Throw useful error when given vector columns in nested fields") {
    val in = generateIndexName()
    val badJson =
      s"""
         |{
         |  "name": "$in",
         |  "fields": [
         |    {
         |      "name": "id",
         |      "type": "Edm.String",
         |      "key": true,
         |      "facetable": false
         |    },
         |    {
         |      "name": "someCollection",
         |      "type": "Edm.String"
         |    },
         |    {
         |      "name": "complexField",
         |      "type": "Edm.ComplexType",
         |      "fields": [
         |        {
         |          "name": "StreetAddress",
         |          "type": "Edm.String"
         |        },
         |        {
         |          "name": "contentVector",
         |          "type": "Collection(Edm.Single)",
         |          "dimensions": 3,
         |          "vectorSearchConfiguration": "vectorConfig"
         |        }
         |      ]
         |    }
         |  ]
         |}
    """.stripMargin

    assertThrows[IllegalArgumentException] {
      AzureSearchWriter.write(df4,
        Map(
          "subscriptionKey" -> azureSearchKey,
          "actionCol" -> "searchAction",
          "serviceName" -> testServiceName,
          "filterNulls" -> "true",
          "indexJson" -> badJson
        ))
    }
  }

  test("Throw useful error when one of dimensions or vectorSearchConfig is not defined") {
    val in = generateIndexName()
    val badJson =
      s"""
         |{
         |  "name": "$in",
         |  "fields": [
         |    {
         |      "name": "id",
         |      "type": "Edm.String",
         |      "key": true,
         |      "facetable": false
         |    },
         |    {
         |      "name": "someCollection",
         |      "type": "Edm.String"
         |    },
         |    {
         |      "name": "contentVector",
         |      "type": "Collection(Edm.Single)",
         |      "dimensions": 3
         |    }
         |  ]
         |}
    """.stripMargin

    assertThrows[IllegalArgumentException] {
      SearchIndex.createIfNoneExists(azureSearchKey, testServiceName, badJson)
    }
  }

  test("Handle non-existent vector column specified in vectorCols option") {
    val in = generateIndexName()
    val phraseDF = Seq(
      ("upload", "0", "file0"),
      ("upload", "1", "file1"))
      .toDF("searchAction", "id", "fileName")

    AzureSearchWriter.write(phraseDF,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexName" -> in,
        "keyCol" -> "id",
        "vectorCols" -> """[{"name": "vectorCol", "dimension": 3}]"""
      ))

    retryWithBackoff(assertSize(in, 2))
  }

  test("Handle non-existing vector column specified in index JSON option") {
    val in = generateIndexName()
    val phraseDF = Seq(
      ("upload", "0", "file0"),
      ("upload", "1", "file1"))
      .toDF("searchAction", "id", "fileName")

    AzureSearchWriter.write(phraseDF,
      Map(
        "subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> createSimpleIndexJsonWithVector(in)
      ))

    retryWithBackoff(assertSize(in, 2))
  }

  test("Throw useful error when the vector column is an unsupported type") {
    val in = generateIndexName()
    val badDF = Seq(
      ("upload", "0", "file0", Array("p1", "p2", "p3")),
      ("upload", "1", "file1", Array("p4", "p5", "p6")))
      .toDF("searchAction", "id", "fileName", "vectorCol")

    assertThrows[AssertionError] {
      writeHelper(badDF, in, isVectorField=true)
    }
  }

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
}
