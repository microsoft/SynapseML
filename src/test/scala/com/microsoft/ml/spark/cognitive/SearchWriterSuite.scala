// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import java.util.UUID

import com.microsoft.ml.spark.Secrets
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.http.client.methods.HttpDelete
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.{DataFrame, Row}
import com.microsoft.ml.spark.cognitive.RESTHelpers._

import scala.collection.mutable
import scala.concurrent.blocking

trait AzureSearchKey {
  lazy val azureSearchKey = sys.env.getOrElse("AZURE_SEARCH_KEY", Secrets.AzureSearchKey)
}

class SearchWriterSuite extends TestBase with AzureSearchKey with IndexLister
  with TransformerFuzzing[AddDocuments] with VisionKey {

  import session.implicits._

  private val testServiceName = "mmlspark-azure-search"

  private def createTestData(numDocs: Int): DataFrame = {
    (0 until numDocs)
      .map(i => ("upload", s"$i", s"file$i", s"text$i"))
      .toDF("searchAction", "id", "fileName", "text")
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

  private val createdIndexes: mutable.ListBuffer[String] = mutable.ListBuffer()

  private def generateIndexName(): String = {
    val name = s"test-${UUID.randomUUID().hashCode()}"
    createdIndexes.append(name)
    name
  }

  override def afterAll(): Unit = {
    //TODO make this existing search indices when multiple builds are allowed
    println("Cleaning up services")
    val successfulCleanup = getExisting(azureSearchKey, testServiceName)
      .intersect(createdIndexes).map { n =>
      val deleteRequest = new HttpDelete(
        s"https://$testServiceName.search.windows.net/indexes/$n?api-version=2017-11-11")
      deleteRequest.setHeader("api-key", azureSearchKey)
      val response = safeSend(deleteRequest)
      response.getStatusLine.getStatusCode
    }.forall(_ == 204)
    super.afterAll()
    assert(successfulCleanup)
    ()
  }

  private def retryWithBackoff[T](f: => T,
                                  timeouts: List[Long] =
                                  List(5000, 10000, 50000, 100000, 200000, 200000)): T = {
    try {
      f
    } catch {
      case _: Exception if timeouts.nonEmpty =>
        println(s"Sleeping for ${timeouts.head}")
        blocking {Thread.sleep(timeouts.head)}
        retryWithBackoff(f, timeouts.tail)
    }
  }

  lazy val df4: DataFrame = createTestData(4)
  lazy val df10: DataFrame = createTestData(10)
  lazy val bigDF: DataFrame = createTestData(10000)

  override val sortInDataframeEquality: Boolean = true

  lazy val ad: AddDocuments = {
    val in = generateIndexName()
    SearchIndex.createIfNoneExists(azureSearchKey,
      testServiceName,
      createSimpleIndexJson(in))
    new AddDocuments()
      .setSubscriptionKey(azureSearchKey)
      .setServiceName(testServiceName)
      .setOutputCol("out").setErrorCol("err")
      .setIndexName(in)
      .setActionCol("searchAction")
  }

  override def testObjects(): Seq[TestObject[AddDocuments]] =
    Seq(new TestObject(ad, df4))

  override def reader: MLReadable[_] = AddDocuments

  def writeHelper(df: DataFrame,
                  indexName: String,
                  extraParams: Map[String, String] = Map()): Unit = {
    AzureSearchWriter.write(df,
      Map("subscriptionKey" -> azureSearchKey,
        "actionCol" -> "searchAction",
        "serviceName" -> testServiceName,
        "indexJson" -> createSimpleIndexJson(indexName)) ++ extraParams)
  }

  def assertSize(indexName: String, size: Int): Unit = {
    assert(SearchIndex.getStatistics(indexName, azureSearchKey, testServiceName)._1 == size)
    ()
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
    dependsOn(1, writeHelper(df4, in1))

    //push docs to existing index
    lazy val in2 = generateIndexName()
    lazy val dfA = df10.limit(4)
    lazy val dfB = df10.except(dfA)
    dependsOn(2, writeHelper(dfA, in2))

    dependsOn(2, retryWithBackoff({
      if (getExisting(azureSearchKey, testServiceName).contains(in2)) {
        writeHelper(dfB, in2)
      } else {
        throw new RuntimeException("No existing service found")
      }
    }))

    //push docs with custom batch size
    lazy val in3 = generateIndexName()
    dependsOn(3, writeHelper(bigDF, in3, Map("batchSize" -> "2000")))

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
      writeHelper(mismatchDF, generateIndexName())
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
    val phraseIndex = s"""
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

  test("pipeline with analyze image"){
    val in = generateIndexName()

    val df = Seq(
      ("upload", "0", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"),
      ("upload", "1", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg")
    ).toDF("searchAction", "id", "url")

    val tdf = new AnalyzeImage()
      .setSubscriptionKey(visionKey)
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

}
