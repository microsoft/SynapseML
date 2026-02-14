// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.spark.FluentAPI._
import com.microsoft.azure.synapse.ml.core.test.base.{Flaky, TestBase}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers.retry
import com.microsoft.azure.synapse.ml.services._
import com.microsoft.azure.synapse.ml.services.testutils.ImageDownloadUtils
import com.microsoft.azure.synapse.ml.services.form.FormsFlatteners._
import com.microsoft.azure.synapse.ml.stages.UDFTransformer
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import spray.json._

import java.net.URI
import java.time.{ZoneOffset, ZonedDateTime}
import scala.annotation.tailrec

object TrainCustomModelProtocol extends DefaultJsonProtocol {
  implicit val SourceFilterEnc: RootJsonFormat[SourceFilter] = jsonFormat2(SourceFilter)
  implicit val TrainCustomModelEnc: RootJsonFormat[TrainCustomModelSchema] = jsonFormat3(TrainCustomModelSchema)
}

import com.microsoft.azure.synapse.ml.services.form.TrainCustomModelProtocol._

case class TrainCustomModelSchema(source: String, sourceFilter: SourceFilter, useLabelFile: Boolean)

case class SourceFilter(prefix: String, includeSubFolders: Boolean)

object FormRecognizerUtils extends CognitiveKey {

  import com.microsoft.azure.synapse.ml.io.http.RESTHelpers._

  val PollingDelay = 1000

  def formSend(request: HttpRequestBase, path: String,
               params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", cognitiveKey)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(s"Failed: response: $response " + s"requestUrl: ${request.getURI}" +
            s"requestBody: $bodyOpt")
        }
        if (response.getStatusLine.getReasonPhrase == "No Content") {
          ""
        }
        else if (response.getStatusLine.getReasonPhrase == "Created") {
          response.getHeaders("Location").head.getValue
        }
        else {
          IOUtils.toString(response.getEntity.getContent, "UTF-8")
        }
      }.get
    })
  }

  def formDelete(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpDelete(),
      "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models/" + path, params)
  }

  def formPost(path: String, body: TrainCustomModelSchema, params: Map[String, String] = Map())
              (implicit format: JsonFormat[TrainCustomModelSchema]): String = {
    val post = new HttpPost()
    post.setEntity(new StringEntity(body.toJson.compactPrint))
    formSend(post, "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models" + path, params)
  }

  def formGet(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpGet(), path, params)
  }
}

trait FormRecognizerUtils extends TestBase with CognitiveKey with Flaky with ImageDownloadUtils {

  import spark.implicits._

  def createTestDataframe(baseUrl: String, docs: Seq[String], returnBytes: Boolean): DataFrame = {
    val df = docs.map(doc => baseUrl + doc).toDF("source")
    if (returnBytes) {
      df.withColumn("imageBytes", downloadBytesUdf(col("source")))
        .select("imageBytes")
    } else {
      df
    }
  }

  val baseUrl = "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/"

  lazy val imageDf1: DataFrame = createTestDataframe(baseUrl, Seq("layout1.jpg"), returnBytes = false)

  lazy val bytesDF1: DataFrame = createTestDataframe(baseUrl, Seq("layout1.jpg"), returnBytes = true)

  lazy val imageDf2: DataFrame = createTestDataframe(baseUrl, Seq("receipt1.png"), returnBytes = false)

  lazy val bytesDF2: DataFrame = createTestDataframe(baseUrl, Seq("receipt1.png"), returnBytes = true)

  lazy val imageDf3: DataFrame = createTestDataframe(baseUrl, Seq("business_card.jpg"), returnBytes = false)

  lazy val bytesDF3: DataFrame = createTestDataframe(baseUrl, Seq("business_card.jpg"), returnBytes = true)

  lazy val imageDf4: DataFrame = createTestDataframe(baseUrl, Seq("invoice2.png"), returnBytes = false)

  lazy val bytesDF4: DataFrame = createTestDataframe(baseUrl, Seq("invoice2.png"), returnBytes = true)

  lazy val imageDf5: DataFrame = createTestDataframe(baseUrl, Seq("id1.jpg"), returnBytes = false)

  lazy val bytesDF5: DataFrame = createTestDataframe(baseUrl, Seq("id1.jpg"), returnBytes = true)

  lazy val imageDf6: DataFrame = createTestDataframe(baseUrl, Seq("tables1.pdf"), returnBytes = false)

  lazy val pdfDf1: DataFrame = createTestDataframe(baseUrl, Seq("layout2.pdf"), returnBytes = false)

  lazy val pdfDf2: DataFrame = createTestDataframe(baseUrl, Seq("invoice1.pdf", "invoice3.pdf"), returnBytes = false)

  lazy val pathDf: DataFrame = createTestDataframe(baseUrl, Seq(""), returnBytes = false)

  // TODO refactor tests to share structure
  def basicTest(df: DataFrame,
                method: Transformer with FormRecognizerBase,
                flatteners: Seq[UDFTransformer],
                truePrefixes: Seq[String]): Unit = {
    val results = df.mlTransform(Seq(method) ++ flatteners: _*)
      .select(flatteners.map(f => col(f.getOutputCol)): _*)
      .collect()
    flatteners.indices.zip(truePrefixes).foreach { case (i, truePrefix) =>
      assert(results.head.getString(i).startsWith(truePrefix))
    }
  }
}

class AnalyzeLayoutSuite extends TransformerFuzzing[AnalyzeLayout] with FormRecognizerUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val analyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("layout").setConcurrency(5)

  lazy val bytesAnalyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("layout").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "layout.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf1.mlTransform(analyzeLayout,
        flattenReadResults("layout", "readlayout"),
        flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512 " +
      "Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Details | Quantity | Unit Price | Total"))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf1.mlTransform(analyzeLayout,
        flattenReadResults("layout", "readlayout"),
        flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    val correctPrefix = "UNITED STATES SECURITIES AND EXCHANGE COMMISSION Washington, D.C. 20549 FORM 10-Q"
    assert(headStr.startsWith(correctPrefix))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Title of each class | Trading Symbol | Name of exchange on which registered "))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF1.mlTransform(bytesAnalyzeLayout,
        flattenReadResults("layout", "readlayout"),
        flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512" +
      " Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr.contains("Tables: Details | Quantity | Unit Price | Total"))

  }

  override def testObjects(): Seq[TestObject[AnalyzeLayout]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeLayout
}

class AnalyzeReceiptsSuite extends TransformerFuzzing[AnalyzeReceipts] with FormRecognizerUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val analyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("receipts").setConcurrency(5)

  lazy val bytesAnalyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("receipts").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "receipts.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf2.mlTransform(analyzeReceipts,
        flattenReadResults("receipts", "readReceipts"),
        flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("Tax"))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF2.mlTransform(bytesAnalyzeReceipts,
        flattenReadResults("receipts", "readReceipts"),
        flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("Tax"))
  }

  override def testObjects(): Seq[TestObject[AnalyzeReceipts]] =
    Seq(new TestObject(analyzeReceipts, imageDf2))

  override def reader: MLReadable[_] = AnalyzeReceipts
}

class AnalyzeBusinessCardsSuite extends TransformerFuzzing[AnalyzeBusinessCards] with FormRecognizerUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val analyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("businessCards").setConcurrency(5)

  lazy val bytesAnalyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("businessCards").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "businessCards.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf3.mlTransform(analyzeBusinessCards,
        flattenReadResults("businessCards", "readBusinessCards"),
        flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":["{\"type\":\"string\",\"valueString""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF3.mlTransform(bytesAnalyzeBusinessCards,
        flattenReadResults("businessCards", "readBusinessCards"),
        flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":["{\"type\":\"string\",\"valueString\"""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeBusinessCards]] =
    Seq(new TestObject(analyzeBusinessCards, imageDf3))

  override def reader: MLReadable[_] = AnalyzeBusinessCards
}

class AnalyzeInvoicesSuite extends TransformerFuzzing[AnalyzeInvoices] with FormRecognizerUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val analyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("invoices").setConcurrency(5)

  lazy val bytesAnalyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("invoices").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "invoices.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf4.mlTransform(analyzeInvoices,
        flattenReadResults("invoices", "readInvoices"),
        flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("Enterprise Way Sunnayvale"))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf2.mlTransform(analyzeInvoices,
        flattenReadResults("invoices", "readInvoices"),
        flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("123 Other St"))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeInvoices,
        flattenReadResults("invoices", "readInvoices"),
        flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("Enterprise Way Sunnayvale"))
  }

  override def testObjects(): Seq[TestObject[AnalyzeInvoices]] =
    Seq(new TestObject(analyzeInvoices, imageDf4))

  override def reader: MLReadable[_] = AnalyzeInvoices
}

class AnalyzeIDDocumentsSuite extends TransformerFuzzing[AnalyzeIDDocuments] with FormRecognizerUtils {
  override val compareDataInSerializationTest: Boolean = false


  lazy val analyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageUrlCol("source").setOutputCol("ids").setConcurrency(5)

  lazy val bytesAnalyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey).setLocation("eastus")
    .setImageBytesCol("imageBytes").setOutputCol("ids").setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "ids.analyzeResult.readResults")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf5.mlTransform(analyzeIDDocuments,
        flattenReadResults("ids", "readIds"),
        flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("DateOfExpiration"))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF5.mlTransform(bytesAnalyzeIDDocuments,
        flattenReadResults("ids", "readIds"),
        flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.contains("DateOfExpiration"))

  }

  override def testObjects(): Seq[TestObject[AnalyzeIDDocuments]] =
    Seq(new TestObject(analyzeIDDocuments, imageDf5))

  override def reader: MLReadable[_] = AnalyzeIDDocuments
}

trait CustomModelUtils extends TestBase with CognitiveKey {

  lazy val trainingDataSAS: String = "https://mmlspark.blob.core.windows.net/datasets"

  lazy val getRequestUrl: String = FormRecognizerUtils.formPost("", TrainCustomModelSchema(
    trainingDataSAS, SourceFilter("CustomModelTrain", includeSubFolders = false), useLabelFile = false))

  override def afterAll(): Unit = {
    super.afterAll()
  }
}

class ListCustomModelsSuite extends TransformerFuzzing[ListCustomModels]
  with FormRecognizerUtils with CustomModelUtils {
  override val compareDataInSerializationTest: Boolean = false

  lazy val listCustomModels: ListCustomModels = {
    new ListCustomModels()
      .setSubscriptionKey(cognitiveKey)
      .setLocation("eastus")
      .setOp("full")
      .setOutputCol("models")
      .setConcurrency(5)
  }

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("models.summary.count")
    }

    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  ignore("List model list details") {
    val results = pathDf.mlTransform(listCustomModels,
        flattenModelList("models", "modelIds"))
      .select("modelIds")
      .collect()
    assert(results.head.getString(0) != "")
  }

  ignore("List model list summary") {
    val results = listCustomModels.setOp("summary").transform(pathDf)
      .withColumn("modelCount", col("models").getField("summary").getField("count"))
      .select("modelCount")
      .collect()
    assert(results.head.getInt(0) >= 1)
  }

  override def testObjects(): Seq[TestObject[ListCustomModels]] =
    Seq(new TestObject(listCustomModels, pathDf))

  override def reader: MLReadable[_] = ListCustomModels
}
