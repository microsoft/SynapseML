// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive.split1

import com.microsoft.ml.spark.FluentAPI._
import com.microsoft.ml.spark.cognitive._
import com.microsoft.ml.spark.core.env.StreamUtilities.using
import com.microsoft.ml.spark.core.test.base.{Flaky, TestBase}
import com.microsoft.ml.spark.core.test.fuzzing.{EstimatorFuzzing, ModelFuzzing, TestObject, TransformerFuzzing}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalactic.Equality
import spray.json.DefaultJsonProtocol.{BooleanJsonFormat, StringJsonFormat, jsonFormat2, jsonFormat3}
import spray.json._

import java.net.URI

object FormRecognizerUtils extends CognitiveKey {

  import RESTHelpers._

  val BaseURL = "https://eastus.api.cognitive.microsoft.com/formrecognizer/v2.1/custom/models"

  def formSend(request: HttpRequestBase, path: String,
               params: Map[String, String] = Map()): String = {

    val paramString = if (params.isEmpty) {
      ""
    } else {
      "?" + URLEncodingUtils.format(params)
    }
    request.setURI(new URI(BaseURL + path + paramString))

    retry(List(100, 500, 1000), { () =>
      request.addHeader("Ocp-Apim-Subscription-Key", cognitiveKey)
      request.addHeader("Content-Type", "application/json")
      using(Client.execute(request)) { response =>
        if (!response.getStatusLine.getStatusCode.toString.startsWith("2")) {
          val bodyOpt = request match {
            case er: HttpEntityEnclosingRequestBase => IOUtils.toString(er.getEntity.getContent, "UTF-8")
            case _ => ""
          }
          throw new RuntimeException(
            s"Failed: response: $response " +
              s"requestUrl: ${request.getURI}" +
              s"requestBody: $bodyOpt")
        }
        if (response.getStatusLine.getReasonPhrase == "No Content") {
          ""
        }
        else {
          IOUtils.toString(response.getEntity.getContent, "UTF-8")
        }
      }.get
    })
  }

  def formDelete(path: String, params: Map[String, String] = Map()): String = {
    formSend(new HttpDelete(), "/" + path, params)
  }
}

trait FormRecognizerUtils extends TestBase {

  import spark.implicits._

  def createTestDataframe(v: Seq[String], returnBytes: Boolean): DataFrame = {
    val df = v.toDF("source")
    if (returnBytes) {
      BingImageSearch
        .downloadFromUrls("source", "imageBytes", 4, 10000)
        .transform(df)
        .select("imageBytes")
    } else {
      df
    }
  }

  lazy val imageDf1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"), returnBytes = false)

  lazy val bytesDF1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout1.jpg"), returnBytes = true)

  lazy val imageDf2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png"), returnBytes = false)

  lazy val bytesDF2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/receipt1.png"), returnBytes = true)

  lazy val imageDf3: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg"), returnBytes = false)

  lazy val bytesDF3: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg"), returnBytes = true)

  lazy val imageDf4: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"), returnBytes = false)

  lazy val bytesDF4: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice2.png"), returnBytes = true)

  lazy val imageDf5: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg"), returnBytes = false)

  lazy val bytesDF5: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/id1.jpg"), returnBytes = true)

  lazy val pdfDf1: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/layout2.pdf"), returnBytes = false)

  lazy val pdfDf2: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice1.pdf",
    "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/invoice3.pdf"), returnBytes = false)

  lazy val trainingDataSAS: DataFrame = createTestDataframe(
    Seq("https://mmlspark.blob.core.windows.net/datasets?sp=rl&st=2021-06-25T02:36:37Z&se=2021" +
      "-07-03T02:36:00Z&sv=2020-08-04&sr=c&sig=ROfFMue92rhwZvPA1xe4amTFo0zPJJt%2BIyj42JYHyi0%3D"
  ), returnBytes = false)

  lazy val df: DataFrame = createTestDataframe(Seq(""), returnBytes = false)
}

class AnalyzeLayoutSuite extends TransformerFuzzing[AnalyzeLayout]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val analyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("source")
    .setOutputCol("layout")
    .setConcurrency(5)

  lazy val bytesAnalyzeLayout: AnalyzeLayout = new AnalyzeLayout()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("layout")
    .setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "layout.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf1.mlTransform(analyzeLayout,
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512 " +
      "Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Details | Quantity | Unit Price | Total | Bindings | 20 | 1.00 | 20.00 | Covers Small" +
      " | 20 | 1.00 | 20.00 | Feather Bookmark | 20 | 5.00 | 100.00 | Copper Swirl Marker | 20 | 5.00 | " +
      "100.00\nSUBTOTAL | $140.00 | TAX | $4.00 |  |  | TOTAL | $144.00")
  }

  test("Basic Usage with pdf") {
    val results = pdfDf1.mlTransform(analyzeLayout,
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    val correctPrefix = "UNITED STATES SECURITIES AND EXCHANGE COMMISSION Washington, D.C. 20549 FORM 10-Q"
    assert(headStr.startsWith(correctPrefix))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Title of each class | Trading Symbol | Name of exchange on which registered | " +
      "Common stock, $0.00000625 par value per share | MSFT | NASDAQ | 2.125% Notes due 2021 | MSFT | NASDAQ |" +
      " 3.125% Notes due 2028 | MSFT | NASDAQ | 2.625% Notes due 2033 | MSFT | NASDAQ")
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF1.mlTransform(bytesAnalyzeLayout,
      AnalyzeLayout.flattenReadResults("layout", "readlayout"),
      AnalyzeLayout.flattenPageResults("layout", "pageLayout"))
      .select("readlayout", "pageLayout")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr.startsWith("Purchase Order Hero Limited Purchase Order Company Phone: 555-348-6512" +
      " Website: www.herolimited.com "))
    val pageHeadStr = results.head.getString(1)
    assert(pageHeadStr === "Details | Quantity | Unit Price | Total | Bindings | 20 | 1.00 | 20.00 | Covers Small" +
      " | 20 | 1.00 | 20.00 | Feather Bookmark | 20 | 5.00 | 100.00 | Copper Swirl Marker | 20 | 5.00 | " +
      "100.00\nSUBTOTAL | $140.00 | TAX | $4.00 |  |  | TOTAL | $144.00")
  }

  override def testObjects(): Seq[TestObject[AnalyzeLayout]] =
    Seq(new TestObject(analyzeLayout, imageDf1))

  override def reader: MLReadable[_] = AnalyzeLayout
}

class AnalyzeReceiptsSuite extends TransformerFuzzing[AnalyzeReceipts]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val analyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("source")
    .setOutputCol("receipts")
    .setConcurrency(5)

  lazy val bytesAnalyzeReceipts: AnalyzeReceipts = new AnalyzeReceipts()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("receipts")
    .setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "receipts.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf2.mlTransform(analyzeReceipts,
      AnalyzeReceipts.flattenReadResults("receipts", "readReceipts"),
      AnalyzeReceipts.flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Items":{"type":"array","valueArray":[{"type":"object",""" +
        """"valueObject":{"Name":{"type":"string","valueString":"Surface Pro 6","text":"Surface Pro 6""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF2.mlTransform(bytesAnalyzeReceipts,
      AnalyzeReceipts.flattenReadResults("receipts", "readReceipts"),
      AnalyzeReceipts.flattenDocumentResults("receipts", "docReceipts"))
      .select("readReceipts", "docReceipts")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith(
      ("""{"Items":{"type":"array","valueArray":[{"type":"object",""" +
        """"valueObject":{"Name":{"type":"string","valueString":"Surface Pro 6","text":"Surface Pro 6""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeReceipts]] =
    Seq(new TestObject(analyzeReceipts, imageDf2))

  override def reader: MLReadable[_] = AnalyzeReceipts
}

class AnalyzeBusinessCardsSuite extends TransformerFuzzing[AnalyzeBusinessCards]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val analyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("source")
    .setOutputCol("businessCards")
    .setConcurrency(5)

  lazy val bytesAnalyzeBusinessCards: AnalyzeBusinessCards = new AnalyzeBusinessCards()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("businessCards")
    .setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "businessCards.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf3.mlTransform(analyzeBusinessCards,
      AnalyzeBusinessCards.flattenReadResults("businessCards", "readBusinessCards"),
      AnalyzeBusinessCards.flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":[{"type":""" +
      """"string","valueString":"2 Kingdom Street Paddington, London, W2 6BD""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF3.mlTransform(bytesAnalyzeBusinessCards,
      AnalyzeBusinessCards.flattenReadResults("businessCards", "readBusinessCards"),
      AnalyzeBusinessCards.flattenDocumentResults("businessCards", "docBusinessCards"))
      .select("readBusinessCards", "docBusinessCards")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Addresses":{"type":"array","valueArray":[{"type":""" +
        """"string","valueString":"2 Kingdom Street Paddington, London, W2 6BD""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeBusinessCards]] =
    Seq(new TestObject(analyzeBusinessCards, imageDf3))

  override def reader: MLReadable[_] = AnalyzeBusinessCards
}

class AnalyzeInvoicesSuite extends TransformerFuzzing[AnalyzeInvoices]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val analyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("source")
    .setOutputCol("invoices")
    .setConcurrency(5)

  lazy val bytesAnalyzeInvoices: AnalyzeInvoices = new AnalyzeInvoices()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("invoices")
    .setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "invoices.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf4.mlTransform(analyzeInvoices,
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"type":"string","valueString":"1020 Enterprise Way Sunnayvale, CA 87659","text":""" +
        """"1020 Enterprise Way Sunnayvale, CA 87659""").stripMargin))
  }

  test("Basic Usage with pdf") {
    val results = pdfDf2.mlTransform(analyzeInvoices,
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith("""{"AmountDue":{"type":"number","valueNumber":610,"text":"$610.00"""))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF4.mlTransform(bytesAnalyzeInvoices,
      AnalyzeInvoices.flattenReadResults("invoices", "readInvoices"),
      AnalyzeInvoices.flattenDocumentResults("invoices", "docInvoices"))
      .select("readInvoices", "docInvoices")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"CustomerAddress":{"type":"string","valueString":"1020 Enterprise Way Sunnayvale, CA 87659","text":""" +
        """"1020 Enterprise Way Sunnayvale, CA 87659""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeInvoices]] =
    Seq(new TestObject(analyzeInvoices, imageDf4))

  override def reader: MLReadable[_] = AnalyzeInvoices
}

class AnalyzeIDDocumentsSuite extends TransformerFuzzing[AnalyzeIDDocuments]
  with CognitiveKey with Flaky with FormRecognizerUtils {

  lazy val analyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageUrlCol("source")
    .setOutputCol("ids")
    .setConcurrency(5)

  lazy val bytesAnalyzeIDDocuments: AnalyzeIDDocuments = new AnalyzeIDDocuments()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setImageBytesCol("imageBytes")
    .setOutputCol("ids")
    .setConcurrency(5)

  override def assertDFEq(df1: DataFrame, df2: DataFrame)(implicit eq: Equality[DataFrame]): Unit = {
    def prep(df: DataFrame) = {
      df.select("source", "ids.analyzeResult.readResults")
    }
    super.assertDFEq(prep(df1), prep(df2))(eq)
  }

  test("Basic Usage with URL") {
    val results = imageDf5.mlTransform(analyzeIDDocuments,
      AnalyzeIDDocuments.flattenReadResults("ids", "readIds"),
      AnalyzeIDDocuments.flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Address":{"type":"string","valueString":"123 STREET ADDRESS YOUR CITY WA 99999-1234","text":""" +
        """"123 STREET ADDRESS YOUR CITY WA 99999-1234""").stripMargin))
  }

  test("Basic Usage with Bytes") {
    val results = bytesDF5.mlTransform(bytesAnalyzeIDDocuments,
      AnalyzeIDDocuments.flattenReadResults("ids", "readIds"),
      AnalyzeIDDocuments.flattenDocumentResults("ids", "docIds"))
      .select("readIds", "docIds")
      .collect()
    val headStr = results.head.getString(0)
    assert(headStr === "")
    val docHeadStr = results.head.getString(1)
    assert(docHeadStr.startsWith((
      """{"Address":{"type":"string","valueString":"123 STREET ADDRESS YOUR CITY WA 99999-1234","text":""" +
        """"123 STREET ADDRESS YOUR CITY WA 99999-1234""").stripMargin))
  }

  override def testObjects(): Seq[TestObject[AnalyzeIDDocuments]] =
    Seq(new TestObject(analyzeIDDocuments, imageDf5))

  override def reader: MLReadable[_] = AnalyzeIDDocuments
}
