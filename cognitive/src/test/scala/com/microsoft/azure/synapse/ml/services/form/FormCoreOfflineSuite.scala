// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.form

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.io.http.ErrorUtils
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType, StructField, StructType}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

class FormCoreOfflineSuite extends TestBase {

  private class ExposedAnalyzeLayout(uid: String) extends AnalyzeLayout(uid) {
    def requestEntity(row: Row): Option[AbstractHttpEntity] = prepareEntity(row)
    def requestUrl(row: Row): String = prepareUrl(row)
  }

  private class ExposedAnalyzeDocument(uid: String) extends AnalyzeDocument(uid) {
    def requestEntity(row: Row): Option[AbstractHttpEntity] = prepareEntity(row)
    def requestUrl(row: Row): String = prepareUrl(row)
  }

  private class ExposedGetCustomModel(uid: String) extends GetCustomModel(uid) {
    def requestUrl(row: Row): String = prepareUrl(row)
  }

  private class ExposedAnalyzeCustomModel(uid: String) extends AnalyzeCustomModel(uid) {
    def requestUrl(row: Row): String = prepareUrl(row)
  }

  private def entityToBody(entity: AbstractHttpEntity): String = EntityUtils.toString(entity)

  private def queryParams(url: String): Map[String, String] = {
    URLEncodedUtils
      .parse(new URI(url), StandardCharsets.UTF_8)
      .asScala
      .map(p => p.getName -> p.getValue)
      .toMap
  }

  test("Form Recognizer params validate allowed values offline") {
    val receipts = new AnalyzeReceipts("offline-receipts").setLocale("en-US")
    assert(receipts.getLocale == "en-US")

    val document = new AnalyzeDocument("offline-document")
      .setStringIndexType("utf16CodeUnit")
      .setFeatures(Seq("barcodes", "languages"))
    assert(document.getStringIndexType == "utf16CodeUnit")
    assert(document.getFeatures == Seq("barcodes", "languages"))

    intercept[IllegalArgumentException] {
      receipts.setLocale("fr-FR")
    }
    intercept[IllegalArgumentException] {
      document.setStringIndexType("bad-value")
    }
    intercept[IllegalArgumentException] {
      document.setFeatures(Seq("barcodes", "unsupported"))
    }
  }

  test("Form recognizer request entities build deterministic url and byte payloads") {
    val urlInput = "https://contoso.example/layout.jpg"
    val bytesInput = Array[Byte](1, 2, 3)

    val layoutFromUrl = new ExposedAnalyzeLayout("layout-url").setImageUrl(urlInput)
    val urlPayload = entityToBody(layoutFromUrl.requestEntity(Row.empty).get).parseJson.asJsObject
    assert(urlPayload.fields("source").convertTo[String] == urlInput)

    val layoutFromBytes = new ExposedAnalyzeLayout("layout-bytes").setImageBytes(bytesInput)
    val bodyBytes = EntityUtils.toByteArray(layoutFromBytes.requestEntity(Row.empty).get)
    assert(bodyBytes.sameElements(bytesInput))
  }

  test("AnalyzeDocument builds v3 request url and payload from local params") {
    val imageUrl = "https://contoso.example/form.pdf"
    val analyzeDocument = new ExposedAnalyzeDocument("analyze-document-url")
      .setLocation("eastus")
      .setPrebuiltModelId("prebuilt-layout")
      .setImageUrl(imageUrl)
      .setPages("1-2")
      .setStringIndexType("utf16CodeUnit")
      .setFeatures(Seq("barcodes", "languages"))

    val url = analyzeDocument.requestUrl(Row.empty)
    val uri = new URI(url)
    val query = queryParams(url)
    assert(uri.getPath.endsWith("/formrecognizer/documentModels/prebuilt-layout:analyze"))
    assert(query("api-version") == "2023-07-31")
    assert(query("pages") == "1-2")
    assert(query("stringIndexType") == "utf16CodeUnit")
    assert(query("features") == "List(barcodes, languages)")

    val requestBody = entityToBody(analyzeDocument.requestEntity(Row.empty).get).parseJson.asJsObject
    assert(requestBody.fields("urlSource").convertTo[String] == imageUrl)
  }

  test("custom model endpoints append model id deterministically") {
    val getModel = new ExposedGetCustomModel("get-custom-model-url")
      .setLocation("eastus")
      .setModelId("model-123")
      .setIncludeKeys(true)
    val getModelUrl = getModel.requestUrl(Row.empty)
    assert(new URI(getModelUrl).getPath.endsWith("/formrecognizer/v2.1/custom/models/model-123"))
    assert(queryParams(getModelUrl)("includeKeys") == "true")

    val analyzeCustom = new ExposedAnalyzeCustomModel("analyze-custom-model-url")
      .setLocation("eastus")
      .setModelId("model-123")
    assert(new URI(analyzeCustom.requestUrl(Row.empty))
      .getPath.endsWith("/formrecognizer/v2.1/custom/models/model-123/analyze"))
  }

  test("form recognizer schemas expose deterministic output and error columns") {
    spark
    val inputSchema = StructType(Seq(StructField("id", StringType, nullable = true)))

    val analyzeDocumentSchema = new AnalyzeDocument("analyze-document-schema")
      .setLocation("eastus")
      .setPrebuiltModelId("prebuilt-read")
      .setImageUrl("https://contoso.example/doc.png")
      .setOutputCol("documentResult")
      .setErrorCol("documentError")
      .transformSchema(inputSchema)
    assert(analyzeDocumentSchema("documentResult").dataType == AnalyzeDocumentResponse.schema)
    assert(analyzeDocumentSchema("documentError").dataType == ErrorUtils.ErrorSchema)

    val analyzeLayoutSchema = new AnalyzeLayout("analyze-layout-schema")
      .setLocation("eastus")
      .setImageUrl("https://contoso.example/doc.png")
      .setOutputCol("layoutResult")
      .setErrorCol("layoutError")
      .transformSchema(inputSchema)
    assert(analyzeLayoutSchema("layoutResult").dataType == AnalyzeResponse.schema)
    assert(analyzeLayoutSchema("layoutError").dataType == ErrorUtils.ErrorSchema)
  }

  test("field result helpers convert recursive values and simplify data types") {
    import FormsJsonProtocol._

    val numberValue = FieldResultRecursive(
      `type` = "number",
      page = None,
      confidence = None,
      boundingBox = None,
      text = Some("7"),
      valueString = None,
      valuePhoneNumber = None,
      valueNumber = Some(7.0),
      valueDate = None,
      valueTime = None,
      valueObject = None,
      valueArray = None
    )
    val textValue = FieldResultRecursive(
      `type` = "string",
      page = None,
      confidence = None,
      boundingBox = None,
      text = None,
      valueString = Some("widget"),
      valuePhoneNumber = None,
      valueNumber = None,
      valueDate = None,
      valueTime = None,
      valueObject = None,
      valueArray = None
    )

    val mixedArray = FieldResultRecursive(
      `type` = "array",
      page = None,
      confidence = None,
      boundingBox = None,
      text = None,
      valueString = None,
      valuePhoneNumber = None,
      valueNumber = None,
      valueDate = None,
      valueTime = None,
      valueObject = None,
      valueArray = Some(Seq(textValue, numberValue))
    )
    assert(mixedArray.toSimplifiedDataType == ArrayType(StringType))

    val objectValue = FieldResult(
      `type` = "object",
      page = None,
      confidence = None,
      boundingBox = None,
      text = None,
      valueString = None,
      valuePhoneNumber = None,
      valueNumber = None,
      valueDate = None,
      valueTime = None,
      valueObject = Some(Map("count" -> numberValue, "label" -> textValue).toJson.compactPrint),
      valueArray = None
    ).toFieldResultRecursive

    val objectType = objectValue.toSimplifiedDataType.asInstanceOf[StructType]
    assert(objectType.fieldNames.toSet == Set("count", "label"))
    val objectRow = objectValue.viewAsDataType(objectType).asInstanceOf[Row]
    val objectValues = objectRow.toSeq.toSet
    assert(objectValues.contains(Some(7.0)))
    assert(objectValues.contains(Some("widget")))
    assert(numberValue.viewAsDataType(StringType) == "7")
    assert(numberValue.viewAsDataType(DoubleType) == 7.0)
  }

  test("FormOntologyLearner.combineDataTypes merges nested schemas deterministically") {
    val left = StructType(Seq(
      StructField("shared", StringType, nullable = true),
      StructField("leftOnly", DoubleType, nullable = true)
    ))
    val right = StructType(Seq(
      StructField("shared", DoubleType, nullable = true),
      StructField("rightOnly", StringType, nullable = true)
    ))

    val merged = FormOntologyLearner.combineDataTypes(left, right).asInstanceOf[StructType]
    assert(merged.fieldNames.toSet == Set("shared", "leftOnly", "rightOnly"))
    assert(merged("shared").dataType == StringType)
    assert(FormOntologyLearner.combineDataTypes(ArrayType(StringType), ArrayType(DoubleType)) == ArrayType(StringType))
  }
}
