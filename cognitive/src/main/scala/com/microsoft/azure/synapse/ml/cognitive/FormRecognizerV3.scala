// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.cognitive

import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.http.entity.{AbstractHttpEntity, ByteArrayEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import spray.json.DefaultJsonProtocol._
import spray.json._

trait HasPrebuiltModelID extends HasServiceParams {
  // TODO: Add back custom modelId support after cog service team's fix
  // Currently custom modelId generated from the old version can't be used by V3
  val prebuiltModelId = new ServiceParam[String](this, "prebuiltModelId",
    "Prebuilt Model identifier for Form Recognizer V3.0, supported modelId: prebuilt-layout," +
      "prebuilt-document, prebuilt-businessCard, prebuilt-idDocument, prebuilt-invoice, prebuilt-receipt," +
      "or your custom modelId", {
      case Left(s) => Set("prebuilt-layout", "prebuilt-document", "prebuilt-businessCard",
        "prebuilt-idDocument", "prebuilt-invoice", "prebuilt-receipt")(s)
      case Right(_) => true
    }, isRequired = true)

  def setPrebuiltModelId(v: String): this.type = setScalarParam(prebuiltModelId, v)

  def setPrebuiltModelIdCol(v: String): this.type = setVectorParam(prebuiltModelId, v)

  def getPrebuiltModelId: String = getScalarParam(prebuiltModelId)

  def getPrebuiltModelIdCol: String = getVectorParam(prebuiltModelId)
}

object AnalyzeDocumentV3 extends ComplexParamsReadable[AnalyzeDocumentV3]

class AnalyzeDocumentV3(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with BasicAsyncReply
  with HasPrebuiltModelID with HasPages with HasLocale
  with HasImageInput with HasSetLocation with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("AnalyzeDocumentV3"))

  def urlPath: String = "formrecognizer/documentModels/"

  val stringIndexType = new ServiceParam[String](this, "stringIndexType", "Method used to " +
    "compute string offset and length.", {
    case Left(s) => Set("textElements", "unicodeCodePoint", "utf16CodeUnit")(s)
    case Right(_) => true
  }, isURLParam = true)

  def setStringIndexType(v: String): this.type = setScalarParam(stringIndexType, v)

  def setStringIndexTypeCol(v: String): this.type = setVectorParam(stringIndexType, v)

  def getStringIndexType: String = getScalarParam(stringIndexType)

  def getStringIndexTypeCol: String = getVectorParam(stringIndexType)

  override protected def responseDataType: DataType = AnalyzeDocumentV3Response.schema

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, imageUrl)
        .map(url => new StringEntity(Map("urlSource" -> url).toJson.compactPrint, ContentType.APPLICATION_JSON))
        .orElse(getValueOpt(r, imageBytes)
          .map(bytes => new ByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM))
        ).orElse(throw new IllegalArgumentException(
        "Payload needs to contain image bytes or url. This code should not run"))
  }

  override protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val base = getUrl + s"${getValue(row, prebuiltModelId)}:analyze?api-version=2021-09-30-preview"
      val appended = if (!urlParams.isEmpty) {
        "&" + URLEncodingUtils.format(urlParams.flatMap(p =>
          getValueOpt(row, p).map(v => p.name -> p.toValueString(v))
        ).toMap)
      } else {
        ""
      }
      base + appended
    }
  }

}
