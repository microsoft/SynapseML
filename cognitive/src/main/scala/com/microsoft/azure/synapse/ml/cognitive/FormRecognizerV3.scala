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
    "Prebuilt Model identifier for Form Recognizer V3.0, supported modelId: prebuilt-read, prebuilt-layout," +
      "prebuilt-document, prebuilt-businessCard, prebuilt-idDocument, prebuilt-invoice, prebuilt-receipt," +
      "or your custom modelId", {
      case Left(s) => Set("prebuilt-read", "prebuilt-layout", "prebuilt-document", "prebuilt-businessCard",
        "prebuilt-idDocument", "prebuilt-invoice", "prebuilt-receipt")(s)
      case Right(_) => true
    }, isRequired = true)

  def setPrebuiltModelId(v: String): this.type = setScalarParam(prebuiltModelId, v)

  def setPrebuiltModelIdCol(v: String): this.type = setVectorParam(prebuiltModelId, v)

  def getPrebuiltModelId: String = getScalarParam(prebuiltModelId)

  def getPrebuiltModelIdCol: String = getVectorParam(prebuiltModelId)
}

object AnalyzeDocument extends ComplexParamsReadable[AnalyzeDocument]

class AnalyzeDocument(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasCognitiveServiceInput with HasInternalJsonOutputParser with BasicAsyncReply
  with HasPrebuiltModelID with HasPages with HasLocale with HasAPIVersion
  with HasImageInput with HasSetLocation with BasicLogging {
  logClass()

  setDefault(apiVersion -> Left("2022-01-30-preview"))

  def this() = this(Identifiable.randomUID("AnalyzeDocument"))

  def urlPath: String = "formrecognizer/documentModels"

  val stringIndexType = new ServiceParam[String](this, "stringIndexType", "Method used to " +
    "compute string offset and length.", {
    case Left(s) => Set("textElements", "unicodeCodePoint", "utf16CodeUnit")(s)
    case Right(_) => true
  }, isURLParam = true)

  def setStringIndexType(v: String): this.type = setScalarParam(stringIndexType, v)

  def setStringIndexTypeCol(v: String): this.type = setVectorParam(stringIndexType, v)

  def getStringIndexType: String = getScalarParam(stringIndexType)

  def getStringIndexTypeCol: String = getVectorParam(stringIndexType)

  override protected def responseDataType: DataType = AnalyzeDocumentResponse.schema

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    r =>
      getValueOpt(r, imageUrl)
        .map(url => new StringEntity(Map("urlSource" -> url).toJson.compactPrint, ContentType.APPLICATION_JSON))
        .orElse(getValueOpt(r, imageBytes)
          .map(bytes => new ByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM))
        ).orElse(throw new IllegalArgumentException(
        "Payload needs to contain image bytes or url. This code should not run"))
  }

  override protected def prepareUrlRoot: Row => String = { row =>
    getUrl + s"/${getValue(row, prebuiltModelId)}:analyze"
  }

}
