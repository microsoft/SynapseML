// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cognitive

import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.http.entity.{AbstractHttpEntity, ContentType, StringEntity}
import org.apache.spark.ml.ComplexParamsReadable
import org.apache.spark.ml.param.ServiceParam
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.DataType
import com.microsoft.ml.spark.build.BuildInfo
import com.microsoft.ml.spark.io.http.{HTTPRequestData, HTTPResponseData, HandlingUtils, HeaderValues}
import com.microsoft.ml.spark.io.http.HandlingUtils.{convertAndClose, sendWithRetries}
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import spray.json._

import java.net.URI
import java.util.concurrent.TimeoutException
import scala.concurrent.blocking

object DocumentTranslator extends ComplexParamsReadable[DocumentTranslator]

class DocumentTranslator(override val uid: String) extends CognitiveServicesBaseNoHandler(uid)
  with HasInternalJsonOutputParser with HasCognitiveServiceInput with HasServiceName
  with Wrappable with HasAsyncReply with BasicLogging {
  import TranslatorJsonProtocol._

  logClass()

  def this() = this(Identifiable.randomUID("DocumentTranslator"))

  val filterPrefix = new ServiceParam[String](
    this, "filterPrefix", "A case-sensitive prefix string to filter documents in the source" +
      " path for translation. For example, when using an Azure storage blob Uri, use the prefix to" +
      " restrict sub folders for translation.")

  def setFilterPrefix(v: String): this.type = setScalarParam(filterPrefix, v)

  def setFilterPrefixCol(v: String): this.type = setVectorParam(filterPrefix, v)

  val filterSuffix = new ServiceParam[String](
    this, "filterSuffix", "A case-sensitive suffix string to filter documents in the source" +
      " path for translation. This is most often use for file extensions.")

  def setFilterSuffix(v: String): this.type = setScalarParam(filterSuffix, v)

  def setFilterSuffixCol(v: String): this.type = setVectorParam(filterSuffix, v)

  val sourceLanguage = new ServiceParam[String](this, "sourceLanguage", "Language code." +
    " If none is specified, we will perform auto detect on the document.")

  def setSourceLanguage(v: String): this.type = setScalarParam(sourceLanguage, v)

  def setSourceLanguageCol(v: String): this.type = setVectorParam(sourceLanguage, v)

  val sourceUrl = new ServiceParam[String](this, "sourceUrl", "Location of the folder /" +
    " container or single file with your documents.", isRequired = true)

  def setSourceUrl(v: String): this.type = setScalarParam(sourceUrl, v)

  def setSourceUrlCol(v: String): this.type = setVectorParam(sourceUrl, v)

  val sourceStorageSource = new ServiceParam[String](this, "sourceStorageSource",
    "Storage source of source input.")

  def setSourceStorageSource(v: String): this.type = setScalarParam(sourceStorageSource, v)

  def setSourceStorageSourceCol(v: String): this.type = setVectorParam(sourceStorageSource, v)

  val storageType = new ServiceParam[String](this, "storageType", "Storage type of the input" +
    " documents source string. Required for single document translation only.")

  def setStorageType(v: String): this.type = setScalarParam(storageType, v)

  def setStorageTypeCol(v: String): this.type = setVectorParam(storageType, v)

  val targets = new ServiceParam[Seq[TargetInput]](this, "targets", "Destination for the" +
    " finished translated documents.")

  def setTargets(v: Seq[TargetInput]): this.type = setScalarParam(targets, v)

  def setTargetsCol(v: String): this.type = setVectorParam(targets, v)

  override protected def prepareEntity: Row => Option[AbstractHttpEntity] = {
    def fetchGlossaries(row: Row): Option[Seq[Glossary]] = {
      try {
        Option(row.getSeq(1).asInstanceOf[Seq[Row]].map(
          x => Glossary(x.getString(0), x.getString(1), Option(x.getString(2)), Option(x.getString(3)))
        ))
      } catch {
        case e: NullPointerException => Option(row.getAs[Seq[Glossary]]("glossaries"))
      }
    }

    r => Some(new StringEntity(
      Map("inputs" -> Seq(
        BatchRequest(source = SourceInput(
          filter = Option(DocumentFilter(
            prefix = getValueOpt(r, filterPrefix),
            suffix = getValueOpt(r, filterSuffix))),
          language = getValueOpt(r, sourceLanguage),
          storageSource = getValueOpt(r, sourceStorageSource),
          sourceUrl = getValue(r, sourceUrl)),
          storageType = getValueOpt(r, storageType),
          targets = getValue(r, targets).asInstanceOf[Seq[Row]].map(
            row => TargetInput(Option(row.getString(0)),
              fetchGlossaries(row),
              row.getString(2), row.getString(3), Option(row.getString(4))))
      ))).toJson.compactPrint, ContentType.APPLICATION_JSON))
  }

  private def queryForResult(key: Option[String],
                             client: CloseableHttpClient,
                             location: URI): Option[HTTPResponseData] = {
    val get = new HttpGet()
    get.setURI(location)
    key.foreach(get.setHeader("Ocp-Apim-Subscription-Key", _))
    get.setHeader("User-Agent", s"mmlspark/${BuildInfo.version}${HeaderValues.PlatformInfo}")
    val resp = convertAndClose(sendWithRetries(client, get, getBackoffs))
    get.releaseConnection()
    val status = IOUtils.toString(resp.entity.get.content, "UTF-8")
      .parseJson.asJsObject.fields.get("status").map(_.convertTo[String])
    status.map(_.toLowerCase()).flatMap {
      case "succeeded" | "failed" | "canceled" | "ValidationFailed" => Some(resp)
      case "notstarted" | "running" | "cancelling" => None
      case s => throw new RuntimeException(s"Received unknown status code: $s")
    }
  }

  override protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData = {
    val response = HandlingUtils.advanced(getBackoffs: _*)(client, request)
    if (response.statusLine.statusCode == 202) {
      val location = new URI(response.headers.filter(_.name == "Operation-Location").head.value)
      val maxTries = getMaxPollingRetries
      val key = request.headers.find(_.name == "Ocp-Apim-Subscription-Key").map(_.value)
      val it = (0 to maxTries).toIterator.flatMap { _ =>
        queryForResult(key, client, location).orElse({
          blocking {
            Thread.sleep(getPollingDelay.toLong)
          }
          None
        })
      }
      if (it.hasNext) {
        it.next()
      } else {
        throw new TimeoutException(
          s"Querying for results did not complete within $maxTries tries")
      }
    } else {
      response
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      setUrl(s"https://$getServiceName.cognitiveservices.azure.com/translator/text/batch/v1.0/batches")
      getInternalTransformer(dataset.schema).transform(dataset)
    })
  }

  override def responseDataType: DataType = TranslationStatusResponse.schema
}

