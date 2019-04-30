// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.net.URI

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.http.NameValuePair
import org.apache.http.client.methods.{HttpEntityEnclosingRequestBase, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.entity.AbstractHttpEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.language.existentials
import spray.json.DefaultJsonProtocol._

trait HasServiceParams extends Params {
  def getVectorParam(p: ServiceParam[_]): String = {
    this.getOrDefault(p).data.get.right.get
  }

  def getScalarParam[T](p: ServiceParam[T]): T = {
    this.getOrDefault(p).data.get.left.get
  }

  def setVectorParam[T](p: ServiceParam[T], value: String): this.type = {
    set(p, ServiceParamData[T](
      Some(Right(value)),
      this.get(p).flatMap(_.default))
    )
  }

  def setDefaultValue[T](p: ServiceParam[T], value: T): this.type = {
    set(p, ServiceParamData[T](
      this.get(p).flatMap(_.data),
      Some(value)
    ))
  }

  def setDefaultValue[T](p: ServiceParam[T], value: Option[T]): this.type = {
    set(p, ServiceParamData[T](
      this.get(p).flatMap(_.data),
      value
    ))
  }

  def setScalarParam[T](p: ServiceParam[T], value: T): this.type = {
    set(p, ServiceParamData(
      Some(Left(value)),
      this.get(p).flatMap(_.default))
    )
  }

  def getVectorParam(name: String): String = {
    getVectorParam(this.getParam(name).asInstanceOf[ServiceParam[_]])
  }

  def getScalarParam[T](name: String): T = {
    getScalarParam(this.getParam(name).asInstanceOf[ServiceParam[T]])
  }

  def setVectorParam(name: String, value: String): this.type = {
    setVectorParam(this.getParam(name).asInstanceOf[ServiceParam[_]], value)
  }

  def setScalarParam[T](name: String, value: T): this.type = {
    setScalarParam(this.getParam(name).asInstanceOf[ServiceParam[T]], value)
  }

  protected def getVectorParamMap: Map[String, String] = this.params.flatMap {
    case p: ServiceParam[_] =>
      get(p).orElse(getDefault(p)).flatMap(v =>
        v.data.flatMap(_.right.toOption.map(colname => (p.name, colname))))
    case _ => None
  }.toMap

  protected def getRequiredParams: Array[ServiceParam[_]] = this.params.filter {
    case p: ServiceParam[_] if p.isRequired => true
    case _ => false
  }.map(_.asInstanceOf[ServiceParam[_]])

  protected def getUrlParams: Array[ServiceParam[_]] = this.params.filter {
    case p: ServiceParam[_] if p.isURLParam => true
    case _ => false
  }.map(_.asInstanceOf[ServiceParam[_]])

  protected def emptyParamData[T](row: Row, p: ServiceParam[T]): Boolean = {
    if (get(p).isEmpty && getDefault(p).isEmpty) {
      return true
    }

    val value = get(p).orElse(getDefault(p)).get

    value match {
      case ServiceParamData(_, Some(_)) => false
      case ServiceParamData(Some(Left(_)), _) => false
      case ServiceParamData(Some(Right(colname)), _) =>
        Option(row.get(row.fieldIndex(colname))).isEmpty
      case _ => true
    }
  }

  protected def shouldSkip(row: Row): Boolean = getRequiredParams.exists { p =>
    emptyParamData(row, p)
  }

  protected def getValueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = {
    get(p).orElse(getDefault(p)).flatMap { param =>
      param.data.flatMap {
        case Right(colName) => Option(row.getAs[T](colName))
        case Left(value) => Some(value)
      }.orElse {
        param.default
      }
    }
  }

  protected def getValue[T](row: Row, p: ServiceParam[T]): T =
    getValueOpt(row, p).get

  protected def getValueAnyOpt(row: Row, p: ServiceParam[_]): Option[Any] = {
    get(p).orElse(getDefault(p)).flatMap { param =>
      param.data.flatMap {
        case Right(colName) => Option(row.get(row.fieldIndex(colName)))
        case Left(value) => Some(value)
      }.orElse(param.default)
    }
  }

  protected def getValueAny(row: Row, p: ServiceParam[_]): Any =
    getValueAnyOpt(row, p).get

  protected def getValueMap(row: Row, excludes: Set[ServiceParam[_]] = Set()): Map[String, Any] = {
    this.params.flatMap {
      case p: ServiceParam[_] if !excludes(p) =>
        getValueOpt(row, p).map(v => (p.name, v))
      case _ => None
    }.toMap
  }
}

trait HasSubscriptionKey extends HasServiceParams {
  val subscriptionKey = new ServiceParam[String](
    this, "subscriptionKey", "the API key to use")

  def getSubscriptionKey: String = getScalarParam(subscriptionKey)

  def setSubscriptionKey(v: String): this.type = setScalarParam(subscriptionKey, v)

  def getSubscriptionKeyCol: String = getVectorParam(subscriptionKey)

  def setSubscriptionKeyCol(v: String): this.type = setVectorParam(subscriptionKey, v)

}

object URLEncodingUtils {

  private case class NameValuePairInternal(t: (String, String)) extends NameValuePair {
    override def getName: String = t._1

    override def getValue: String = t._2
  }

  def format(m: Map[String, String]): String = {
    URLEncodedUtils.format(m.toList.map(NameValuePairInternal).asJava, "UTF-8")
  }
}

object CognitiveServiceUtils {

  def setUA(req: HttpRequestBase): Unit = {
    req.setHeader("User-Agent", "mmlspark/0.17")
  }
}

trait HasCognitiveServiceInput extends HasURL with HasSubscriptionKey {

  protected def prepareUrl: Row => String = {
    val urlParams: Array[ServiceParam[Any]] =
      getUrlParams.asInstanceOf[Array[ServiceParam[Any]]];
    // This semicolon is needed to avoid argument confusion
    { row: Row =>
      val base = getUrl
      val appended = if (!urlParams.isEmpty) {
        "?" + URLEncodingUtils.format(urlParams.flatMap(p =>
          getValueOpt(row, p).map(v => p.name -> p.toValueString(v))
        ).toMap)
      } else {
        ""
      }
      base + appended
    }
  }

  protected def prepareEntity: Row => Option[AbstractHttpEntity]

  protected def prepareMethod(): HttpRequestBase = new HttpPost()

  protected val subscriptionKeyHeaderName = "Ocp-Apim-Subscription-Key"

  protected def contentType: Row => String = { _ => "application/json" }

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase] = {
    val rowToUrl = prepareUrl
    val rowToEntity = prepareEntity;
    { row: Row =>
      if (shouldSkip(row)) {
        None
      } else {
        val req = prepareMethod()
        req.setURI(new URI(rowToUrl(row)))
        getValueOpt(row, subscriptionKey).foreach(
          req.setHeader(subscriptionKeyHeaderName, _))
        req.setHeader("Content-Type", contentType(row))
        CognitiveServiceUtils.setUA(req)

        req match {
          case er: HttpEntityEnclosingRequestBase =>
            rowToEntity(row).foreach(er.setEntity)
          case _ =>
        }
        Some(req)
      }
    }
  }

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setNullableUDF(inputFunc(schema))
  }

}

trait HasInternalJsonOutputParser {

  protected def responseDataType: DataType

  protected def getInternalOutputParser(schema: StructType): HTTPOutputParser = {
    new JSONOutputParser().setDataType(responseDataType)
  }

}

abstract class CognitiveServicesBaseWithoutHandler(val uid: String) extends Transformer
  with HTTPParams with HasOutputCol
  with HasURL with ComplexParamsWritable
  with HasSubscriptionKey with HasErrorCol {

  setDefault(
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"))

  protected def handlingFunc(client: CloseableHttpClient,
                             request: HTTPRequestData): HTTPResponseData

  protected def getInternalInputParser(schema: StructType): HTTPInputParser

  protected def getInternalOutputParser(schema: StructType): HTTPOutputParser

  protected def getInternalTransformer(schema: StructType): PipelineModel = {
    val dynamicParamColName = DatasetExtensions.findUnusedColumnName("dynamic", schema)
    val dynamicParamCols = getVectorParamMap.values.toList.map(col) match {
      case Nil => Seq(lit(false).alias("placeholder"))
      case l => l
    }

    val stages = Array(
      Lambda(_.withColumn(dynamicParamColName, struct(dynamicParamCols: _*))),
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamColName)
        .setOutputCol(getOutputCol)
        .setInputParser(getInternalInputParser(schema))
        .setOutputParser(getInternalOutputParser(schema))
        .setHandler(handlingFunc)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(getConcurrentTimeout)
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamColName)
    )

    NamespaceInjections.pipelineModel(stages)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    getInternalTransformer(dataset.schema).transform(dataset)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    getInternalTransformer(schema).transformSchema(schema)
  }
}

abstract class CognitiveServicesBase(uid: String) extends
  CognitiveServicesBaseWithoutHandler(uid) with HasHandler {
  setDefault(handler -> HandlingUtils.advancedUDF(100))

  override def handlingFunc(client: CloseableHttpClient,
                            request: HTTPRequestData): HTTPResponseData =
    getHandler(client, request)
}
