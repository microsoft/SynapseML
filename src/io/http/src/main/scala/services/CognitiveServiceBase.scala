// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.http.NameValuePair
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json.DefaultJsonProtocol.StringJsonFormat
import scala.language.existentials

import scala.collection.JavaConverters._

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

  def getVectorParamMap: Map[String, String] = this.params.flatMap {
    case p: ServiceParam[_] =>
      get(p).orElse(getDefault(p)).flatMap(v =>
        v.data.flatMap(_.right.toOption.map(colname => (p.name, colname))))
    case _ => None
  }.toMap

  def getRequiredParams: Array[ServiceParam[_]] = this.params.filter {
    case p:ServiceParam[_] if p.isRequired => true
    case _ => false
  }.map(_.asInstanceOf[ServiceParam[_]])

  def shouldSkip(row: Row): Boolean = getRequiredParams.exists { p =>
    val value = get(p).orElse(getDefault(p)).get
    value match {
      case ServiceParamData(_,Some(_)) => false
      case ServiceParamData(Some(Left(_)),_)=> false
      case ServiceParamData(Some(Right(colname)), _) =>
        Option(row.get(row.fieldIndex(colname))).isEmpty
      case _ => true
    }
  }

  def getValueOpt[T](row: Row, p: ServiceParam[T]): Option[T] = {
    get(p).orElse(getDefault(p)).flatMap {param =>
      param.data.flatMap {
        case Right(colName) => Option(row.getAs[T](colName))
        case Left(value) => Some(value)
      }.orElse(param.default)
    }
  }

  def getValue[T](row: Row, p: ServiceParam[T]): T =
    getValueOpt(row, p).get

  def getValueAnyOpt(row: Row, p: ServiceParam[_]): Option[Any] = {
    get(p).orElse(getDefault(p)).map {param =>
      param.data.flatMap {
        case Right(colName) => Option(row.get(row.fieldIndex(colName)))
        case Left(value) => Some(value)
      }.orElse(param.default)
    }
  }

  def getValueAny(row: Row, p: ServiceParam[_]): Any =
    getValueAnyOpt(row, p).get

  def getValueMap(row: Row, excludes: Set[ServiceParam[_]] = Set()): Map[String, Any] = {
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

trait HasInternalCustomInputParser {

  protected def inputFunc(schema: StructType): Row => Option[HttpRequestBase]

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
    val stages = Array(
      Lambda(_.withColumn(
        dynamicParamColName,
        struct(getVectorParamMap.values.toList.map(col): _*))),
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
