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

import scala.collection.JavaConverters._

trait HasVectorizableParams extends Params {
  def getVectorParam(p: VectorizableParam[_]): String = {
    this.getOrDefault(p).right.get
  }

  def getScalarParam[T](p: VectorizableParam[T]): T = {
    this.getOrDefault(p).left.get
  }

  def setVectorParam(p: VectorizableParam[_], value: String): this.type = {
    set(p, Right(value))
  }

  def setScalarParam[T](p: VectorizableParam[T], value: T): this.type = {
    set(p, Left(value))
  }

  def getVectorParam(name: String): String = {
    getVectorParam(this.getParam(name).asInstanceOf[VectorizableParam[_]])
  }

  def getScalarParam[T](name: String): T = {
    getScalarParam(this.getParam(name).asInstanceOf[VectorizableParam[T]])
  }

  def setVectorParam(name: String, value: String): this.type = {
    setVectorParam(this.getParam(name).asInstanceOf[VectorizableParam[_]], value)
  }

  def setScalarParam[T](name: String, value: T): this.type = {
    setScalarParam(this.getParam(name).asInstanceOf[VectorizableParam[T]], value)
  }

  def getVectorParamMap: Map[String, String] = this.params.flatMap {
    case p: VectorizableParam[_] =>
      get(p).orElse(getDefault(p)).flatMap(v =>
        v.right.toOption.map(colname => (p.name, colname)))
    case _ => None
  }.toMap

  def getValueOpt[T](row: Row, p: VectorizableParam[T], defaultValue: Option[T] = None): Option[T] = {
    get(p).map {
      case Right(colName) => row.getAs[T](colName)
      case Left(value) => value
    } match {
      case None =>
        getDefault(p).map {
          case Right(colName) => row.getAs[T](colName)
          case Left(value) => value
        }.orElse(defaultValue)
      case s => s
    }
  }

  def getValue[T](row: Row, p: VectorizableParam[T]): T =
    getValueOpt(row, p).get

  def getValueAnyOpt(row: Row, p: VectorizableParam[_]): Option[Any] = {
    get(p).map {
      case Right(colName) => row.get(row.fieldIndex(colName))
      case Left(value) => value
    } match {
      case None =>
        getDefault(p).map {
          case Right(colName) => row.get(row.fieldIndex(colName))
          case Left(value) => value
        }
      case s => s
    }
  }

  def getValueAny(row: Row, p: VectorizableParam[_]): Any =
    getValueAnyOpt(row, p).get

  def getValueMap(row: Row, excludes: Set[VectorizableParam[_]] = Set()): Map[String, Any] = {
    this.params.flatMap {
      case p: VectorizableParam[_] if !excludes(p) =>
        getValueOpt(row, p).map(v => (p.name, v))
      case _ => None
    }.toMap
  }
}

trait HasSubscriptionKey extends HasVectorizableParams {
  val subscriptionKey = new VectorizableParam[String](
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

  protected def inputFunc(schema: StructType): Row => HttpRequestBase

  protected def getInternalInputParser(schema: StructType): HTTPInputParser = {
    new CustomInputParser().setUDF(inputFunc(schema))
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
