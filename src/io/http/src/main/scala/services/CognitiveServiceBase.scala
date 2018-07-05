// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.http.NameValuePair
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.client.utils.URLEncodedUtils
import org.apache.spark.ml.param.{MapParam, Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{NamespaceInjections, PipelineModel, Transformer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spray.json.DefaultJsonProtocol.StringJsonFormat

import scala.collection.JavaConverters._

trait HasSubscriptionKey extends Params {
  val subscriptionKey = new Param[String](this, "subscriptionKey",
    "the API key to use")

  def getSubscriptionKey: String = $(subscriptionKey)

  def setSubscriptionKey(v: String): this.type = set(subscriptionKey, v)

}

trait HasStaticParams extends Params {

  val staticParams = new MapParam[String, String](this, "staticParams",
    "Params which are the same for all requests")(StringJsonFormat, StringJsonFormat)

  def getStaticParams: Map[String, String] = $(staticParams)

  def setStaticParams(v: Map[String, String]): this.type = set(staticParams, v)

  protected def updateStatic(k: String, v: String): this.type =
    setStaticParams(get(staticParams).getOrElse(Map()) + (k -> v))

}

trait HasDynamicParamCols extends Params {

  val dynamicParamCols = new MapParam[String, String](this, "dynamicParamCols",
    "columns that represent parameters")(StringJsonFormat, StringJsonFormat)

  def getDynamicParamCols: Map[String, String] = $(dynamicParamCols)

  def setDynamicParamCols(v: Map[String, String]): this.type = set(dynamicParamCols, v)

  protected def updateDynamicCols(k: String, v: String): this.type =
    setDynamicParamCols(get(dynamicParamCols).getOrElse(Map()) + (k -> v))

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

abstract class CognitiveServicesBase(val uid: String) extends Transformer with HTTPParams with HasOutputCol
  with HasURL with ComplexParamsWritable
  with HasSubscriptionKey with HasStaticParams
  with HasDynamicParamCols with HasErrorCol {

  setDefault(staticParams -> Map(),
    outputCol -> (this.uid + "_output"),
    errorCol -> (this.uid + "_error"),
    handlingStrategy -> "advanced",
    backoffTiming -> Array(100)
  )

  def inputFunc: Map[String, String] => HttpRequestBase

  def responseDataType: DataType

  private[ml] def getInternalTransformer(dynamicParamCol: String, schema: StructType): PipelineModel = {
    NamespaceInjections.pipelineModel(Array(
      new SimpleHTTPTransformer()
        .setInputCol(dynamicParamCol)
        .setOutputCol(getOutputCol)
        .setInputParser(new CustomInputParser().setUDF(inputFunc))
        .setOutputParser(new JSONOutputParser().setDataType(responseDataType))
        .setHandlingStrategy(getHandlingStrategy)
        .setConcurrency(getConcurrency)
        .setConcurrentTimeout(getConcurrentTimeout)
        .setErrorCol(getErrorCol),
      new DropColumns().setCol(dynamicParamCol)
    ))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    // Yes, this map(identity) is needed
    // https://issues.scala-lang.org/browse/SI-7005
    val colToIndex = dataset.sparkSession.sparkContext.broadcast(
      getDynamicParamCols.mapValues(dataset.schema.fieldIndex(_)).map(identity)
    )
    val dynamicParamMapCol = DatasetExtensions.findUnusedColumnName("dynamic", dataset.schema)
    val dfWithDynamicCol = dataset.toDF().map { row =>
      val dynamicParamMap = colToIndex.value.mapValues(i => Option(row.get(i)).map(_.toString).orNull)
      Row.merge(row, Row(dynamicParamMap))
    }(RowEncoder(dataset.schema.add(
      dynamicParamMapCol, MapType(StringType, StringType))))
    getInternalTransformer(dynamicParamMapCol, dataset.schema)
      .transform(dfWithDynamicCol)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(getOutputCol, responseDataType)
      .add(getErrorCol, HTTPSchema.response)
  }
}
