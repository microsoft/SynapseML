// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.DatasetExtensions
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, from_json, to_json, struct}
import spire.ClassTag

import DatasetExtensions.findUnusedColumnName

private[ml] abstract class HTTPTransformer[T <: DataFrameClient: ClassTag](val uid: String)
    extends Transformer with MMLParams with ClientTypeAliases {

  val url: Param[String] = new Param(this, "url", "hostname to call")

  /** @group getParam */
  final def getUrl: String = $(url)

  /** @group setParam */
  def setUrl(value: String): this.type = set(url, value)

  val method: Param[String] = new Param(this, "method", "hostname to call")

  /** @group getParam */
  final def getMethod: String = $(method)

  /** @group setParam */
  def setMethod(value: String): this.type = set(method, value)

  def getClient: T

  val clientHolder = SharedVariable {
    getClient
  }

  /** @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = dataset.toDF()
    val enc = RowEncoder(transformSchema(df.schema))
    df.mapPartitions { it =>
      clientHolder.get.send(it)
    }(enc)
  }

  def copy(extra: ParamMap): HTTPTransformer[T] = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType =
    clientHolder.get.transformSchema(schema)

}

abstract class WrappedHTTPTransformer[In, Out](uid: String)
    extends HTTPTransformer[UnaryTransformerWrapper[In, Out]](uid)
    with HasInputCol with HasOutputCol {

  type InnerClient = UnaryTransformerWrapper[In, Out]

  def getBaseClient: BaseClient[In, Out] with ColumnSchema

  override def getClient: InnerClient = ClientTools.toUnaryTransformerClient(getBaseClient)

  def convertToJson: Boolean = getBaseClient match {
    case _: BaseClient[_, _] with ColumnSchema with JsonInput[_] => true
    case _ => false
  }

  def convertFromJson: Boolean = getBaseClient match {
    case _: BaseClient[_, _] with ColumnSchema with JsonOutput[_] => true
    case _ => false
  }

  private def configuredClient(schema: StructType,
                               inputCol:String,
                               outputCol: String): InnerClient = {
    val client = clientHolder.get
    client.setInputIndex(schema.fields.indexOf(schema(inputCol)))
    client.setOutputCol(outputCol)
    client.setUrl(getUrl)
    client
  }

  private def getTempColumns(schema: StructType): (String, String) = {
    val input = if (convertToJson) {
      findUnusedColumnName("input_json")(schema.fieldNames.toSet)
    } else {
      getInputCol
    }
    val output = if (convertFromJson) {
      findUnusedColumnName("output_json")(schema.fieldNames.toSet)
    } else {
      getOutputCol
    }
    (input, output)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val (tempInputCol, tempOutputCol) = getTempColumns(dataset.schema)
    val schema = dataset.schema
    val df = if (convertToJson) {
      val columnOperation = (ic: String) => schema(ic).dataType match {
        case _: StructType => to_json(col(ic))
        case _             => to_json(struct(ic))
      }
      dataset.toDF().withColumn(tempInputCol, columnOperation(getInputCol))
    } else {
      dataset.toDF()
    }
    val modifiedSchema = df.schema
    val driverClient = configuredClient(modifiedSchema, tempInputCol, tempOutputCol)
    val enc = RowEncoder(driverClient.transformSchema(modifiedSchema))
    val outputColumnSchema = transformColumnSchema(schema(getInputCol).dataType)
    val transformed = df.mapPartitions { it =>
      val workerClient = configuredClient(modifiedSchema, tempInputCol, tempOutputCol)
      workerClient.send(it)
    }(enc)
    val postprocessed = if (convertFromJson) {
      transformed.withColumn(getOutputCol,
                             from_json(col(tempOutputCol), outputColumnSchema))
    } else {
      transformed
    }
    val pp0 = postprocessed
    val pp1 = if (convertToJson) pp0.drop(tempInputCol) else pp0
    if (convertFromJson) pp1.drop(tempOutputCol) else pp1
  }

  def transformColumnSchema(schema: DataType): DataType

  override def transformSchema(schema: StructType): StructType = {
    val inputDataType = schema(getInputCol).dataType
    val outputDataType = transformColumnSchema(inputDataType)
    schema.add(getOutputCol, outputDataType)
  }

}
