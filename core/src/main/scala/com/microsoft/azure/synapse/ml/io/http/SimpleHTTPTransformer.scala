// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.http

import com.microsoft.azure.synapse.ml.core.contracts.{HasInputCol, HasOutputCol}
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions.{findUnusedColumnName => newCol}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.TransformerParam
import com.microsoft.azure.synapse.ml.stages.{DropColumns, FlattenBatch, HasMiniBatcher, Lambda}
import org.apache.commons.io.IOUtils
import org.apache.spark.injections.UDFUtils
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object SimpleHTTPTransformer extends ComplexParamsReadable[SimpleHTTPTransformer]

trait HasErrorCol extends Params {
  val errorCol = new Param[String](this, "errorCol", "column to hold http errors")

  def setErrorCol(v: String): this.type = set(errorCol, v)

  def getErrorCol: String = $(errorCol)
  setDefault(errorCol -> "Error")
}

object ErrorUtils extends Serializable {

  val ErrorSchema: StructType = new StructType()
    .add("response", StringType, nullable = true)
    .add("status", StatusLineData.schema, nullable = true)

  protected def addError(fromRow: Row => HTTPResponseData)(responseRow: Row): Option[Row] = {
    val respOpt = Option(responseRow).map(fromRow)
    respOpt match {
      case Some(resp) if resp.statusLine.statusCode == 200 => None
      case Some(resp) =>
        Some(Row(resp.entity.map(entity =>
          IOUtils.toString(entity.content, "UTF-8")), resp.statusLine))
      case None => None
    }
  }

  def nullifyResponse(errorRow: Row, responseRow: Row): Option[Row] = {
    if (errorRow == null && responseRow != null) {
      Some(responseRow)
    } else {
      None
    }
  }

  def addErrorUDF: UserDefinedFunction = {
    val fromRow = HTTPResponseData.makeFromRowConverter
    UDFUtils.oldUdf(addError(fromRow) _, ErrorSchema)
  }

  val NullifyResponseUDF: UserDefinedFunction = UDFUtils.oldUdf(nullifyResponse _, HTTPSchema.Response)

}

class SimpleHTTPTransformer(val uid: String)
  extends Transformer with ConcurrencyParams with HasMiniBatcher with HasHandler
    with HasInputCol with HasOutputCol with ComplexParamsWritable with HasErrorCol with SynapseMLLogging {
  logClass(FeatureNames.Core)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("SimpleHTTPTransformer"))

  val flattenOutputBatches: Param[Boolean] = new BooleanParam(
    this, "flattenOutputBatches", "whether to flatten the output batches")

  /** @group getParam */
  def getFlattenOutputBatches: Boolean = $(flattenOutputBatches)

  /** @group setParam */
  def setFlattenOutputBatches(value: Boolean): this.type = set(flattenOutputBatches, value)

  val inputParser: Param[Transformer] = new TransformerParam(
    this, "inputParser", "format to parse the column to",
    { case _: HTTPInputParser => true; case _ => false })

  /** @group getParam */
  def getInputParser: HTTPInputParser = $(inputParser).asInstanceOf[HTTPInputParser]

  /** @group setParam */
  def setInputParser(value: HTTPInputParser): this.type = set(inputParser, value)

  setDefault(
    inputParser -> new JSONInputParser(),
    handler -> HandlingUtils.advancedUDF(0, 50, 100, 500),  //scalastyle:ignore magic.number
    errorCol -> (this.uid + "_errors"))

  def setUrl(url: String): SimpleHTTPTransformer.this.type = {
    getInputParser match {
      case jip: JSONInputParser => setInputParser(jip.setUrl(url))
      case _ => throw new IllegalArgumentException("this setting is only available when using a JSONInputParser")
    }
  }

  val outputParser: Param[Transformer] = new TransformerParam(
    this, "outputParser", "format to parse the column to",
    { case _: HTTPOutputParser => true; case _ => false })

  /** @group getParam */
  def getOutputParser: HTTPOutputParser = $(outputParser).asInstanceOf[HTTPOutputParser]

  /** @group setParam */
  def setOutputParser(value: HTTPOutputParser): this.type = set(outputParser, value)

  private def makePipeline(schema: StructType): PipelineModel = {
    val colsToAvoid = schema.fieldNames.toSet ++ Set(getOutputCol)

    val parsedInputCol = newCol("parsedInput")(colsToAvoid)
    val unparsedOutputCol = newCol("unparsedOutput")(colsToAvoid)

    val mb = get(miniBatcher)

    val inputParser = Some(getInputParser
      .setInputCol(getInputCol)
      .setOutputCol(parsedInputCol))

    val client = Some(new HTTPTransformer()
      .setHandler(getHandler)
      .setConcurrency(getConcurrency)
      .setConcurrentTimeout(get(concurrentTimeout))
      .setTimeout(getTimeout)
      .setInputCol(parsedInputCol)
      .setOutputCol(unparsedOutputCol))

    val parseErrors = Some(Lambda(_
      .withColumn(getErrorCol, ErrorUtils.addErrorUDF(col(unparsedOutputCol)))
      .withColumn(unparsedOutputCol, ErrorUtils.NullifyResponseUDF(col(getErrorCol), col(unparsedOutputCol)))
    ))

    val outputParser =
      Some(getOutputParser
        .setInputCol(unparsedOutputCol)
        .setOutputCol(getOutputCol))

    val dropCols = Some(new DropColumns().setCols(Array(parsedInputCol, unparsedOutputCol)))

    val flatten = mb.flatMap(_ =>
      get(flattenOutputBatches) match {
        case None => Some(new FlattenBatch())
        case Some(true) => Some(new FlattenBatch())
        case Some(false) => None
      }
    )

    NamespaceInjections.pipelineModel(Array(
      mb, inputParser, client, parseErrors, outputParser, dropCols, flatten
    ).flatten)

  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame](
      makePipeline(dataset.schema).transform(dataset.toDF()),
      dataset.columns.length
    )
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    makePipeline(schema).transformSchema(schema)
  }

}
