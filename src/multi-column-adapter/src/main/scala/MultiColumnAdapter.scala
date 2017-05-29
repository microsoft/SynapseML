// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.{PipelineStage, Transformer}
import org.apache.spark.ml.param.{Param, ParamMap, TransformerParam}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.types._

object MultiColumnAdapter extends DefaultParamsReadable[MultiColumnAdapter]

/**
  * The <code>MultiColumnAdapter</code> takes a unary transformer and a list of input output column pairs
  * and applies the transformer to each column
  */
class MultiColumnAdapter(override val uid: String) extends Transformer with MMLParams {

  def this() = this(Identifiable.randomUID("MultiColumnAdapter"))

  /**
    * Comma separated list of input column names, encoded as a string. These are the columns to be transformed.
    * @group param
    */
  val inputCols: Param[String] =
    StringParam(
      this,
      "inputCols",
      "comma separated list of column names encoded as a string")

  /** @group getParam */
  final def getInputCols: String = $(inputCols)

  /** @group setParam */
  def setInputCols(value: String): this.type = set(inputCols, value)

  /**
    * Comma separated list of column names for the transformed columns, encoded as a string.
    * @group param
    */
  val outputCols: Param[String] =
    StringParam(
      this,
      "outputCols",
      "comma separated list of column names encoded as a string")

  /** @group getParam */
  final def getOutputCols: String = $(outputCols)

  /** @group setParam */
  def setOutputCols(value: String): this.type = set(outputCols, value)

  /**
    * @return List of input/output column name pairs
    */
  def getInputOutputPairs: List[(String, String)] =
    getInputCols.split(",").zip(getOutputCols.split(",")).toList

  /**
    * Base transformer to apply to every column in the input column list.
    * @group param
    */
  val baseTransformer: TransformerParam =
    new TransformerParam(this,
                         "baseTransformer",
                         "base transformer to apply to every column")

  /** @group getParam */
  final def getBaseTransformer: Transformer = $(baseTransformer)

  /** @group setParam */
  def setBaseTransformer(value: Transformer): this.type = {
    try {
      //Test to see whether the class has the appropriate getters and setters
      value.getParam("inputCol")
      value.getParam("outputCol")
      setParamInternal(value, "inputCol", this.uid + "__in")
      setParamInternal(value, "outputCol", this.uid + "__out")
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          "Need to pass a transformer with inputCol and outputCol params")
    }
    set(baseTransformer, value)
  }

  private def setParamInternal[M <: PipelineStage, V](model: M,
                                                      name: String,
                                                      value: V) = {
    model.set(model.getParam(name), value)
  }

  private def getParamInternal[M <: PipelineStage](model: M, name: String) = {
    model.getOrDefault(model.getParam(name))
  }

  private def setInOutCols[M <: PipelineStage](
      model: M,
      inputOutputPair: (String, String)) = {
    setParamInternal(setParamInternal(model, "inputCol", inputOutputPair._1),
                     "outputCol",
                     inputOutputPair._2)
  }

  /**
    * Apply the transform to all the columns in the input column list
    * @param dataset
    * @return DataFrame with transformed columns bearing the output column names
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val firstOutput = setInOutCols(getBaseTransformer,
                                   getInputOutputPairs.head).transform(dataset)
    getInputOutputPairs.tail.foldLeft(firstOutput: DataFrame) { (df, pair) =>
      setInOutCols(getBaseTransformer, pair).transform(df)
    }
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def verifyCols(df: DataFrame,
                         inputOutputPairs: List[(String, String)]) = {
    inputOutputPairs.foreach {
      case (s1, s2) if !df.columns.contains(s1) =>
        throw new IllegalArgumentException(
          s"DataFrame does not contain specified column: $s1")
      case (s1, s2) if df.columns.contains(s2) =>
        throw new IllegalArgumentException(
          s"DataFrame already contains specified column: $s2")
      case _ =>
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    getInputOutputPairs.foldLeft(schema) { (schema, pair) =>
      setInOutCols(getBaseTransformer, pair).transformSchema(schema)
    }
  }

}
