// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.core.contracts.{HasInputCol, HasOutputCol, Wrappable}
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

trait AMLParams extends Wrappable with DefaultParamsWritable {

  /** Tokenize the input when set to true
    * @group param
    */
  val useTokenizer = new BooleanParam(this, "useTokenizer", "Whether to tokenize the input")

  /** @group getParam */
  final def getUseTokenizer: Boolean = $(useTokenizer)

}

object AMLEstimator extends DefaultParamsReadable[AMLEstimator]

/** Featurize text.
  *
  * @param uid The id of the module
  */
class AMLEstimator(override val uid: String)
  extends Estimator[AMLModel]
    with AMLParams with HasInputCol with HasOutputCol {
  def this() = this(Identifiable.randomUID("AMLEstimator"))

  setDefault(outputCol, uid + "_output")

  def setUseTokenizer(value: Boolean): this.type = set(useTokenizer, value)

  setDefault(useTokenizer -> true)

  override def fit(dataset: Dataset[_]): AMLModel = {
    ???
  }

  override def copy(extra: ParamMap): this.type =
    defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = {
   ???
  }


}

class AMLModel(val uid: String,
                          fitPipeline: PipelineModel,
                          colsToDrop: List[String])
  extends Model[AMLModel] with DefaultParamsWritable {

  override def copy(extra: ParamMap): AMLModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    ???
  }

  override def transformSchema(schema: StructType): StructType = {
    ???
  }

}

object AMLModel extends DefaultParamsReadable[AMLModel]
