// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.schema._
import com.microsoft.ml.spark.CastUtilities._
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.classification._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/** Trains a LightGBM model.
  */
class LightGBM(override val uid: String) extends Estimator[LightGBMModel]
  with HasLabelCol with MMLParams {

  def this() = this(Identifiable.randomUID("LightGBM"))

  /** Fits the LightGBM model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  override def fit(dataset: Dataset[_]): LightGBMModel = {
    com.microsoft.ml.lightgbm.lightgbmlib.LGBM_NetworkFree()
    new LightGBMModel(uid)
  }

  override def copy(extra: ParamMap): Estimator[LightGBMModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBM extends DefaultParamsReadable[LightGBM]

/** Model produced by [[LightGBM]]. */
class LightGBMModel(val uid: String)
    extends Model[LightGBMModel] with ConstructorWritable[LightGBMModel] {

  val ttag: TypeTag[LightGBMModel] = typeTag[LightGBMModel]
  val objectsToSave: List[AnyRef] = List(uid)

  override def copy(extra: ParamMap): LightGBMModel =
    new LightGBMModel(uid)

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMModel extends ConstructorReadable[LightGBMModel]
