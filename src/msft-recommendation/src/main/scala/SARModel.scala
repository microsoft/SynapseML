// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.recommendation.{MsftRecommendationModelParams, MsftRecommendationParams}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{NumericType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

@InternalWrapper
class SARModel(override val uid: String,
               userDataFrame: DataFrame,
               itemDataFrame: DataFrame) extends Model[SARModel]
  with MsftRecommendationModelParams with Wrappable with SARModelParams with ConstructorWritable[SARModel] {

  def this() = this(Identifiable.randomUID("SARModel"), null, null)

  override def recommendForAllItems(k: Int): DataFrame = {
    recommendForAllItems($(rank), userDataFrame, itemDataFrame, k)
  }

  override def recommendForAllUsers(k: Int): DataFrame = {
    recommendForAllUsers($(rank), userDataFrame, itemDataFrame, k)
  }

  override def copy(extra: ParamMap): SARModel = {
    val copied = new SARModel(uid, userDataFrame, itemDataFrame)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transform($(rank), userDataFrame, itemDataFrame, dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    checkNumericType(schema, $(userCol))
    checkNumericType(schema, $(itemCol))
    schema
  }

  /**
    * Check whether the given schema contains a column of the numeric data type.
    *
    * @param colName column name
    */
  private def checkNumericType(
                                schema: StructType,
                                colName: String,
                                msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.isInstanceOf[NumericType], s"Column $colName must be of type " +
      s"NumericType but was actually of type $actualDataType.$message")
  }

  override val ttag: TypeTag[SARModel] = typeTag[SARModel]

  override def objectsToSave: List[AnyRef] = List(uid, userDataFrame, itemDataFrame)
}

trait SARModelParams extends Wrappable with MsftRecommendationParams {

  /** @group setParam */
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def setSupportThreshold(value: Int): this.type = set(supportThreshold, value)

  val supportThreshold = new Param[Int](this, "supportThreshold", "Warm Cold Item Threshold")

  /** @group getParam */
  def getSupportThreshold: Int = $(supportThreshold)

  setDefault(supportThreshold -> 4)

  setDefault(ratingCol -> "rating")
  setDefault(userCol -> "user")
  setDefault(itemCol -> "item")

}

object SARModel extends ConstructorReadable[SARModel]
