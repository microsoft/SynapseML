// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.MsftRecommendationModelParams
import org.apache.spark.sql.types.{NumericType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

@InternalWrapper
class SARModel(override val uid: String,
               userDataFrame: DataFrame,
               itemDataFrame: DataFrame) extends Model[SARModel]
  with MsftRecommendationModelParams with SARParams with ConstructorWritable[SARModel] {

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

object SARModel extends ConstructorReadable[SARModel]
