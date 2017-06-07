// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.schema.SchemaConstants.MMLTag
import com.microsoft.ml.spark.schema.{CategoricalColumnInfo, CategoricalMap, CategoricalUtilities, DatasetExtensions}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IndexToValue extends DefaultParamsReadable[IndexToValue]

/**
  * This class takes in a categorical column with MML style attibutes and then transforms
  * it back to the original values.  This extends MLLIB IndexToString by allowing the transformation
  * back to any types of values.
  */

class IndexToValue(val uid: String) extends Transformer with HasInputCol with HasOutputCol with MMLParams {
  def this() = this(Identifiable.randomUID("IndexToValue"))

  /**
    * @param dataset - The input dataset, to be transformed
    * @return The DataFrame that results from column selection
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val info = new CategoricalColumnInfo(dataset.toDF(), getInputCol)
    require(info.isCategorical, "column " + getInputCol + "is not Categorical")
    val dataType = info.dataType
    var getLevel =
      dataType match {
        case _: IntegerType => {
          val map = CategoricalUtilities.getMap[Int](dataset.schema(getInputCol).metadata)
          udf((index: Int) => map.getLevel(index))
        }
        case _: LongType => {
          val map = CategoricalUtilities.getMap[Long](dataset.schema(getInputCol).metadata)
          udf((index: Int) => map.getLevel(index))
        }
        case _: DoubleType => {
          val map = CategoricalUtilities.getMap[Double](dataset.schema(getInputCol).metadata)
          udf((index: Int) => map.getLevel(index))
        }
        case _: StringType => {
          val map = CategoricalUtilities.getMap[String](dataset.schema(getInputCol).metadata)
          udf((index: Int) => map.getLevel(index))
        }
        case _: BooleanType => {
          val map = CategoricalUtilities.getMap[Boolean](dataset.schema(getInputCol).metadata)
          udf((index: Int) => map.getLevel(index))
        }
        case _ => throw new Exception("Unsupported type " + dataType.toString)
      }
    dataset.withColumn(getOutputCol, getLevel(dataset(getInputCol)).as(getOutputCol))
  }

  def transformSchema(schema: StructType): StructType = {
    val metadata = schema(getInputCol).metadata
    val dataType =
      if (metadata.contains(MMLTag)) {
        CategoricalColumnInfo.getDataType(metadata.getMetadata(MMLTag))
      } else {
        schema(getInputCol).dataType
      }
    val newField = StructField(getOutputCol, dataType)
    if (schema.fieldNames.contains(getOutputCol)) {
      val index = schema.fieldIndex(getOutputCol)
      val fields = schema.fields
      fields(index) = newField
      StructType(fields)
    } else {
      schema.add(newField)
    }
  }

  def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
