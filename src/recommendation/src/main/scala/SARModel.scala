// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.{DataFrameParam, ParamMap}
import org.apache.spark.ml.recommendation.{BaseRecommendationModel, Constants}
import org.apache.spark.ml.util.{ComplexParamsReadable, ComplexParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField => SF, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/** SAR Model
  *
  * @param uid The id of the module
  */
@InternalWrapper
class SARModel(override val uid: String) extends Model[SARModel]
  with BaseRecommendationModel with Wrappable with SARParams with ComplexParamsWritable {

  /** @group setParam */
  def setUserDataFrame(value: DataFrame): this.type = set(userDataFrame, value)

  val userDataFrame = new DataFrameParam(this, "userDataFrame", "Time of activity")

  /** @group getParam */
  def getUserDataFrame: DataFrame = $(userDataFrame)

  /** @group setParam */
  def setItemDataFrame(value: DataFrame): this.type = set(itemDataFrame, value)

  val itemDataFrame = new DataFrameParam(this, "itemDataFrame", "Time of activity")

  /** @group getParam */
  def getItemDataFrame: DataFrame = $(itemDataFrame)

  def this() = this(Identifiable.randomUID("SARModel"))

  /**
    * Personalized recommendations for a single user are obtained by multiplying the Item-to-Item similarity matrix
    * with a user affinity vector. The user affinity vector is simply a transposed row of the affinity matrix
    * corresponding to that user.
    *
    * @param k
    * @return
    */
  override def recommendForAllUsers(k: Int): DataFrame = {
    def dfToRDDMatrxEntry(dataframe: DataFrame) = {
      dataframe.rdd
        .flatMap(row =>
          row.getAs[Seq[Float]](1).zipWithIndex.map { case (list, index) => Row(row.getDouble(0), index, list) })
        .map(item => MatrixEntry(item.getDouble(0).toLong, item.getInt(1).toLong, item.getFloat(2).toDouble))
    }

    val userItemMatrix = new CoordinateMatrix(dfToRDDMatrxEntry(getUserDataFrame)).toBlockMatrix().cache()
    val itemItemMatrix = new CoordinateMatrix(dfToRDDMatrxEntry(getItemDataFrame)).toBlockMatrix().cache()

    val userToItemMatrix = userItemMatrix
      .multiply(itemItemMatrix)
      .toIndexedRowMatrix()
      .rows.map(indexedRow => (indexedRow.index.toInt, indexedRow.vector))

    val orderAndTakeTopK = udf((vector: DenseVector) => {
      vector.toArray.zipWithIndex
        .map { case (list, index) => (index, list) }
        .sortWith(_._2 > _._2)
        .take(k)
    })

    val recommendationArrayType =
      ArrayType(new StructType(Array(SF(getItemCol, IntegerType), SF(Constants.ratingCol, FloatType))))

    getUserDataFrame.sparkSession.createDataFrame(userToItemMatrix)
      .toDF(id, ratings).withColumn(recommendations, orderAndTakeTopK(col(ratings))).select(id, recommendations)
      .select(col(id).as(getUserCol), col(recommendations).cast(recommendationArrayType))
  }

  private val id = Constants.idCol
  private val ratings = Constants.ratingCol + "s"
  private val recommendations = Constants.recommendations

  override def copy(extra: ParamMap): SARModel = {
    val copied = new SARModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transform($(rank), $(userDataFrame), $(itemDataFrame), dataset)
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
}

object SARModel extends ComplexParamsReadable[SARModel]
