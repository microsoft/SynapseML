// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.recommendation.{MsftRecommendationModelParams, MsftRecommendationParams}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{Estimator, Model, Pipeline}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.language.existentials
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/** SAR
  *
  * @param uid The id of the module
  */
class SAR(override val uid: String) extends Estimator[SARModel] with SARParams with DefaultParamsWritable {

  def setTimeCol(value: String): this.type = set(timeCol, value)

  val timeCol = new Param[String](this, "time", "Time of activity")

  /** @group getParam */
  def getTimeCol: String = $(timeCol)

  setDefault(timeCol -> "time")

  private def hash(dataset: Dataset[_]) = {
    val customerIndex = new StringIndexer()
      .setInputCol($(userCol))
      .setOutputCol("customerIDtemp")

    val ratingsIndex = new StringIndexer()
      .setInputCol($(itemCol))
      .setOutputCol("itemIDtemp")

    val pipeline = new Pipeline()
      .setStages(Array(customerIndex, ratingsIndex))

    pipeline.fit(dataset).transform(dataset)
      .drop($(userCol)).withColumnRenamed("customerIDtemp", $(userCol))
      .drop($(itemCol)).withColumnRenamed("itemIDtemp", $(itemCol))
  }

  override def fit(dataset: Dataset[_]): SARModel = {

    val itemSimMatrixdf = itemFeatures(dataset)
    //      .union(coldItemFeatures(dataset, itemFeatures))

    val userAffinityMatrix = userFeatures(dataset)

    new SARModel(uid, userAffinityMatrix, itemSimMatrixdf)
      .setParent(this)
      .setSupportThreshold(getSupportThreshold)
      .setItemCol(getItemCol)
      .setUserCol(getUserCol)
      .setRank(1)
  }

  override def copy(extra: ParamMap): SAR = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def this() = this(Identifiable.randomUID("SAR"))

  def itemFeatures(df: Dataset[_]): DataFrame = {
    SAR.itemFeatures(getUserCol, getItemCol, $(supportThreshold), df)
  }

  def userFeatures(dataset: Dataset[_]): DataFrame = {
    val currentDate: Date = Calendar.getInstance().getTime()
    val timeDecayCoeff = 1
    val ratingBoostCoeff = 1
    val blendCoeff = 1
    val blendIntercept = 0
    val timeDecay = udf((time: String) => {
      val activityDate = new SimpleDateFormat("yyyy-MM-dd h:mm:ss").parse(time)
      timeDecayCoeff / (currentDate.getTime - activityDate.getTime)
    })
    val ratingBoost = udf((rating: Double) => rating * ratingBoostCoeff)
    val blendWeights = udf((theta: Double, roe: Double) => theta * roe * blendCoeff + blendIntercept)
    val blendWeightsRoe = udf((roe: Double) => roe * blendCoeff + blendIntercept)

    val ds = if (dataset.columns.contains($(timeCol))) {
      dataset
        .withColumn("theta", timeDecay(col($(timeCol))))
        .withColumn("roe", ratingBoost(col($(ratingCol))))
        .withColumn("affinity", blendWeights(col("theta"), col("roe")))
        .select(getUserCol, getItemCol, "affinity")
    }
    else {
      dataset
        .withColumn("roe", ratingBoost(col($(ratingCol))))
        .withColumn("affinity", blendWeightsRoe(col("roe")))
        .select(getUserCol, getItemCol, "affinity")
    }

    val itemCount = dataset.select($(itemCol)).distinct.count

    val numItems = dataset.sparkSession.sparkContext.broadcast(itemCount)

    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))

    val flatlist = udf((r: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
      val map = r.map(r => r(0).toInt -> r(1)).toMap
      (1 to numItems.value.toInt).map(i => map.getOrElse(i, 0.0).toFloat).toArray
    })

    val userFeaturesDF = ds
      .withColumn("itemAffinityPair", wrapColumn(col($(itemCol)), col("affinity")))
      .groupBy($(userCol))
      .agg(collect_list(col("itemAffinityPair")))
      .withColumn("flatList", flatlist(col("collect_list(itemAffinityPair)")))
      .select(col($(userCol)).cast(IntegerType), col("flatList"))
    userFeaturesDF
  }

}

trait SARParams extends Wrappable with MsftRecommendationParams {

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

object SAR extends DefaultParamsReadable[SAR] {

  def calcJaccard(map: Map[Double, Map[Double, Double]], supportThreshold: Int): mutable.Map[(Double, Double),
    Double] = {
    val outputMap: mutable.Map[(Double, Double), Double] = mutable.Map()
    for ((keyA, v) <- map) {
      for ((keyB, vv) <- v) {
        val jaccardVal = jaccard(map, keyA, keyB, supportThreshold)
        outputMap.put((keyA, keyB), jaccardVal)
        outputMap.put((keyB, keyA), jaccardVal)
      }
    }
    outputMap
  }

  def jaccard(map: Map[Double, Map[Double, Double]], a: Double, b: Double, supportThreshold: Int): Double = {
    val cooCcur = map.get(a).get(b)
    val occA = map.get(a).get(a)
    val occB = map.get(b).get(b)
    if (occA < supportThreshold || occB < supportThreshold)
      -1
    else
      cooCcur / (occA + occB - cooCcur)
  }

  def calcLift(map: Map[Double, Map[Double, Double]], supportThreshold: Int): mutable.Map[(Double, Double), Double] = {
    val outputMap: mutable.Map[(Double, Double), Double] = mutable.Map()
    for ((keyA, v) <- map) {
      for ((keyB, vv) <- v) {
        val liftVal = lift(map, keyA, keyB, supportThreshold)
        outputMap.put((keyA, keyB), liftVal)
        outputMap.put((keyB, keyA), liftVal)
      }
    }
    outputMap
  }

  def lift(map: Map[Double, Map[Double, Double]], a: Double, b: Double, supportThreshold: Int): Double = {
    val cooCcur = map.get(a).get(b)
    val occA = map.get(a).get(a)
    val occB = map.get(b).get(b)
    if (occA < supportThreshold || occB < supportThreshold)
      -1
    else
      cooCcur / (occA * occB)
  }

  def itemFeatures(userColumn: String, itemColumn: String, supportThreshold: Int, transformedDf: Dataset[_]):
  DataFrame = {

    val sc = transformedDf.sparkSession

    val itemCounts = transformedDf
      .groupBy(col(itemColumn))
      .agg(countDistinct(col(userColumn))).collect()
      .map(r => r.get(0) -> r.getLong(1)).toMap

    val itemCountsBC = sc.sparkContext.broadcast(itemCounts)

    val calculateFeature = udf((itemID: Int, features: linalg.Vector) => {
      val jaccardFlag = true
      val liftFlag = true

      def lift(countI: Double, countJ: Long, cooco: Double) = (cooco / (countI * countJ)).toFloat

      def jaccard(countI: Double, countJ: Long, cooco: Double) = (cooco / (countI + countJ - cooco)).toFloat

      val countI = features.apply(itemID)
      features.toArray.indices.map(i => {
        val countJ: Long = itemCountsBC.value.getOrElse(i, 0)
        if (countI < supportThreshold || countJ < supportThreshold) {
          val cooco = features.apply(i)
          if (jaccardFlag)
            jaccard(countI, countJ, cooco)
          else if (liftFlag)
            lift(countI, countJ, cooco)
          else
            cooco.toFloat
        }
        else -1
      })
    })

    val rdd = transformedDf
      .select(
        col(userColumn).cast(LongType),
        col(itemColumn).cast(LongType)
      ).rdd.map(r => MatrixEntry(r.getLong(0), r.getLong(1), 1.0))

    val matrix = new CoordinateMatrix(rdd).toBlockMatrix().cache()

    val rowMatrix = matrix.transpose
      .multiply(matrix)
      .toIndexedRowMatrix()
      .rows.map(index => (index.index.toInt, index.vector))

    sc.createDataFrame(rowMatrix)
      .toDF(itemColumn, "features")
      .withColumn("jaccardList", calculateFeature(col(itemColumn), col("features")))
      .select(
        col(itemColumn).cast(IntegerType),
        col("jaccardList")
      )

    //    val localMatrix = ata.toLocalMatrix().asML.compressed
    //    val broadcastMatrix = transformedDf.sparkSession.sparkContext.broadcast(localMatrix)
    //
    //    val distinctItems = transformedDf.select("itemID").distinct().cache
    //    val items = distinctItems.collect()
    //    val broadcastItems = transformedDf.sparkSession.sparkContext.broadcast(items)
    //    val jaccardColumn = udf((i: Double) => {
    //      val countI = broadcastMatrix.value.apply(i.toInt, i.toInt)
    //      broadcastItems.value.map(j => {
    //        val countJ = broadcastMatrix.value.apply(j.getDouble(0).toInt, j.getDouble(0).toInt)
    //        val cooco = broadcastMatrix.value.apply(i.toInt, j.getDouble(0).toInt)
    //        val lift = cooco / (countI * countJ)
    //        val jaccard = cooco / (countI + countJ - cooco)
    //        //        MatrixEntry(i.toInt, j.getDouble(0).toInt, jaccard)
    //        Array(i, j.getDouble(0), countI, countJ, cooco, lift, jaccard)
    //      })
    //    })
    //
    //    val distinctItemsDF = distinctItems
    //      .withColumn("jaccard", jaccardColumn(col("itemID")))
    //
    //    val jaccard = distinctItemsDF.collect()
    //
    //    var map2: Map[Double, Map[Double, Double]] = Map()
    //    jaccard.foreach(row => {
    //      row.getAs[mutable.WrappedArray[mutable.WrappedArray[Double]]]("jaccard").foreach(pair => {
    //        var littleMap = map2.getOrElse(pair(0), Map())
    //        val value: Double = pair(6)
    //        if (value != 0) {
    //          littleMap = littleMap + (pair(1) -> value)
    //          map2 = map2 + (pair(0) -> littleMap)
    //        }
    //      })
    //    })

    //    (map2, jaccardListDF)
  }

  val processRow2: UserDefinedFunction = udf((r: mutable.WrappedArray[Double]) => {
    val distinctSet: Set[Double] = Set()
    val map: mutable.Map[(Double, Double), Double] = mutable.Map()

    def incrementMap(i: Double, j: Double) = {
      map.put((i, j), map.getOrElse((i, j), 0.0) + 1)
    }

    r.foreach(distinctSet += _)
    distinctSet.foreach(r => distinctSet.foreach(rr => {
      incrementMap(r, rr)
    }))
    map
  })
  val processRow: UserDefinedFunction = udf((r: mutable.WrappedArray[Double]) => {
    val distinctSet: Set[Double] = Set()
    val map: mutable.Map[Double, mutable.Map[Double, Double]] = mutable.Map()

    def incrementMap(i: Double, j: Double) = {
      val littleMap: mutable.Map[Double, Double] = map.getOrElse(i, mutable.Map())
      val currentCount: Double = littleMap.getOrElse(j, 0.0)
      val updatedCount: Double = currentCount + 1
      littleMap.put(j, updatedCount)
      map.put(i, littleMap)
    }

    r.foreach(distinctSet += _)
    distinctSet.foreach(r => distinctSet.foreach(rr => {
      incrementMap(r, rr)
    }))
    map
  })
}

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
