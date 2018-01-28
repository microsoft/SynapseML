// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.spark.ml.recommendations

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.recommendation.{MsftRecommendationModelParams, MsftRecommendationParams}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.language.existentials

class SAR(override val uid: String) extends Estimator[SARModel]
  with MsftRecommendationParams {

  def setTimeCol(value: String): this.type = set(timeCol, value)

  val timeCol = new Param[String](this, "time", "Time of activity")

  /** @group getParam */
  def getTimeCol: String = $(timeCol)

  setDefault(timeCol -> "time")

  def setSupportThreshold(value: Int): this.type = set(supportThreshold, value)

  val supportThreshold = new Param[Int](this, "supportThreshold", "Warm Cold Item Threshold")

  /** @group getParam */
  def getSupportThreshold: Int = $(supportThreshold)

  setDefault(supportThreshold -> 4)

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  setDefault(userCol -> "user")
  setDefault(itemCol -> "item")
  setDefault(ratingCol -> "rating")

  def userAffinity(dataset: Dataset[_]): DataFrame = {
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
      .withColumn("itemAffinityPair", wrapColumn(col("itemID"), col("affinity")))
      .groupBy("customerID")
      .agg(collect_list(col("itemAffinityPair")))
      .withColumn("flatList", flatlist(col("collect_list(itemAffinityPair)")))
      .select(col("customerID").cast(IntegerType), col("flatList"))
    userFeaturesDF
  }

  override def fit(dataset: Dataset[_]): SARModel = {

    val itemSimMatrixdf = itemFactorize2(dataset)
    val userAffinityMatrix = userAffinity(dataset)

    new SARModel(uid, userAffinityMatrix, itemSimMatrixdf, getSupportThreshold)
      .setParent(this)
      .setItemCol(getItemCol)
      .setUserCol(getUserCol)
  }

  override def copy(extra: ParamMap): SAR = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def this() = this(Identifiable.randomUID("SAR"))

  def itemFactorize2(df: Dataset[_]): DataFrame = {
    SAR.itemFactorize2(getUserCol, getItemCol, df)
  }

  def itemFactorize(df: Dataset[_]): (mutable.Map[Double, Map[Double, (Double, Double, Double, Double, Double,
    Double, Double)]], DataFrame) = {
    SAR.itemFactorize(getUserCol, getItemCol, df)
  }

  def calcJaccard(map: Map[Double, Map[Double, Double]]): mutable.Map[(Double, Double), Double] = {
    SAR.calcJaccard(map, getSupportThreshold)
  }

  def calcLift(map: Map[Double, Map[Double, Double]]): mutable.Map[(Double, Double), Double] = {
    SAR.calcLift(map, getSupportThreshold)
  }
}

object SAR {

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

  def itemFactorize2(userColumn: String, itemColumn: String, transformedDf: Dataset[_]):
  DataFrame = {

    val sc = transformedDf.sparkSession

    val itemCounts = transformedDf
      .groupBy(col(itemColumn))
      .agg(countDistinct(col(userColumn))).collect()
      .map(r => r.getDouble(0).toInt -> r.getLong(1)).toMap

    val itemCountsBC = sc.sparkContext.broadcast(itemCounts)

    val jaccardColumn2 = udf((itemID: Int, features: linalg.Vector) => {
      val countI = features.apply(itemID)
      features.toArray.indices.map(i => {
        val countJ: Long = itemCountsBC.value.getOrElse(i, 0)
        val cooco = features.apply(i)
        val out = cooco / (countI + countJ - cooco)
        out.toFloat
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
      .toDF("itemID", "features")
      .withColumn("jaccardList", jaccardColumn2(col("itemID"), col("features")))
      .select(
        col("itemID").cast(IntegerType),
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

  def itemFactorize(userColumn: String, itemColumn: String, transformedDf: Dataset[_]): (mutable.Map[Double,
    Map[Double, (Double, Double, Double, Double, Double, Double, Double)]], DataFrame) = {

    import org.apache.spark.sql.functions._
    val userToItemsList = transformedDf.groupBy(userColumn)
      .agg(collect_list(col(itemColumn)))
    userToItemsList.cache

    val countedItems = userToItemsList.withColumn("counted", processRow(col("collect_list(" + itemColumn + ")")))

    val func = new CountTableUDAF
    val itemMatrix = countedItems.agg(func(col("counted")))

    var l2 = scala.collection.mutable.ListBuffer[(Double, Double, Double, Double, Double, Double, Double)]()
    import org.apache.spark.mllib.linalg.Vector

    var l3 = scala.collection.mutable.ListBuffer[Vector]()
    //    var testrdd: RDD[Vector] = transformedDf.sparkSession.sparkContext.parallelize(l3)
    val doubleToDoubleToDouble = itemMatrix.collect()(0)(0).asInstanceOf[Map[Double, Map[Double, Double]]]
    val outputMap = mutable.Map[Double, Map[Double, (Double, Double, Double, Double, Double, Double, Double)]]()
    doubleToDoubleToDouble.foreach {
      outer => {
        val outerKey = outer._1
        val outerMap = outer._2
        outerMap.foreach {
          innerMap => {
            val innerKey = innerMap._1
            val cooCcur = innerMap._2
            if (outerKey != innerKey) {
              val occA: Double = doubleToDoubleToDouble.getOrElse(outerKey, Map()).getOrElse(outerKey, 0.0)
              val occB: Double = doubleToDoubleToDouble.getOrElse(innerKey, Map()).getOrElse(innerKey, 0.0)
              val lift = cooCcur / (occA * occB)
              val jaccard = cooCcur / (occA + occB - cooCcur)
              val tuple: (Double, Double, Double, Double, Double, Double, Double) = (outerKey, innerKey, cooCcur,
                occA, occB, lift, jaccard)
              val doubleToTuple: Map[Double, (Double, Double, Double, Double, Double, Double, Double)] = outputMap
                .getOrElse(outerKey, Map())
              outputMap.put(outerKey, (doubleToTuple + (innerKey -> tuple)))
              l2 += tuple
              //              val vector = Vectors.dense(outerKey, innerKey, cooCcur, occA, occB, lift, jaccard)
              //              l3 += vector
              //              testrdd = testrdd.union(transformedDf.sparkSession.sparkContext.parallelize(Seq(vector)))
            }

          }
        }
      }
    }
    val df = transformedDf.sparkSession.createDataFrame(l2)
    //    val value: RDD[Vector] = transformedDf.sparkSession.sparkContext.parallelize(l3)
    //    val matrix = new RowMatrix(value)
    //    matrix
    (outputMap, df)
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
               itemDataFrame: DataFrame,
               minWarmScore: Int) extends Model[SARModel] with MsftRecommendationModelParams {

  override def recommendForAllItems(k: Int): DataFrame = {
    recommendForAllItems(userDataFrame, itemDataFrame, k)
  }

  override def recommendForAllUsers(k: Int): DataFrame = {
    recommendForAllUsers(userDataFrame, itemDataFrame, k)
  }

  //  def recommendForAllUsers_old(k: Int): DataFrame = {
  //    val ss = dataset.sparkSession
  //
  //    val map1: Map[Double, Broadcast[Map[Double, Double]]] = itemSimMatrix.keySet.map(key => {
  //      val littleMap = itemSimMatrix.getOrElse(key, Map())
  //      val bclittleMap = ss.sparkContext.broadcast(littleMap)
  //      (key -> bclittleMap)
  //    }).toMap
  //    val itemSimMatrixBC = ss.sparkContext.broadcast(map1)
  //    //    val itemSimMatrixBC2 = ss.sparkContext.broadcast(itemSimMatrix)
  //    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))
  //    val sortList = udf((list: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
  //      list.sortBy(e => e(1)).reverse
  //    })
  //    val recommend = udf((userId: Double, sortedList: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
  //      var i = 0
  //      var outSeq: Seq[(Double, Double)] = Seq()
  //      val itemsList: mutable.WrappedArray[Double] = sortedList.map(_ (0))
  //
  //      def makeSeq = {
  //        while (outSeq.length < k && i < sortedList.length) {
  //          val first = sortedList(i)
  //          //          val itemMap2 = itemSimMatrixBC2.value.getOrElse(first(0), Map())
  //          val itemMap = itemSimMatrixBC.value.get(first(0)).get.value
  //          val items = ListMap(itemMap.toSeq.sortWith(_._2 < _._2): _*).map(r => {
  //            (r._1, r._2)
  //          })
  //          outSeq = outSeq.union(items.toSeq).distinct.filterNot(itemsList.contains(_)).slice(0, k)
  //          i += 1
  //        }
  //        outSeq
  //      }
  //
  //      def makeSeq2 = {
  //        while (outSeq.length < k && i < sortedList.length) {
  //          val first = sortedList(i)
  //          //          val itemMap = itemSimMatrixBC.value.getOrElse(first(0), Map())
  //          val itemMap = itemSimMatrixBC.value.get(first(0)).get.value
  //          val testItems = ListMap(itemMap.toSeq: _*).map(r => {
  //            (r._1.toDouble, first(1) * r._2)
  //          })
  //          outSeq = outSeq.union(testItems.toSeq)
  //          //          outSeq = outSeq.union(items.toSeq).distinct.filterNot(itemsList.contains(_)).slice(0, k)
  //          i += 1
  //        }
  //        outSeq.sortWith(_._2 > _._2).distinct.filterNot(itemsList.contains(_)).slice(0, k)
  //      }
  //
  //      val testList = makeSeq2
  //      testList.length
  //      makeSeq2.map(_._1)
  //    })
  //    val arrayType = ArrayType(
  //      new StructType()
  //        .add(itemColumn, IntegerType)
  //        .add("rating", FloatType))
  //
  //    dataset
  //      .withColumn("itemIDAffinity", wrapColumn(col(itemColumn), col("affinity")))
  //      .groupBy(col(userColumn))
  //      .agg(collect_list(col("itemIDAffinity")))
  //      .withColumn("sortedList", sortList(col("collect_list(itemIDAffinity)")))
  //      .select(userColumn, "sortedList")
  //      .withColumn("recommended", recommend(col(userColumn), col("sortedList")))
  //      .select(col(userColumn).as(userColumn), col("recommended").as("recommendations." + itemColumn))
  //    //      .select(col(userColumn).as(userColumn), col("recommended").cast(arrayType).as("recommendations." +
  //    // itemColumn))
  //  }

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  override def copy(extra: ParamMap): SARModel = {
    val copied = new SARModel(uid, userDataFrame, itemDataFrame, minWarmScore)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): Nothing = ???

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

  //  override val ttag: TypeTag[SARModel] = typeTag[SARModel]
}

//class SARModel2(override val uid: String,
//                dataset: Dataset[_],
//                itemSimMatrix: mutable.Map[Double, Map[Double, (Double, Double, Double, Double, Double, Double,
//                  Double)]],
//                itemSimDF: DataFrame,
//                userColumn: String,
//                itemColumn: String,
//                minWarmScore: Int) extends Model[SARModel] with MsftRecommendationModelParams {
//
//  def recommendForAllItems(i: Int): Int = 0
//
//  def recommendForAllUsers(k: Int): DataFrame = {
//    val ss = dataset.sparkSession
//    val itemSimMatrixBC = ss.sparkContext.broadcast(itemSimMatrix)
//    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))
//    val sortList = udf((list: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
//      list.sortBy(e => e(1)).reverse
//    })
//    val recommend = udf((userId: Double, sortedList: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
//      var i = 0
//      var outSeq: Seq[(Double, Double)] = Seq()
//      val itemsList: mutable.WrappedArray[Double] = sortedList.map(_ (0))
//
//      while (outSeq.length < k && i < sortedList.length) {
//        val first = sortedList(i)
//        val itemMap = itemSimMatrixBC.value.getOrElse(first(0), Map())
//        val items = ListMap(itemMap.toSeq.sortWith(_._2._7 < _._2._7): _*).map(r => {
//          (r._1, r._2._7)
//        })
//        outSeq = outSeq.union(items.toSeq).distinct.filterNot(itemsList.contains(_)).slice(0, k)
//        i += 1
//      }
//      outSeq
//    })
//    val arrayType = ArrayType(
//      new StructType()
//        .add(itemColumn, IntegerType)
//        .add("rating", FloatType))
//
//    dataset
//      .withColumn("itemIDAffinity", wrapColumn(col(itemColumn), col("affinity")))
//      .groupBy(col(userColumn))
//      .agg(collect_list(col("itemIDAffinity")))
//      .withColumn("sortedList", sortList(col("collect_list(itemIDAffinity)")))
//      .select(userColumn, "sortedList")
//      .withColumn("recommended", recommend(col(userColumn), col("sortedList")))
//      .select(col(userColumn).as(userColumn), col("recommended").cast(arrayType).as("recommendations." + itemColumn))
//  }
//
//  override def copy(extra: ParamMap): SARModel = {
//    val copied = new SARModel2(uid, dataset, itemSimMatrix, itemSimDF, userColumn, itemColumn, minWarmScore)
//    copyValues(copied, extra).setParent(parent)
//  }
//
//  override def transform(dataset: Dataset[_]): Nothing = ???
//
//  override def transformSchema(schema: StructType): StructType = {
//    checkNumericType(schema, $(userCol))
//    checkNumericType(schema, $(itemCol))
//    schema
//  }
//
//  /**
//    * Check whether the given schema contains a column of the numeric data type.
//    *
//    * @param colName column name
//    */
//  private def checkNumericType(
//                                schema: StructType,
//                                colName: String,
//                                msg: String = ""): Unit = {
//    val actualDataType = schema(colName).dataType
//    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
//    require(actualDataType.isInstanceOf[NumericType], s"Column $colName must be of type " +
//      s"NumericType but was actually of type $actualDataType.$message")
//  }
//
//  //  override val ttag: TypeTag[SARModel] = typeTag[SARModel]
//}

class CountTableUDAF2 extends UserDefinedAggregateFunction {
  private val inner = StructType(Seq(
    StructField("_1", DoubleType),
    StructField("_2", DoubleType))
  )

  def bufferSchema: org.apache.spark.sql.types.StructType = {
    new StructType().add("userMap", MapType(inner, DoubleType))
  }

  def dataType: org.apache.spark.sql.types.DataType = {
    MapType(inner, DoubleType)
  }

  def deterministic: Boolean = true

  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    buffer.get(0)
  }

  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer.update(0, mutable.Map())
  }

  def inputSchema: org.apache.spark.sql.types.StructType = {
    new StructType().add("userMap", MapType(inner, DoubleType))
  }

  def merge(buffer1: org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row)
  : Unit = {
    var map1 = buffer1.getAs[scala.collection.immutable.Map[(Double, Double), Double]](0)
    val map2 = buffer2.getAs[scala.collection.immutable.Map[Row, Double]](0)
    val keySet1 = map1.keySet
    val keySet2 = map2.keySet

    for ((aKey, bKey) <- keySet1) {
      val a = map1.getOrElse((aKey, bKey), 0.0)
      val row: Row = Row((aKey, bKey))
      val b = map2.getOrElse(row, 0.0)
      map1 = map1 + ((aKey, bKey) -> (a + b))
    }
    for ((keys: Row) <- keySet2) {
      val aKey = keys.getDouble(0)
      val bKey = keys.getDouble(1)
      val row: Row = Row((aKey, bKey))
      if (!keySet1.contains((aKey, bKey))) {
        val a = map1.getOrElse((aKey, bKey), 0.0)
        val b = map2.getOrElse(row, 0.0)
        map1 = map1 + ((aKey, bKey) -> (a + b))
      }
    }
    buffer1(0) = map1
  }

  def update(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer, input: org.apache.spark.sql.Row)
  : Unit = {
    var map1 = buffer.get(0).asInstanceOf[scala.collection.immutable.Map[(Double, Double), Double]]
    val map2 = input.getAs[scala.collection.immutable.Map[Row, Double]](0)

    val keySet1 = map1.keySet
    val keySet2 = map2.keySet
    for ((aKey, bKey) <- keySet1) {
      val a = map1.getOrElse((aKey, bKey), 0.0)
      val row: Row = Row((aKey, bKey))
      val b = map2.getOrElse(row, 0.0)
      map1 = map1 + ((aKey, bKey) -> (a + b))
    }
    for ((keys: Row) <- keySet2) {
      val aKey = keys.getDouble(0)
      val bKey = keys.getDouble(1)
      if (!keySet1.contains((aKey, bKey))) {
        val a = map1.getOrElse((aKey, bKey), 0.0)
        val row: Row = Row((aKey, bKey))
        val b = map2.getOrElse(row, 0.0)
        map1 = map1 + ((aKey, bKey) -> (a + b))
      }
    }
    buffer(0) = map1
  }

}

class CountTableUDAF extends UserDefinedAggregateFunction {
  def bufferSchema: org.apache.spark.sql.types.StructType = {
    new StructType().add("userMap", MapType(DoubleType, MapType(DoubleType, DoubleType)))
  }

  def dataType: org.apache.spark.sql.types.DataType = {
    MapType(DoubleType, MapType(DoubleType, DoubleType))
  }

  def deterministic: Boolean = true

  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    buffer.get(0)
  }

  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer.update(0, mutable.Map())
  }

  def inputSchema: org.apache.spark.sql.types.StructType = {
    new StructType().add("userMap", MapType(DoubleType, MapType(DoubleType, DoubleType)))
  }

  def merge(buffer1: org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row)
  : Unit = {
    var map1 = buffer1.get(0).asInstanceOf[scala.collection.immutable.Map[Double, scala.collection.immutable
    .Map[Double, Double]]]
    val map2 = buffer2.get(0).asInstanceOf[scala.collection.immutable.Map[Double, scala.collection.immutable
    .Map[Double, Double]]]
    val keys = map1.keySet ++ map2.keySet
    for (key <- keys) {
      var littlemap1 = map1.getOrElse(key, scala.collection.immutable.Map())
      var littlemap2: scala.collection.immutable.Map[Double, Double] = map2.getOrElse(key, scala.collection.immutable
        .Map())
      for ((k, v) <- littlemap2) {
        val m1Count = littlemap1.getOrElse(k, 0.0)
        val count = v + m1Count
        littlemap1 = littlemap1 + (k -> count)
        map1 = map1 + (key -> littlemap1)
      }
    }
    buffer1(0) = map1
  }

  def update(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer, input: org.apache.spark.sql.Row)
  : Unit = {
    var map1 = buffer.get(0).asInstanceOf[scala.collection.immutable.Map[Double, scala.collection.immutable
    .Map[Double, Double]]]
    val map2 = input.get(0).asInstanceOf[scala.collection.immutable.Map[Double, scala.collection.immutable
    .Map[Double, Double]]]
    val keys = map1.keySet ++ map2.keySet
    for (key <- keys) {
      var littlemap1 = map1.getOrElse(key, scala.collection.immutable.Map())
      val littlemap2: scala.collection.immutable.Map[Double, Double] = map2.getOrElse(key, scala.collection.immutable
        .Map())
      for ((k, v) <- littlemap2) {
        val m1Count = littlemap1.getOrElse(k, 0.0)
        val count = v + m1Count
        littlemap1 = littlemap1 + (k -> count)
        map1 = map1 + (k -> littlemap1)
      }
    }
    buffer(0) = map1
  }

}
