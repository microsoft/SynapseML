
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.param._
import org.apache.spark.ml.recommendation.MsftRecommendationParams
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.{Estimator, Pipeline}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseMatrix}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, collect_list, sum, udf, _}
import org.apache.spark.sql.types.{IntegerType, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable
import scala.language.existentials

/** SAR
  *
  * @param uid The id of the module
  */
@InternalWrapper
class SAR(override val uid: String) extends Estimator[SARModel] with SARParams with
  DefaultParamsWritable {

  /** @group getParam */
  def getSimilarityFunction: String = $(similarityFunction)

  /** @group getParam */
  def getTimeCol: String = $(timeCol)

  /** @group getParam */
  def getItemFeatures: DataFrame = $(itemFeatures)

  /** @group getParam */
  def getSupportThreshold: Int = $(supportThreshold)

  override def fit(dataset: Dataset[_]): SARModel = {

    val sarModel = if ($(autoIndex)) {

      val customerIndex = new StringIndexer()
        .setInputCol($(userCol))
        .setOutputCol($(userCol) + "temp")

      val ratingsIndex = new StringIndexer()
        .setInputCol($(itemCol))
        .setOutputCol($(itemCol) + "temp")

      val pipeline = new Pipeline()
        .setStages(Array(customerIndex, ratingsIndex))

      val pipelineModel = pipeline
        .fit(dataset)

      val transformedDf = pipelineModel
        .transform(dataset)
        .withColumnRenamed($(itemCol), $(itemCol) + "_org")
        .withColumnRenamed($(userCol), $(userCol) + "_org")
        .withColumnRenamed($(itemCol) + "temp", $(itemCol))
        .withColumnRenamed($(userCol) + "temp", $(userCol))

      val itemAffinityMatrix = calculateItemAffinities(transformedDf)
      val userAffinityMatrix = calculateUserAffinities(transformedDf)

      val recoverUser = {
        val userMap = pipelineModel
          .stages(0).asInstanceOf[StringIndexerModel]
          .labels
          .zipWithIndex
          .map(t => (t._2, t._1))
          .toMap

        val userMapBC = dataset.sparkSession.sparkContext.broadcast(userMap)
        udf((userID: Integer) => userMapBC.value.getOrElse[String](userID, "-1"))
      }
      val recoverItem = {
        val itemMap = pipelineModel
          .stages(1).asInstanceOf[StringIndexerModel]
          .labels
          .zipWithIndex
          .map(t => (t._2, t._1))
          .toMap

        val itemMapBC = dataset.sparkSession.sparkContext.broadcast(itemMap)
        udf((itemID: Integer) => itemMapBC.value.getOrElse[String](itemID, "-1"))
      }

      new SARModel(uid)
        .setUserDataFrame(userAffinityMatrix.withColumn($(userCol) + "_org", recoverUser(col($(userCol)))))
        .setItemDataFrame(itemAffinityMatrix.withColumn($(itemCol) + "_org", recoverItem(col($(itemCol)))))
    }
    else {

      val itemAffinityMatrix = calculateItemAffinities(dataset)
      val userAffinityMatrix = calculateUserAffinities(dataset)

      new SARModel(uid)
        .setUserDataFrame(userAffinityMatrix)
        .setItemDataFrame(itemAffinityMatrix)
    }
    sarModel
      .setParent(this)
      .setSupportThreshold(getSupportThreshold)
      .setItemCol(getItemCol)
      .setUserCol(getUserCol)
      .setAllowSeedItemsInRecommendations($(allowSeedItemsInRecommendations))
      .setAutoIndex($(autoIndex))
  }

  override def copy(extra: ParamMap): SAR = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def this() = this(Identifiable.randomUID("SAR"))

  def calculateItemAffinities(df: Dataset[_]): DataFrame = {
    SAR.calculateItemAffinities(getUserCol, getItemCol, getSupportThreshold, df, get(itemFeatures),
      getSimilarityFunction)
  }

  def calculateUserAffinities(dataset: Dataset[_]): DataFrame = {
    val referenceTime: Date = new SimpleDateFormat($(startTimeFormat))
      .parse(get(startTime).getOrElse(Calendar.getInstance().getTime.toString))

    val timeDecay = udf((time: String) => {
      val activityDate = new SimpleDateFormat($(activityTimeFormat)).parse(time)
      val timeDifference = (referenceTime.getTime - activityDate.getTime) / (1000 * 60)
      math.pow(2, -1.0 * timeDifference / ($(timeDecayCoeff) * 24 * 60))
    })
    val ratingWeight = udf((rating: Double) => rating)
    val blendWeights = udf((theta: Double, roe: Double) => theta * roe)

    val itemCount = dataset.select(col($(itemCol)).cast(IntegerType)).groupBy().max($(itemCol)).collect()(0).getInt(0)
    val numItems = dataset.sparkSession.sparkContext.broadcast(itemCount)

    val calculateCustomerAffinity = {
      if (dataset.columns.contains($(timeCol)) && dataset.columns.contains($(ratingCol))) {
        dataset
          .withColumn("affinity", blendWeights(timeDecay(col($(timeCol))), ratingWeight(col($(ratingCol)))))
          .select(getUserCol, getItemCol, "affinity")
      }
      else if (dataset.columns.contains($(timeCol))) {
        dataset
          .withColumn("affinity", timeDecay(col($(timeCol))))
          .select(getUserCol, getItemCol, "affinity")
      }
      else if (dataset.columns.contains($(ratingCol))) {
        dataset
          .withColumn("affinity", ratingWeight(col($(ratingCol))))
          .select(getUserCol, getItemCol, "affinity")
      } else {
        val fillOne = udf((_: String) => 1)
        dataset
          .withColumn("affinity", fillOne(col($(userCol))))
          .select(getUserCol, getItemCol, "affinity")
      }
    }

    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))
    val flattenItems = udf((r: mutable.WrappedArray[mutable.WrappedArray[Double]]) => {
      val map = r.map(r => r(0).toInt -> r(1)).toMap
      val array = (0 to numItems.value).map(i => map.getOrElse(i, 0.0).toFloat).toArray
      array
    })

    val userFeaturesDF = calculateCustomerAffinity
      .groupBy($(userCol), $(itemCol)).agg(sum(col("affinity")) as "affinity")
      .withColumn("itemAffinityPair", wrapColumn(col($(itemCol)), col("affinity")))
      .groupBy($(userCol))
      .agg(collect_list(col("itemAffinityPair")))
      .withColumn("flatList", flattenItems(col("collect_list(itemAffinityPair)")))
      .select(col($(userCol)).cast(IntegerType), col("flatList"))
    userFeaturesDF
  }
}

object SAR extends DefaultParamsReadable[SAR] {

  def calculateItemAffinities(userColumn: String, itemColumn: String, supportThreshold: Int, transformedDf: Dataset[_],
                              itemFeaturesDF: Option[DataFrame], similarityFunction: String): DataFrame = {
    val warmItemWeights: DataFrame = weightWarmItems(userColumn, itemColumn, supportThreshold, transformedDf,
      similarityFunction).cache
    itemFeaturesDF.map(weightColdItems(itemColumn, _, warmItemWeights)).getOrElse(warmItemWeights)
  }

  def weightWarmItems(userColumn: String, itemColumn: String, supportThreshold: Int, transformedDf: Dataset[_],
                      similarityFunction: String): DataFrame = {

    val sc = transformedDf.sparkSession

    transformedDf.cache
    val itemCounts = transformedDf
      .groupBy(col(itemColumn))
      .agg(countDistinct(col(userColumn)))
      .collect()
      .map(r => r.get(0) -> r.getLong(1)).toMap

    val itemCountsBC = sc.sparkContext.broadcast(itemCounts)

    val calculateFeature = udf((itemID: Int, features: linalg.Vector) => {
      val (jaccardFlag, liftFlag) =
        if (similarityFunction == "jaccard") (true, true)
        else if (similarityFunction == "lift") (false, true)
        else (false, false)

      def lift(countI: Double, countJ: Long, cooco: Double) = (cooco / (countI * countJ)).toFloat

      def jaccard(countI: Double, countJ: Long, cooco: Double) = (cooco / (countI + countJ - cooco)).toFloat

      val countI = features.apply(itemID)
      features.toArray.indices.map(i => {
        val countJ: Long = itemCountsBC.value.getOrElse(i, 0)
        val cooco = features.apply(i)
        if (!(cooco < supportThreshold)) {
          if (jaccardFlag)
            jaccard(countI, countJ, cooco)
          else if (liftFlag)
            lift(countI, countJ, cooco)
          else
            cooco.toFloat
        }
        else 0
      })
    })

    val df = if (isLarge()) {
      val itemCount = transformedDf
        .select(col(itemColumn).cast(IntegerType))
        .groupBy()
        .max(itemColumn)
        .collect()(0).getInt(0) + 1

      val userCount = transformedDf
        .select(col(userColumn).cast(IntegerType))
        .groupBy()
        .max(userColumn)
        .collect()(0).getInt(0) + 1

      val df = transformedDf
        .groupBy(userColumn, itemColumn)
        .agg(count(itemColumn))
        .select(col(userColumn).cast(LongType), col(itemColumn).cast(LongType))

      val matrixBC = {
        val rows: Array[Row] = df.collect()
        val tuples: Array[(Int, Int, Double)] = rows.map(f => {
          (f.getLong(0).toInt, f.getLong(1).toInt, 1.0)
        })
        val sparse = SparseMatrix.fromCOO(userCount, itemCount, tuples)
        val sparseBSM: BSM[Double] = new BSM[Double](sparse.values, sparse.numRows, sparse.numCols, sparse.colPtrs,
          sparse.rowIndices)
        transformedDf.sparkSession.sparkContext.broadcast(sparseBSM)
      }

      val makeSparseMatrix = udf((wrappedList: mutable.WrappedArray[Int]) => {
        val vec = Array.fill[Double](userCount)(0.0)
        wrappedList.foreach(user => {
          vec(user) = 1.0
        })
        val sm = Matrices.dense(1, vec.length, vec).asML.toSparse
        val smBSM: BSM[Double] = new BSM[Double](sm.values, sm.numRows, sm.numCols, sm.colPtrs,
          sm.rowIndices)
        val value: BSM[Double] = smBSM * matrixBC.value
        new DenseVector(value.toDense.toArray)
      })

      transformedDf
        .select(col(itemColumn).cast(IntegerType), col(userColumn).cast(IntegerType))
        .groupBy(itemColumn)
        .agg(collect_list(userColumn) as "collect_list")
        .withColumn("features", makeSparseMatrix(col("collect_list")))
        .select(col(itemColumn).cast(IntegerType), col("features"))
    }
    else {
      val rdd = transformedDf
        .groupBy(userColumn, itemColumn)
        .agg(count(itemColumn))
        .select(col(userColumn).cast(LongType), col(itemColumn).cast(LongType)).rdd
        .map(r => MatrixEntry(r.getLong(0), r.getLong(1), 1.0))

      val matrix = new CoordinateMatrix(rdd).toBlockMatrix.cache()

      val rowMatrix = matrix.transpose
        .multiply(matrix)
        .toIndexedRowMatrix()
        .rows.map(index => (index.index.toInt, index.vector))
        .cache()

      sc.createDataFrame(rowMatrix)
        .toDF(itemColumn, "features")
    }

    df.select(col(itemColumn).cast(IntegerType), col("features"))
      .withColumn("jaccardList", calculateFeature(col(itemColumn), col("features")))
      .select(
        col(itemColumn).cast(IntegerType),
        col("jaccardList")
      )
  }

  def isLarge(): Boolean = true //todo: add real is large check here

  def weightColdItems(itemColumn: String, itemFeaturesDF: Dataset[_], jaccard: DataFrame): DataFrame = {
    val sc = itemFeaturesDF.sparkSession

    val itemFeatureVectors = itemFeaturesDF.select(
      col(itemColumn).cast(LongType),
      col("tagId").cast(LongType),
      col("relevance")
    )
      .rdd.map(r => MatrixEntry(r.getLong(0), r.getLong(1), 1.0))

    val itemFeatureMatrix = new CoordinateMatrix(itemFeatureVectors)
      .toBlockMatrix()
      .toIndexedRowMatrix()
      .rows.map(index => (index.index.toInt, index.vector))

    val pairs =
      itemFeatureMatrix.cartesian(itemFeatureMatrix)
        .map(row => {
          val vec1: linalg.Vector = row._1._2
          val vec2 = row._2._2
          //consider if not equal 0
          val productArray = (0 to vec1.argmax + 1).map(i => vec1.apply(i) * vec2.apply(i)).toArray
          (row._1._1, row._2._1, new ml.linalg.DenseVector(productArray))
        })

    val selectScore = udf((itemID: Integer, wrappedList: mutable.WrappedArray[Double]) => wrappedList(itemID))

    val itempairsDF = sc.createDataFrame(pairs)
      .toDF(itemColumn + "1", itemColumn + "2", "features")
      .join(jaccard, col(itemColumn) === col(itemColumn + "1"))
      .withColumn("label", selectScore(col(itemColumn + "2"), col("jaccardList")))
      .select(itemColumn + "1", itemColumn + "2", "features", "label")

    val cold = itempairsDF.where(col("label") < 0)

    val wrapColumn = udf((itemId: Double, rating: Double) => Array(itemId, rating))

    val coldData = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setElasticNetParam(1.0)
      .fit(itempairsDF)
      .transform(cold)
      .withColumn("wrappedPrediction", wrapColumn(col(itemColumn + "2"), col("prediction")))
      .groupBy(itemColumn + "1")
      .agg(collect_list(col("wrappedPrediction")))
      .select(col(itemColumn + "1"), col("collect_list(wrappedPrediction)").as("wrappedPrediction"))

    val mergeScore = udf((jaccard: mutable.WrappedArray[Float], cold: mutable.WrappedArray[mutable
    .WrappedArray[Double]]) => {
      cold.foreach(coldItem => {
        jaccard.update(coldItem(0).toInt, coldItem(1).toFloat)
      })
      jaccard
    })

    val coldJaccard = jaccard
      .join(coldData, col(itemColumn) === col(itemColumn + "1"))
      .withColumn("output", mergeScore(col("jaccardList"), col("wrappedPrediction")))
      .select(col("itemID"), col("output").as("jaccardList"))
    coldJaccard
  }

  val processRow2: UserDefinedFunction = udf((r: mutable.WrappedArray[Double]) => {
    val distinctSet: mutable.Set[Double] = mutable.Set()
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
    val distinctSet: mutable.Set[Double] = mutable.Set()
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

trait SARParams extends Wrappable with MsftRecommendationParams {

  /** @group setParam */
  def setSimilarityFunction(value: String): this.type = set(similarityFunction, value)

  val similarityFunction = new Param[String](this, "similarityFunction",
    "Defines the similarity function to be used by " +
      "the model. Lift favors serendipity, Co-occurrence favors predictability, " +
      "and Jaccard is a nice compromise between the two.")

  /** @group setParam */
  def setTimeCol(value: String): this.type = set(timeCol, value)

  val timeCol = new Param[String](this, "timeCol", "Time of activity")

  /** @group setParam */
  def setItemFeatures(value: DataFrame): this.type = set(itemFeatures, value)

  val itemFeatures = new DataFrameParam2(this, "itemFeatures", "Time of activity")

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def setSupportThreshold(value: Int): this.type = set(supportThreshold, value)

  val supportThreshold = new IntParam(this, "supportThreshold", "Minimum number of ratings per item")

  def setStartTime(value: String): this.type = set(startTime, value)

  val startTime = new Param[String](this, "startTime", "Set time ")

  def setActivityTimeFormat(value: String): this.type = set(activityTimeFormat, value)

  val activityTimeFormat = new Param[String](this, "activityTimeFormat", "Time format for events, " +
    "default: yyyy/MM/dd'T'h:mm:ss")

  def setTimeDecayCoeff(value: Int): this.type = set(timeDecayCoeff, value)

  val timeDecayCoeff = new IntParam(this, "timeDecayCoeff", "Minimum number of ratings per item")

  def setStartTimeFormat(value: String): this.type = set(startTimeFormat, value)

  val startTimeFormat = new Param[String](this, "startTimeFormat", "Minimum number of ratings per item")

  def setAllowSeedItemsInRecommendations(value: Boolean): this.type = set(allowSeedItemsInRecommendations, value)

  val allowSeedItemsInRecommendations = new BooleanParam(this, "allowSeedItemsInRecommendations",
    "Allow seed items (items in the input or in the user history) to be returned as recommendation results. " +
      "True,False " +
      "Default: False")

  def setAutoIndex(value: Boolean): this.type = set(autoIndex, value)

  val autoIndex = new BooleanParam(this, "autoIndex",
    "Auto index customer and item ids if they are not ints" +
      "True,False " +
      "Default: False")

  setDefault(timeDecayCoeff -> 30, activityTimeFormat -> "yyyy/MM/dd'T'h:mm:ss", supportThreshold -> 4,
    ratingCol -> "rating", userCol -> "user", itemCol -> "item", similarityFunction -> "jaccard", timeCol -> "time",
    startTimeFormat -> "EEE MMM dd HH:mm:ss Z yyyy", allowSeedItemsInRecommendations -> true, autoIndex -> false)
}
