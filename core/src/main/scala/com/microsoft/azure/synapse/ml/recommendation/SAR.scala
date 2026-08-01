// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import breeze.linalg.{CSCMatrix => BSM}
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.recommendation.{Constants => C, RecommendationParams}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, SparseMatrix}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, collect_list, countDistinct, lit, max, row_number, struct, sum, udf}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, NumericType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Smart Adaptive Recommendations (SAR) Algorithm
  *
  * https://aka.ms/reco-sar
  *
  * SAR is a fast scalable adaptive algorithm for personalized recommendations based on user transactions history and
  * items description. It produces easily explainable / interpretable recommendations.
  *
  * User and item identifiers can be strings or numeric values. SAR deterministically indexes the identifiers while
  * fitting and stores both reversible mappings in the resulting model. Caller-visible outputs use the original types.
  *
  * @param uid The id of the module
  */
class SAR(override val uid: String) extends Estimator[SARModel]
  with SARParams with DefaultParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  /** @group getParam */
  def getSimilarityFunction: String = $(similarityFunction)

  /** @group getParam */
  def getTimeCol: String = $(timeCol)

  /** @group getParam */
  def getSupportThreshold: Int = $(supportThreshold)

  /** @group getParam */
  def getStartTimeFormat: String = $(startTimeFormat)

  /** @group getParam */
  def getActivityTimeFormat: String = $(activityTimeFormat)

  /** @group getParam */
  def getTimeDecayCoeff: Int = $(timeDecayCoeff)

  def this() = this(Identifiable.randomUID("SAR"))

  override def copy(extra: ParamMap): SAR = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    SAR.validateIdentifierColumn(schema, getUserCol)
    SAR.validateIdentifierColumn(schema, getItemCol)
    SAR.validateNumericColumnIfPresent(schema, getRatingCol)
    SAR.appendPrediction(schema, getPredictionCol)
  }

  override def fit(dataset: Dataset[_]): SARModel = {
    logFit({
      transformSchema(dataset.schema)

      // RecommendationIndexer stringifies numeric values and exposes its index columns to callers. SAR instead owns
      // typed mappings so direct identifiers round-trip without an extra pipeline stage or a lossy recovery cast.
      val userIdMapping = buildIdMapping(dataset, getUserCol)
      val itemIdMapping = buildIdMapping(dataset, getItemCol)
      val indexedUserCol = SAR.unusedColumnName(dataset, "__sar_user_index")
      val withUsers = SAR.attachIdMapping(dataset, userIdMapping, getUserCol, indexedUserCol)
      val indexedItemCol = SAR.unusedColumnName(withUsers, "__sar_item_index")
      val indexed = SAR.attachIdMapping(withUsers, itemIdMapping, getItemCol, indexedItemCol)

      val userData = calculateUserItemAffinities(indexed, indexedUserCol, indexedItemCol)
        .withColumnRenamed(indexedUserCol, getUserCol)
      val itemData = calculateItemItemSimilarity(indexed, indexedUserCol, indexedItemCol)
        .withColumnRenamed(indexedItemCol, getItemCol)

      val model = new SARModel(uid)
        .setUserDataFrame(userData)
        .setItemDataFrame(itemData)
        .setUserIdMapping(userIdMapping)
        .setItemIdMapping(itemIdMapping)
      copyValues(model).setParent(this)
    }, dataset.columns.length)
  }

  private def buildIdMapping(dataset: Dataset[_], inputCol: String): DataFrame = {
    val values = dataset.select(col(inputCol).as(SAR.OriginalIdCol))
    val containsNull = values.filter(col(SAR.OriginalIdCol).isNull).limit(1).count() > 0
    require(!containsNull, s"SAR does not support null identifiers in column $inputCol")

    val distinctValues = values.distinct()
    val valueCount = distinctValues.count()
    require(valueCount > 0, s"SAR requires at least one identifier in column $inputCol")
    require(valueCount <= Int.MaxValue,
      s"SAR supports at most ${Int.MaxValue} distinct identifiers in column $inputCol, but found $valueCount")

    val deterministicOrder = Window.orderBy(col(SAR.OriginalIdCol).asc)
    distinctValues.withColumn(
      SAR.IndexCol,
      (row_number().over(deterministicOrder) - 1).cast(DoubleType)
    )
  }

  /**
    * Retained for package-level compatibility. Fitting uses the same deterministic indexing.
    */
  private[ml] def calculateUserItemAffinities(dataset: Dataset[_]): DataFrame = {
    val userIdMapping = buildIdMapping(dataset, getUserCol)
    val itemIdMapping = buildIdMapping(dataset, getItemCol)
    val indexedUserCol = SAR.unusedColumnName(dataset, "__sar_user_index")
    val withUsers = SAR.attachIdMapping(dataset, userIdMapping, getUserCol, indexedUserCol)
    val indexedItemCol = SAR.unusedColumnName(withUsers, "__sar_item_index")
    val indexed = SAR.attachIdMapping(withUsers, itemIdMapping, getItemCol, indexedItemCol)
    calculateUserItemAffinities(indexed, indexedUserCol, indexedItemCol)
      .withColumnRenamed(indexedUserCol, getUserCol)
  }

  private def calculateUserItemAffinities(
      dataset: Dataset[_],
      indexedUserCol: String,
      indexedItemCol: String): DataFrame = {
    val referenceTime: Date = new SimpleDateFormat(getStartTimeFormat)
      .parse(get(startTime).getOrElse(Calendar.getInstance().getTime.toString))

    val timeDecay = udf((time: String) => {
      val activityDate = new SimpleDateFormat(getActivityTimeFormat).parse(time)
      val timeDifference = (referenceTime.getTime - activityDate.getTime) / (1000 * 60)
      math.pow(2, -1.0 * timeDifference / (getTimeDecayCoeff * 24 * 60))
    })

    val itemCount = dataset.select(max(col(indexedItemCol))).first().getDouble(0).toInt + 1
    val seqToArray = udf((itemUserAffinityPairs: Seq[Row]) => {
      val values = Array.fill[Float](itemCount)(0.0f)
      itemUserAffinityPairs.foreach(pair => values(pair.getDouble(0).toInt) = pair.getDouble(1).toFloat)
      values
    })

    val hasTime = dataset.columns.contains(getTimeCol)
    val hasRating = dataset.columns.contains(getRatingCol)
    val affinity = (hasTime, hasRating) match {
      case (true, true) => timeDecay(col(getTimeCol).cast(StringType)) * col(getRatingCol).cast(DoubleType)
      case (true, false) => timeDecay(col(getTimeCol).cast(StringType))
      case (false, true) => col(getRatingCol).cast(DoubleType)
      case (false, false) => lit(1.0)
    }

    dataset
      .withColumn(C.AffinityCol, affinity)
      .select(indexedUserCol, indexedItemCol, C.AffinityCol)
      .groupBy(indexedUserCol, indexedItemCol)
      .agg(sum(col(C.AffinityCol)).cast(DoubleType).as(C.AffinityCol))
      .withColumn("itemUserAffinityPair",
        struct(col(indexedItemCol), col(C.AffinityCol)))
      .groupBy(indexedUserCol)
      .agg(collect_list(col("itemUserAffinityPair")).as("itemUserAffinityPairs"))
      .withColumn("flatList", seqToArray(col("itemUserAffinityPairs")))
      .select(col(indexedUserCol), col("flatList"))
  }

  /**
    * Retained for package-level compatibility. Fitting uses the same deterministic indexing.
    */
  private[ml] def calculateItemItemSimilarity(dataset: Dataset[_]): DataFrame = {
    val userIdMapping = buildIdMapping(dataset, getUserCol)
    val itemIdMapping = buildIdMapping(dataset, getItemCol)
    val indexedUserCol = SAR.unusedColumnName(dataset, "__sar_user_index")
    val withUsers = SAR.attachIdMapping(dataset, userIdMapping, getUserCol, indexedUserCol)
    val indexedItemCol = SAR.unusedColumnName(withUsers, "__sar_item_index")
    val indexed = SAR.attachIdMapping(withUsers, itemIdMapping, getItemCol, indexedItemCol)
    calculateItemItemSimilarity(indexed, indexedUserCol, indexedItemCol)
      .withColumnRenamed(indexedItemCol, getItemCol)
  }

  private def collectItemCounts(
      dataset: Dataset[_],
      indexedUserCol: String,
      indexedItemCol: String): Map[Int, Long] = {
    dataset
      .groupBy(col(indexedItemCol))
      .agg(countDistinct(col(indexedUserCol)))
      .collect()
      .map(row => row.getDouble(0).toInt -> row.getLong(1))
      .toMap
  }

  private def createInteractionMatrix(
      dataset: Dataset[_],
      indexedUserCol: String,
      indexedItemCol: String,
      userCount: Int,
      itemCount: Int): BSM[Double] = {
    val sparse = SparseMatrix.fromCOO(userCount, itemCount,
      dataset
        .select(col(indexedUserCol), col(indexedItemCol))
        .distinct()
        .collect()
        .map(pair => (pair.getDouble(0).toInt, pair.getDouble(1).toInt, 1.0)))
    new BSM[Double](sparse.values, sparse.numRows, sparse.numCols, sparse.colPtrs, sparse.rowIndices)
  }

  private def itemFeaturesVector(
      userCount: Int,
      interactionMatrix: Broadcast[BSM[Double]]): UserDefinedFunction = {
    udf((users: Seq[Double]) => {
      val values = Array.fill[Double](userCount)(0.0)
      users.foreach(user => values(user.toInt) = 1.0)
      val matrix = Matrices.dense(1, values.length, values).asML.toSparse
      val breezeMatrix = new BSM[Double](
        matrix.values,
        matrix.numRows,
        matrix.numCols,
        matrix.colPtrs,
        matrix.rowIndices
      )
      val multiplied: BSM[Double] = breezeMatrix * interactionMatrix.value
      new DenseVector(multiplied.toDense.toArray)
    })
  }

  private def similarityFeature(
      itemCounts: Broadcast[Map[Int, Long]]): UserDefinedFunction = {
    udf((itemId: Double, features: linalg.Vector) => {
      val countI = features(itemId.toInt)
      features.toArray.indices.map(index => {
        val countJ = itemCounts.value.getOrElse(index, 0L)
        val cooccurrence = features(index)
        if (cooccurrence >= getSupportThreshold) {
          getSimilarityFunction match {
            case "jaccard" => (cooccurrence / (countI + countJ - cooccurrence)).toFloat
            case "lift" => (cooccurrence / (countI * countJ)).toFloat
            case _ => cooccurrence.toFloat
          }
        } else {
          0.0f
        }
      })
    })
  }

  private def calculateItemItemSimilarity(
      dataset: Dataset[_],
      indexedUserCol: String,
      indexedItemCol: String): DataFrame = {
    val context = dataset.sparkSession.sparkContext
    val itemCounts = context.broadcast(collectItemCounts(dataset, indexedUserCol, indexedItemCol))
    val maxCounts = dataset.agg(max(col(indexedUserCol)), max(col(indexedItemCol))).first()
    val userCount = maxCounts.getDouble(0).toInt + 1
    val itemCount = maxCounts.getDouble(1).toInt + 1
    val interactionMatrix = context.broadcast(
      createInteractionMatrix(dataset, indexedUserCol, indexedItemCol, userCount, itemCount)
    )

    dataset
      .select(col(indexedItemCol), col(indexedUserCol))
      .groupBy(indexedItemCol)
      .agg(collect_list(indexedUserCol).as("users"))
      .withColumn(C.FeaturesCol, itemFeaturesVector(userCount, interactionMatrix)(col("users")))
      .select(col(indexedItemCol), col(C.FeaturesCol))
      .withColumn(C.ItemAffinities, similarityFeature(itemCounts)(col(indexedItemCol), col(C.FeaturesCol)))
      .select(col(indexedItemCol), col(C.ItemAffinities))
  }
}

object SAR extends DefaultParamsReadable[SAR] {
  private[recommendation] val OriginalIdCol = "originalID"
  private[recommendation] val IndexCol = "index"

  private[recommendation] def validateIdentifierColumn(schema: StructType, columnName: String): Unit = {
    val dataType = schema(columnName).dataType
    require(dataType == StringType || dataType.isInstanceOf[NumericType],
      s"Column $columnName must be string or numeric, but was $dataType")
  }

  private[recommendation] def validateNumericColumnIfPresent(
      schema: StructType,
      columnName: String): Unit = {
    if (schema.fieldNames.contains(columnName)) {
      val dataType = schema(columnName).dataType
      require(dataType.isInstanceOf[NumericType],
        s"Column $columnName must be numeric, but was $dataType")
    }
  }

  private[recommendation] def appendPrediction(schema: StructType, predictionCol: String): StructType = {
    require(!schema.fieldNames.contains(predictionCol), s"Output column $predictionCol already exists")
    StructType(schema.fields :+ StructField(predictionCol, FloatType, nullable = false))
  }

  private[recommendation] def unusedColumnName(dataset: Dataset[_], baseName: String): String = {
    Iterator.from(0)
      .map(index => if (index == 0) baseName else s"${baseName}_$index")
      .find(name => !dataset.columns.contains(name))
      .get
  }

  private[recommendation] def identifierTypesCompatible(actualType: DataType, trainedType: DataType): Boolean = {
    actualType == trainedType ||
      (actualType.isInstanceOf[NumericType] && trainedType.isInstanceOf[NumericType])
  }

  private[recommendation] def attachIdMapping(
      dataset: Dataset[_],
      mapping: DataFrame,
      inputCol: String,
      outputCol: String): DataFrame = {
    val actualType = dataset.schema(inputCol).dataType
    val trainedType = mapping.schema(OriginalIdCol).dataType
    require(identifierTypesCompatible(actualType, trainedType),
      s"Column $inputCol has type $actualType, but SAR was trained with $trainedType")

    val (prepared, identifier) = if (actualType == trainedType) {
      val frame = dataset.toDF()
      (frame, frame(inputCol))
    } else {
      val castedCol = unusedColumnName(dataset, "__sar_casted_identifier")
      val casted = dataset.withColumn(castedCol, col(inputCol).cast(trainedType))
      val safelyCasted = casted.filter(
        casted(inputCol).isNotNull &&
          casted(castedCol).isNotNull &&
          (casted(castedCol).cast(actualType) === casted(inputCol))
      )
      (safelyCasted, safelyCasted(castedCol))
    }
    val originalColumns = dataset.columns.map(prepared(_))

    prepared
      .join(mapping, identifier === mapping(OriginalIdCol), "inner")
      .select((originalColumns :+ mapping(IndexCol).as(outputCol)): _*)
  }
}

trait SARParams extends Wrappable with RecommendationParams {

  /** @group setParam */
  def setSimilarityFunction(value: String): this.type = set(similarityFunction, value)

  val similarityFunction = new Param[String](this, "similarityFunction",
    "Defines the similarity function to be used by the model. Lift favors serendipity, " +
      "Co-occurrence favors predictability, and Jaccard is a compromise between the two.")

  /** @group setParam */
  def setTimeCol(value: String): this.type = set(timeCol, value)

  val timeCol = new Param[String](this, "timeCol", "Time of activity")

  /** @group setParam */
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  def setSupportThreshold(value: Int): this.type = set(supportThreshold, value)

  val supportThreshold = new IntParam(this, "supportThreshold", "Minimum number of ratings per item")

  def setStartTime(value: String): this.type = set(startTime, value)

  val startTime = new Param[String](this, "startTime", "Set time custom now time if using historical data")

  def setActivityTimeFormat(value: String): this.type = set(activityTimeFormat, value)

  val activityTimeFormat = new Param[String](this, "activityTimeFormat", "Time format for events, " +
    "default: yyyy/MM/dd'T'h:mm:ss")

  def setTimeDecayCoeff(value: Int): this.type = set(timeDecayCoeff, value)

  val timeDecayCoeff = new IntParam(this, "timeDecayCoeff", "Use to scale time decay coeff to different half life dur")

  def setStartTimeFormat(value: String): this.type = set(startTimeFormat, value)

  val startTimeFormat = new Param[String](this, "startTimeFormat", "Format for start time")

  setDefault(timeDecayCoeff -> 30, activityTimeFormat -> "yyyy/MM/dd'T'h:mm:ss", supportThreshold -> 4,
    ratingCol -> C.RatingCol, userCol -> C.UserCol, itemCol -> C.ItemCol, similarityFunction ->
      "jaccard", timeCol -> "time", startTimeFormat -> "EEE MMM dd HH:mm:ss Z yyyy")
}
