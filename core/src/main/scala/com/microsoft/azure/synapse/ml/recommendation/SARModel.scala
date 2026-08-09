// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.recommendation

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.param.DataFrameParam
import org.apache.spark.ml.param.{BooleanParam, ParamMap}
import org.apache.spark.ml.recommendation.{BaseRecommendationModel, Constants}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Model}
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  sort_array,
  struct,
  transform => arrayTransform,
  udf
}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, NumericType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/** SAR Model
  *
  * @param uid The id of the module
  */
class SARModel(override val uid: String) extends Model[SARModel]
  with BaseRecommendationModel with Wrappable with SARParams with ComplexParamsWritable with SynapseMLLogging {
  logClass(FeatureNames.Recommendation)

  override protected lazy val pyInternalWrapper = true

  /** @group setParam */
  def setUserDataFrame(value: DataFrame): this.type = set(userDataFrame, value)

  val userDataFrame = new DataFrameParam(this, "userDataFrame", "Internal user affinity factors")

  /** @group getParam */
  def getUserDataFrame: DataFrame = $(userDataFrame)

  /** @group setParam */
  def setItemDataFrame(value: DataFrame): this.type = set(itemDataFrame, value)

  val itemDataFrame = new DataFrameParam(this, "itemDataFrame", "Internal item similarity factors")

  /** @group getParam */
  def getItemDataFrame: DataFrame = $(itemDataFrame)

  /** @group setParam */
  def setUserIdMapping(value: DataFrame): this.type = set(userIdMapping, value)

  val userIdMapping = new DataFrameParam(
    this,
    "userIdMapping",
    "Deterministic mapping from original user identifiers to internal indices"
  )

  /** Returns the model-owned user mapping with columns `originalID` and `index`. */
  def getUserIdMapping: DataFrame = get(userIdMapping)
    .getOrElse(identityMapping(getUserDataFrame, getUserCol))

  /** @group setParam */
  def setItemIdMapping(value: DataFrame): this.type = set(itemIdMapping, value)

  val itemIdMapping = new DataFrameParam(
    this,
    "itemIdMapping",
    "Deterministic mapping from original item identifiers to internal indices"
  )

  /** Returns the model-owned item mapping with columns `originalID` and `index`. */
  def getItemIdMapping: DataFrame = get(itemIdMapping)
    .getOrElse(identityMapping(getItemDataFrame, getItemCol))

  /** Whether user identifiers can use the established integer recommendation schema without loss. */
  val userIdsFitInt = new BooleanParam(
    this,
    "userIdsFitInt",
    "Whether all original user identifiers round-trip through IntegerType"
  )

  def setUserIdsFitInt(value: Boolean): this.type = set(userIdsFitInt, value)

  def getUserIdsFitInt: Boolean = get(userIdsFitInt).getOrElse(false)

  /** Whether item identifiers can use the established integer recommendation schema without loss. */
  val itemIdsFitInt = new BooleanParam(
    this,
    "itemIdsFitInt",
    "Whether all original item identifiers round-trip through IntegerType"
  )

  def setItemIdsFitInt(value: Boolean): this.type = set(itemIdsFitInt, value)

  def getItemIdsFitInt: Boolean = get(itemIdsFitInt).getOrElse(false)

  setDefault(userIdsFitInt -> false, itemIdsFitInt -> false)

  private def identityMapping(factors: DataFrame, identifierColumn: String): DataFrame = {
    val identifierType = factors.schema(identifierColumn).dataType
    val integerCol = SAR.unusedColumnName(factors, "__sar_legacy_integer_identifier")
    val casted = factors.withColumn(integerCol, SAR.tryCast(identifierColumn, IntegerType))
    val identifier = casted(identifierColumn)
    val integerIdentifier = casted(integerCol)
    val roundTripped = SAR.tryCast(integerCol, identifierType)
    casted
      .filter(
        identifier.isNotNull &&
          integerIdentifier.isNotNull &&
          roundTripped.isNotNull &&
          (roundTripped === identifier)
      )
      .select(
        integerIdentifier.as(SAR.OriginalIdCol),
        identifier.cast(DoubleType).as(SAR.IndexCol)
      )
      .distinct()
  }

  def this() = this(Identifiable.randomUID("SARModel"))

  private case class RecommendationSide(
      factors: DataFrame,
      indexColumn: String,
      vectorColumn: String,
      mapping: DataFrame,
      outputColumn: String,
      outputAsInt: Boolean,
      mappingIsLegacy: Boolean,
      mayBeEmpty: Boolean)

  private def userRecommendationSide(
      factors: DataFrame,
      mayBeEmpty: Boolean = false): RecommendationSide = {
    RecommendationSide(
      factors,
      getUserCol,
      "flatList",
      getUserIdMapping,
      getUserCol,
      getUserIdsFitInt,
      mappingIsLegacy = !isSet(userIdMapping),
      mayBeEmpty = mayBeEmpty
    )
  }

  private def itemRecommendationSide(
      factors: DataFrame,
      mayBeEmpty: Boolean = false): RecommendationSide = {
    RecommendationSide(
      factors,
      getItemCol,
      Constants.ItemAffinities,
      getItemIdMapping,
      getItemCol,
      getItemIdsFitInt,
      mappingIsLegacy = !isSet(itemIdMapping),
      mayBeEmpty = mayBeEmpty
    )
  }

  /**
    * Returns top `numItems` items recommended for every user.
    * String and non-integer numeric identifiers retain their training data types.
    */
  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(
      userRecommendationSide(getUserDataFrame),
      itemRecommendationSide(getItemDataFrame),
      numItems
    )
  }

  /**
    * Returns top `numItems` items recommended for each known user in `dataset`.
    * Duplicate, null, and unknown user identifiers do not create output rows.
    */
  def recommendForUserSubset(dataset: Dataset[_], numItems: Int): DataFrame = {
    validateSubsetType(dataset, getUserCol, getUserIdMapping)
    val sourceFactors = getSourceFactorSubset(
      dataset,
      getUserIdMapping,
      getUserCol,
      getUserDataFrame,
      getUserCol
    )
    recommendForAll(
      userRecommendationSide(sourceFactors, mayBeEmpty = true),
      itemRecommendationSide(getItemDataFrame),
      numItems
    )
  }

  /** Returns top users for every item. The `numItems` name is retained for source compatibility. */
  def recommendForAllItems(numItems: Int): DataFrame = {
    recommendForAll(
      itemRecommendationSide(getItemDataFrame),
      userRecommendationSide(getUserDataFrame),
      numItems
    )
  }

  /**
    * Returns top `numUsers` users recommended for each known item in `dataset`.
    * Duplicate, null, and unknown item identifiers do not create output rows.
    */
  def recommendForItemSubset(dataset: Dataset[_], numUsers: Int): DataFrame = {
    validateSubsetType(dataset, getItemCol, getItemIdMapping)
    val sourceFactors = getSourceFactorSubset(
      dataset,
      getItemIdMapping,
      getItemCol,
      getItemDataFrame,
      getItemCol
    )
    recommendForAll(
      itemRecommendationSide(sourceFactors, mayBeEmpty = true),
      userRecommendationSide(getUserDataFrame),
      numUsers
    )
  }

  private def dotProduct = udf((left: Seq[Float], right: Seq[Float]) => {
    left.iterator.zip(right.iterator)
      .map { case (leftValue, rightValue) => leftValue.toDouble * rightValue.toDouble }
      .sum
      .toFloat
  })

  private def recommendationOutputMapping(mapping: DataFrame, outputAsInt: Boolean): DataFrame = {
    val identifierType = mapping.schema(SAR.OriginalIdCol).dataType
    if (outputAsInt && identifierType.isInstanceOf[NumericType] && identifierType != IntegerType) {
      mapping.withColumn(
        SAR.OriginalIdCol,
        SAR.tryCast(SAR.OriginalIdCol, IntegerType)
      )
    } else {
      mapping
    }
  }

  private def recommendForAll(
      source: RecommendationSide,
      destination: RecommendationSide,
      numRecommendations: Int): DataFrame = {
    require(numRecommendations > 0, "The number of recommendations must be positive")
    val outputSource = source.copy(mapping = recommendationOutputMapping(source.mapping, source.outputAsInt))
    val outputDestination = destination.copy(
      mapping = recommendationOutputMapping(destination.mapping, destination.outputAsInt)
    )
    val topScores = rankScores(outputSource, outputDestination, numRecommendations)
    aggregateRecommendations(
      decodeScores(topScores, outputSource, outputDestination),
      outputSource,
      outputDestination
    )
  }

  private def factorMatrix(side: RecommendationSide): CoordinateMatrix = {
    val entries = side.factors
      .select(col(side.indexColumn), col(side.vectorColumn))
      .rdd
      .flatMap(row => {
        val identifier = row.get(0).asInstanceOf[Number].longValue()
        row.getSeq[Float](1).zipWithIndex.map { case (value, featureIndex) =>
          MatrixEntry(identifier, featureIndex.toLong, value.toDouble)
        }
      })
    new CoordinateMatrix(entries)
  }

  private def collectLegacyDestinationIndices(destination: RecommendationSide): Array[Int] = {
    val destinationFactorIndices = destination.factors
      .select(col(destination.indexColumn).cast(DoubleType).as(SARModel.DestinationIndexCol))
      .distinct()
    val destinationMappingIndices = destination.mapping
      .select(col(SAR.IndexCol).cast(DoubleType).as(SARModel.DestinationIndexCol))
      .distinct()
    destinationFactorIndices
      .join(destinationMappingIndices, Seq(SARModel.DestinationIndexCol), "inner")
      .collect()
      .flatMap(row => {
        val index = row.getDouble(0)
        if (index >= 0.0 && index <= Int.MaxValue && index == index.toInt.toDouble) {
          Some(index.toInt)
        } else {
          None
        }
      })
      .distinct
      .sorted
  }

  private def topCandidates(
      scores: Array[Double],
      destinationIndices: Option[Array[Int]],
      numRecommendations: Int): Array[(Double, Int)] = {
    val candidates = destinationIndices match {
      case Some(indices) => indices.iterator
        .filter(_ < scores.length)
        .map(destinationIndex => (scores(destinationIndex), destinationIndex))
      case None => scores.iterator.zipWithIndex
    }
    candidates
      .toArray
      .sortBy { case (score, destinationIndex) => (-score, destinationIndex) }
      .take(numRecommendations)
  }

  private def rankScores(
      source: RecommendationSide,
      destination: RecommendationSide,
      numRecommendations: Int): DataFrame = {
    val spark = source.factors.sparkSession
    if (source.mayBeEmpty && source.factors.isEmpty) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SARModel.ScoreSchema)
    } else {
      val destinationIndices = if (destination.mappingIsLegacy) {
        Some(collectLegacyDestinationIndices(destination))
      } else {
        None
      }
      val scoreRows = factorMatrix(source)
        .toBlockMatrix()
        .multiply(factorMatrix(destination).toBlockMatrix().transpose)
        .toIndexedRowMatrix()
        .rows
        .flatMap(indexedRow => {
          topCandidates(indexedRow.vector.toArray, destinationIndices, numRecommendations)
            .zipWithIndex
            .map { case ((score, destinationIndex), rank) =>
              Row(indexedRow.index.toDouble, destinationIndex.toDouble, score.toFloat, rank + 1)
            }
        })
      val scores = spark.createDataFrame(scoreRows, SARModel.ScoreSchema)
      val actualSources = source.factors
        .select(col(source.indexColumn).cast(DoubleType).as(SARModel.SourceIndexCol))
        .distinct()
      scores.join(actualSources, Seq(SARModel.SourceIndexCol), "inner")
    }
  }

  private def decodeScores(
      topScores: DataFrame,
      source: RecommendationSide,
      destination: RecommendationSide): DataFrame = {
    val sourceMapping = source.mapping.select(
      col(SAR.IndexCol).as(SARModel.SourceMappingIndexCol),
      col(SAR.OriginalIdCol).as(SARModel.SourceOriginalIdCol)
    )
    val destinationMapping = destination.mapping.select(
      col(SAR.IndexCol).as(SARModel.DestinationMappingIndexCol),
      col(SAR.OriginalIdCol).as(SARModel.DestinationOriginalIdCol)
    )

    topScores
      .join(
        sourceMapping,
        topScores(SARModel.SourceIndexCol) === sourceMapping(SARModel.SourceMappingIndexCol),
        "inner"
      )
      .join(
        destinationMapping,
        topScores(SARModel.DestinationIndexCol) ===
          destinationMapping(SARModel.DestinationMappingIndexCol),
        "inner"
      )
  }

  private def aggregateRecommendations(
      decoded: DataFrame,
      source: RecommendationSide,
      destination: RecommendationSide): DataFrame = {
    val recommendation = struct(
      col(SARModel.DestinationOriginalIdCol).as(destination.outputColumn),
      col(SARModel.ScoreCol).as(Constants.RatingCol)
    )
    val rankedRecommendation = struct(
      col(SARModel.RankCol).as(SARModel.RankCol),
      recommendation.as(SARModel.RecommendationValueCol)
    )

    decoded
      .groupBy(col(SARModel.SourceOriginalIdCol))
      .agg(sort_array(collect_list(rankedRecommendation)).as(SARModel.RankedRecommendationsCol))
      .select(
        col(SARModel.SourceOriginalIdCol).as(source.outputColumn),
        arrayTransform(
          col(SARModel.RankedRecommendationsCol),
          entry => entry.getField(SARModel.RecommendationValueCol)
        ).as(Constants.Recommendations)
      )
  }

  private def getSourceFactorSubset(
      dataset: Dataset[_],
      mapping: DataFrame,
      inputColumn: String,
      factors: DataFrame,
      factorIndexColumn: String): DataFrame = {
    val identifiers = dataset.select(col(inputColumn)).distinct()
    val indexColumn = SAR.unusedColumnName(identifiers, "__sar_subset_index")
    val indexedIdentifiers = SAR.attachIdMapping(identifiers, mapping, inputColumn, indexColumn)
    val factorColumns = factors.columns.map(factors(_))

    indexedIdentifiers
      .join(factors, indexedIdentifiers(indexColumn) === factors(factorIndexColumn), "inner")
      .select(factorColumns: _*)
  }

  private def validateSubsetType(dataset: Dataset[_], columnName: String, mapping: DataFrame): Unit = {
    SAR.validateIdentifierColumn(dataset.schema, columnName)
    validateIdentifierType(dataset.schema, columnName, mapping)
  }

  override def copy(extra: ParamMap): SARModel = {
    val copied = new SARModel(uid)
    copyValues(copied, extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(dataset.schema)
      val indexedUserCol = SAR.unusedColumnName(dataset, "__sar_user_index")
      val withUsers = SAR.attachIdMapping(dataset, getUserIdMapping, getUserCol, indexedUserCol)
      val indexedItemCol = SAR.unusedColumnName(withUsers, "__sar_item_index")
      val indexed = SAR.attachIdMapping(withUsers, getItemIdMapping, getItemCol, indexedItemCol)
      val originalColumns = dataset.columns.map(indexed(_))

      val userFactors = getUserDataFrame.alias("sarUserFactors")
      val itemFactors = getItemDataFrame.alias("sarItemFactors")
      indexed
        .join(userFactors, indexed(indexedUserCol) === userFactors(getUserCol), "inner")
        .join(itemFactors, indexed(indexedItemCol) === itemFactors(getItemCol), "inner")
        .select((originalColumns :+ dotProduct(
          col("sarUserFactors.flatList"),
          col(s"sarItemFactors.${Constants.ItemAffinities}")
        ).as(getPredictionCol)): _*)
    }, dataset.columns.length)
  }

  override def transformSchema(schema: StructType): StructType = {
    SAR.validateIdentifierColumn(schema, getUserCol)
    SAR.validateIdentifierColumn(schema, getItemCol)
    validateIdentifierType(schema, getUserCol, getUserIdMapping)
    validateIdentifierType(schema, getItemCol, getItemIdMapping)
    SAR.appendPrediction(schema, getPredictionCol)
  }

  private def validateIdentifierType(schema: StructType, columnName: String, mapping: DataFrame): Unit = {
    val actualType = schema(columnName).dataType
    val trainedType = mapping.schema(SAR.OriginalIdCol).dataType
    require(SAR.identifierTypesCompatible(actualType, trainedType),
      s"Column $columnName has type $actualType, but SAR was trained with $trainedType")
  }
}

object SARModel extends ComplexParamsReadable[SARModel] {
  private val SourceIndexCol = "__sar_source_index"
  private val DestinationIndexCol = "__sar_destination_index"
  private val ScoreCol = "__sar_score"
  private val RankCol = "__sar_rank"
  private val SourceMappingIndexCol = "__sar_source_mapping_index"
  private val DestinationMappingIndexCol = "__sar_destination_mapping_index"
  private val SourceOriginalIdCol = "__sar_source_original_id"
  private val DestinationOriginalIdCol = "__sar_destination_original_id"
  private val RecommendationValueCol = "value"
  private val RankedRecommendationsCol = "__sar_ranked_recommendations"
  private val ScoreSchema = StructType(Seq(
    StructField(SourceIndexCol, DoubleType, nullable = false),
    StructField(DestinationIndexCol, DoubleType, nullable = false),
    StructField(ScoreCol, FloatType, nullable = false),
    StructField(RankCol, IntegerType, nullable = false)
  ))
}
