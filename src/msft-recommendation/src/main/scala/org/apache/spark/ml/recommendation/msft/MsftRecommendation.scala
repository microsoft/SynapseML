// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation.msft

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.BLAS
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.msft.MsftRecommendationModel.MsftRecommendationModelWriter
import org.apache.spark.ml.recommendation.{ALS, ALSModelParams, ALSParams, TopByKeyAggregator}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.BoundedPriorityQueue
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import scala.util.Random

private[recommendation] trait MsftRecommendationModelParams extends Params with ALSModelParams with HasPredictionCol

private[recommendation] trait MsftRecommendationParams extends MsftRecommendationModelParams with ALSParams

class MsftRecommendationModel private[ml](
                                           override val uid: String,
                                           val rank: Int,
                                           val userFactors: DataFrame,
                                           val itemFactors: DataFrame)
  extends Model[MsftRecommendationModel] with MsftRecommendationModelParams with MLWritable {

  def recommendForAllUsers(numItems: Int): DataFrame = {
    recommendForAll(userFactors, itemFactors, $(userCol), $(itemCol), numItems)
  }

  private def recommendForAll(
                               srcFactors: DataFrame,
                               dstFactors: DataFrame,
                               srcOutputColumn: String,
                               dstOutputColumn: String,
                               num: Int): DataFrame = {
    import srcFactors.sparkSession.implicits._

    val srcFactorsBlocked = blockify(srcFactors.as[(Int, Array[Float])])
    val dstFactorsBlocked = blockify(dstFactors.as[(Int, Array[Float])])
    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Seq[(Int, Array[Float])], Seq[(Int, Array[Float])])]
      .flatMap { case (srcIter, dstIter) =>
        val m = srcIter.size
        val n = math.min(dstIter.size, num)
        val output = new Array[(Int, Int, Float)](m * n)
        var i = 0
        val pq = new BoundedPriorityQueue[(Int, Float)](num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = BLAS.f2jBLAS.sdot(rank, srcFactor, 1, dstFactor, 1)
            pq += dstId -> score
          }
          pq.foreach { case (dstId, score) =>
            output(i) = (srcId, dstId, score)
            i += 1
          }
          pq.clear()
        }
        output.toSeq
      }
    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = new TopByKeyAggregator[Int, Int, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Int, Int, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, IntegerType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))
  }

  private def blockify(
                        factors: Dataset[(Int, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Int, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  override def copy(extra: ParamMap): MsftRecommendationModel = {
    val copied = new MsftRecommendationModel(uid, rank, userFactors, itemFactors)
    copyValues(copied, extra).setParent(parent)
  }

  private val predict = udf { (featuresA: Seq[Float], featuresB: Seq[Float]) =>
    if (featuresA != null && featuresB != null) {
      // TODO(SPARK-19759): try dot-producting on Seqs or another non-converted type for
      // potential optimization.
      blas.sdot(rank, featuresA.toArray, 1, featuresB.toArray, 1)
    } else {
      Float.NaN
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    // create a new column named map(predictionCol) by running the predict UDF.
    val predictions = dataset
      .join(userFactors,
        checkedCast(dataset($(userCol))) === userFactors("id"), "left")
      .join(itemFactors,
        checkedCast(dataset($(itemCol))) === itemFactors("id"), "left")
      .select(dataset("*"),
        predict(userFactors("features"), itemFactors("features")).as($(predictionCol)))
    getColdStartStrategy match {
      case MsftRecommendationModel.Drop =>
        predictions.na.drop("all", Seq($(predictionCol)))
      case MsftRecommendationModel.NaN =>
        predictions
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    // user and item will be cast to Int
    SchemaUtils.checkNumericType(schema, $(userCol))
    SchemaUtils.checkNumericType(schema, $(itemCol))
    SchemaUtils.appendColumn(schema, $(predictionCol), FloatType)
  }

  override def write: MsftRecommendationModelWriter = new MsftRecommendationModel.MsftRecommendationModelWriter(this)
}

object MsftRecommendationModel extends MLReadable[MsftRecommendationModel] {

  private val NaN = "nan"
  private val Drop = "drop"

  override def read: MsftRecommendationModelReader = new MsftRecommendationModelReader

  override def load(path: String): MsftRecommendationModel = super.load(path)

  private[MsftRecommendationModel] class MsftRecommendationModelWriter(instance: MsftRecommendationModel)
    extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      val extraMetadata = "rank" -> instance.rank
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val userPath = new Path(path, "userFactors").toString
      instance.userFactors.write.format("parquet").save(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      instance.itemFactors.write.format("parquet").save(itemPath)
    }

  }

  private[MsftRecommendationModel] class MsftRecommendationModelReader extends MLReader[MsftRecommendationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MsftRecommendationModel].getName

    override def load(path: String): MsftRecommendationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      implicit val format: DefaultFormats.type = DefaultFormats
      val rank = (metadata.metadata \ "rank").extract[Int]
      val userPath = new Path(path, "userFactors").toString
      val userFactors = sparkSession.read.format("parquet").load(userPath)
      val itemPath = new Path(path, "itemFactors").toString
      val itemFactors = sparkSession.read.format("parquet").load(itemPath)

      val model = new MsftRecommendationModel(metadata.uid, rank, userFactors, itemFactors)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

}

/** Featurize text.
  *
  * @param uid The id of the module
  */
class MsftRecommendation(override val uid: String) extends Estimator[MsftRecommendationModel]
  with MsftRecommendationParams with HasPredictionCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("msftRecommendation"))

  /** @group setParam */
  @Since("1.3.0")
  def setRank(value: Int): this.type = set(rank, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)

  /** @group setParam */
  @Since("1.3.0")
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)

  /** @group setParam */
  @Since("1.3.0")
  def setAlpha(value: Double): this.type = set(alpha, value)

  /** @group setParam */
  @Since("1.3.0")
  def setUserCol(value: String): this.type = set(userCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setItemCol(value: String): this.type = set(itemCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRatingCol(value: String): this.type = set(ratingCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.3.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.3.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  @Since("1.3.0")
  def setNonnegative(value: Boolean): this.type = set(nonnegative, value)

  /** @group setParam */
  @Since("1.4.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.3.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setIntermediateStorageLevel(value: String): this.type = set(intermediateStorageLevel, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setFinalStorageLevel(value: String): this.type = set(finalStorageLevel, value)

  /** @group expertSetParam */
  @Since("2.2.0")
  def setColdStartStrategy(value: String): this.type = set(coldStartStrategy, value)

  /**
    * Sets both numUserBlocks and numItemBlocks to the specific value.
    *
    * @group setParam
    */
  @Since("1.3.0")
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  override def fit(dataset: Dataset[_]): MsftRecommendationModel = {
    transformSchema(dataset.schema)
    import dataset.sparkSession.implicits._

    val r = if ($(ratingCol) != "") col($(ratingCol)).cast(FloatType) else lit(1.0f)
    val ratings = dataset
      .select(checkedCast(col($(userCol))), checkedCast(col($(itemCol))), r)
      .rdd
      .map { row =>
        Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
      }

    val instr = Instrumentation.create(this, ratings)
    instr.logParams(rank, numUserBlocks, numItemBlocks, implicitPrefs, alpha, userCol,
      itemCol, ratingCol, predictionCol, maxIter, regParam, nonnegative, checkpointInterval,
      seed, intermediateStorageLevel, finalStorageLevel)

    val (userFactors, itemFactors) = ALS.train(ratings, rank = $(rank),
      numUserBlocks = $(numUserBlocks), numItemBlocks = $(numItemBlocks),
      maxIter = $(maxIter), regParam = $(regParam), implicitPrefs = $(implicitPrefs),
      alpha = $(alpha), nonnegative = $(nonnegative),
      intermediateRDDStorageLevel = StorageLevel.fromString($(intermediateStorageLevel)),
      finalRDDStorageLevel = StorageLevel.fromString($(finalStorageLevel)),
      checkpointInterval = $(checkpointInterval), seed = $(seed))
    val userDF = userFactors.toDF("id", "features")
    val itemDF = itemFactors.toDF("id", "features")
    val model = new MsftRecommendationModel(uid, $(rank), userDF, itemDF).setParent(this)
    instr.logSuccess(model)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MsftRecommendation = defaultCopy(extra)
}

object MsftRecommendation extends DefaultParamsReadable[MsftRecommendation]

class MsftRecommendationHelper() {
  def split(dfRaw: DataFrame): (DataFrame, DataFrame) = {
    val ratingsTemp = dfRaw.dropDuplicates()

    val customerIndexer = new StringIndexer()
      .setInputCol("customerID")
      .setOutputCol("customerIDindex")

    val ratingsIndexed1 = customerIndexer.fit(ratingsTemp).transform(ratingsTemp)

    val itemIndexer = new StringIndexer()
      .setInputCol("itemID")
      .setOutputCol("itemIDindex")

    val ratings = itemIndexer.fit(ratingsIndexed1).transform(ratingsIndexed1)
      .drop("customerID").withColumnRenamed("customerIDindex", "customerID")
      .drop("itemID").withColumnRenamed("itemIDindex", "itemID")

    ratings.cache()

    val minRatingsU = 1
    val minRatingsI = 1
    val RATIO = 0.75

    import dfRaw.sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val tmpDF = ratings
      .groupBy("customerID")
      .agg('customerID, count('itemID))
      .withColumnRenamed("count(itemID)", "nitems")
      .where(col("nitems") >= minRatingsU)

    val inputDF = ratings.groupBy("itemID")
      .agg('itemID, count('customerID))
      .withColumnRenamed("count(customerID)", "ncustomers")
      .where(col("ncustomers") >= minRatingsI)
      .join(ratings, "itemID")
      .drop("ncustomers")
      .join(tmpDF, "customerID")
      .drop("nitems")

    inputDF.cache()

    val nusers_by_item = inputDF.groupBy("itemID")
      .agg('itemID, count("customerID"))
      .withColumnRenamed("count(customerID)", "nusers")
      .rdd

    val perm_indices = nusers_by_item.map(r => (r(0), Random.shuffle(List(r(1))), List(r(1))))
    perm_indices.cache()

    val tr_idx = perm_indices.map(r => (r._1, r._2.slice(0, math.round(r._3.size.toDouble * RATIO).toInt)))

    val train = inputDF.rdd
      .groupBy(r => r(1))
      .join(tr_idx)
      .flatMap(r => r._2._1.slice(0, r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2)))
      .toDF("customerID", "itemID", "rating")

    train.cache()

    val testIndex = perm_indices.map(r => (r._1, r._2.drop(math.round(r._3.size.toDouble * RATIO).toInt)))

    val test = inputDF.rdd
      .groupBy(r => r(1))
      .join(testIndex)
      .flatMap(r => r._2._1.drop(r._2._2.size))
      .map(r => (r.getDouble(0).toInt, r.getDouble(1).toInt, r.getInt(2))).toDF("customerID", "itemID", "rating")

    test.cache()

    (train, test)
  }

}

