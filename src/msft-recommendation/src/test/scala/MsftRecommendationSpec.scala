// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

class MsftRecommendationSpec extends TestBase with EstimatorFuzzing[MsftRecommendation] {

  test("No Cold Start") {
    val dfRaw2: DataFrame = session
      .createDataFrame(Seq(
        ("11", "Movie 01", 4),
        ("11", "Movie 03", 1),
        ("11", "Movie 04", 5),
        ("11", "Movie 05", 3),
        ("11", "Movie 06", 4),
        ("11", "Movie 07", 1),
        ("11", "Movie 08", 5),
        ("11", "Movie 09", 3),
        ("22", "Movie 01", 4),
        ("22", "Movie 02", 5),
        ("22", "Movie 03", 1),
        ("22", "Movie 05", 3),
        ("22", "Movie 06", 4),
        ("22", "Movie 07", 5),
        ("22", "Movie 08", 1),
        ("22", "Movie 10", 3),
        ("33", "Movie 01", 4),
        ("33", "Movie 03", 1),
        ("33", "Movie 04", 5),
        ("33", "Movie 05", 3),
        ("33", "Movie 06", 4),
        ("33", "Movie 08", 1),
        ("33", "Movie 09", 5),
        ("33", "Movie 10", 3),
        ("44", "Movie 01", 4),
        ("44", "Movie 02", 5),
        ("44", "Movie 03", 1),
        ("44", "Movie 05", 3),
        ("44", "Movie 06", 4),
        ("44", "Movie 07", 5),
        ("44", "Movie 08", 1),
        ("44", "Movie 10", 3)))
      .toDF("customerID", "itemID", "rating")

    val dfSplit = MsftRecommendation.split(dfRaw2)
    val dfFit = dfSplit.where("train == 1").drop("train")
    val dfTransform = dfSplit.where("train == 0").drop("train")

    val model = new MsftRecommendation()
      .setRank(1)
      .setRegParam(1)
      .setMaxIter(1)
      .setNumUserBlocks(200)
      .setNumItemBlocks(20)
      .setItemCol("itemID")
      .setUserCol("customerID")
      .setRatingCol("rating")
      .fit(dfFit)

    val items = model.recommendForAllUsers(3)
    val users = model.recommendForAllItems(3)
    //    val predictions = model.transform(dfTransform)
    //    assert(predictions.count() > 0)
    //    val test1 = predictions.collect()
    //    val userRecs = model.recommendForAllUsers(3)
    //    val test2 = userRecs.collect()
    //    assert(userRecs.count() > 0)
  }

  test("exact rank-1 matrix") {
    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)
    testALS(training, test, maxIter = 1, rank = 1, regParam = 1e-5, targetRMSE = 0.001)
    testALS(training, test, maxIter = 1, rank = 2, regParam = 1e-5, targetRMSE = 0.001)
  }

  test("approximate rank-1 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 0.01, targetRMSE = 0.02)
    testALS(training, test, maxIter = 2, rank = 2, regParam = 0.01, targetRMSE = 0.02)
  }

  test("approximate rank-2 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03)
    testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03)
  }

  test("different block settings") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
      testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03,
        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks)
    }
  }

  test("more blocks than ratings") {
    val (training, test) =
      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 1e-4, targetRMSE = 0.002,
      numItemBlocks = 5, numUserBlocks = 5)
  }

  override def testObjects(): Seq[TestObject[MsftRecommendation]] = {
    val df = session.createDataFrame(Seq((0, 0, 0),
      (0, 1, 4),
      (0, 2, 4),
      (0, 3, 4),
      (1, 1, 4),
      (1, 2, 4),
      (1, 3, 4),
      (1, 0, 4),
      (2, 1, 5),
      (2, 3, 5),
      (2, 2, 4)
    )).toDF("customerID", "itemID", "rating")

    List(
      new TestObject(new MsftRecommendation()
        .setRank(1)
        .setRegParam(1)
        .setMaxIter(1)
        .setNumUserBlocks(200)
        .setNumItemBlocks(20)
        .setItemCol("itemID")
        .setUserCol("customerID")
        .setRatingCol("rating"), df))
  }

  override def reader: MLReadable[_] = MsftRecommendation

  override def modelReader: MLReadable[_] = MsftRecommendationModel

  def testALS(
               training: RDD[Rating[Int]],
               test: RDD[Rating[Int]],
               rank: Int,
               maxIter: Int,
               regParam: Double,
               implicitPrefs: Boolean = false,
               numUserBlocks: Int = 2,
               numItemBlocks: Int = 3,
               targetRMSE: Double = 0.05): Unit = {
    val spark = this.session
    import spark.implicits._
    val als = new MsftRecommendation()
      .setRank(rank)
      .setRegParam(regParam)
      .setImplicitPrefs(implicitPrefs)
      .setNumUserBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
      .setSeed(0)
    val alpha = als.getAlpha
    val model = als.fit(training.toDF())
    val predictions = model.transform(test.toDF()).select("rating", "prediction").rdd.map {
      case Row(rating: Float, prediction: Float) =>
        (rating.toDouble, prediction.toDouble)
    }
    val rmse =
      if (implicitPrefs) {
        // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
        // We limit the ratings and the predictions to interval [0, 1] and compute the weighted RMSE
        // with the confidence scores as weights.
        val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
          val confidence = 1.0 + alpha * math.abs(rating)
          val rating01 = math.max(math.min(rating, 1.0), 0.0)
          val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
          val err = prediction01 - rating01
          (confidence, confidence * err * err)
        }.reduce { case ((c0, e0), (c1, e1)) =>
          (c0 + c1, e0 + e1)
        }
        math.sqrt(weightedSumSq / totalWeight)
      } else {
        val mse = predictions.map { case (rating, prediction) =>
          val err = rating - prediction
          err * err
        }.mean()
        math.sqrt(mse)
      }
    assert(rmse < targetRMSE)
    ()
  }

  def genExplicitTestData(
                           numUsers: Int,
                           numItems: Int,
                           rank: Int,
                           noiseStd: Double = 0.0,
                           seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating[Int]]
    val test = ArrayBuffer.empty[Rating[Int]]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val x = random.nextDouble()
      if (x < totalFraction) {
        val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
        if (x < trainingFraction) {
          val noise = noiseStd * random.nextGaussian()
          training += Rating(userId, itemId, rating + noise.toFloat)
        } else {
          test += Rating(userId, itemId, rating)
        }
      }
    }
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  private def genFactors(
                          size: Int,
                          rank: Int,
                          random: Random,
                          a: Float = -1.0f,
                          b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    require(b > a)
    val ids = mutable.Set.empty[Int]
    while (ids.size < size) {
      ids += random.nextInt()
    }
    val width = b - a
    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
  }

}
