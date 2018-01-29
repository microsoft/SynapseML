//import java.util.Random
//
//import com.microsoft.ml.spark.{MsftRecommendation, TestBase}
//import org.apache.spark.ml.recommendation.ALS.Rating
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.scalatest.FunSuite
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//abstract class RecommendationSpec extends TestBase{
//
//  val recommender = new MsftRecommendation()
//
//  test("exact rank-1 matrix") {
//    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)
//    testRecommender(training, test, maxIter = 1, rank = 1, regParam = 1e-5, targetRMSE = 0.001, recommender =
// recommender)
//    testRecommender(training, test, maxIter = 1, rank = 2, regParam = 1e-5, targetRMSE = 0.001, recommender =
// recommender)
//  }
//
//  test("approximate rank-1 matrix") {
//    val (training, test) =
//      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
//    testRecommender(training, test, maxIter = 2, rank = 1, regParam = 0.01, targetRMSE = 0.02, recommender =
// recommender)
//    testRecommender(training, test, maxIter = 2, rank = 2, regParam = 0.01, targetRMSE = 0.02, recommender =
// recommender)
//  }
//
//  test("approximate rank-2 matrix") {
//    val (training, test) =
//      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
//    testRecommender(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03, recommender =
// recommender)
//    testRecommender(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03, recommender =
// recommender)
//  }
//
//  test("different block settings") {
//    val (training, test) =
//      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
//    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
//      testRecommender(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03,
//        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks, recommender = recommender)
//    }
//  }
//
//  test("more blocks than ratings") {
//    val (training, test) =
//      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
//    testRecommender(training, test, maxIter = 2, rank = 1, regParam = 1e-4, targetRMSE = 0.002,
//      numItemBlocks = 5, numUserBlocks = 5, recommender = recommender )
//  }
//
//  def testRecommender(
//                       training: RDD[Rating[Int]],
//                       test: RDD[Rating[Int]],
//                       rank: Int,
//                       maxIter: Int,
//                       regParam: Double,
//                       implicitPrefs: Boolean = false,
//                       numUserBlocks: Int = 2,
//                       numItemBlocks: Int = 3,
//                       targetRMSE: Double = 0.05,
//                       recommender: MsftRecommendation): Unit = {
//    val spark = this.session
//    import spark.implicits._
//    val als = recommender
//      .setRank(rank)
//      .setRegParam(regParam)
//      .setImplicitPrefs(implicitPrefs)
//      .setNumUserBlocks(numUserBlocks)
//      .setNumItemBlocks(numItemBlocks)
//      .setSeed(0)
//    val alpha = als.getAlpha
//    val model = als.fit(training.toDF())
//    val predictions = model.transform(test.toDF()).select("rating", "prediction").rdd.map {
//      case Row(rating: Float, prediction: Float) =>
//        (rating.toDouble, prediction.toDouble)
//    }
//    val rmse =
//      if (implicitPrefs) {
//        // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
//        // We limit the ratings and the predictions to interval [0, 1] and compute the weighted RMSE
//        // with the confidence scores as weights.
//        val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
//          val confidence = 1.0 + alpha * math.abs(rating)
//          val rating01 = math.max(math.min(rating, 1.0), 0.0)
//          val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
//          val err = prediction01 - rating01
//          (confidence, confidence * err * err)
//        }.reduce { case ((c0, e0), (c1, e1)) =>
//          (c0 + c1, e0 + e1)
//        }
//        math.sqrt(weightedSumSq / totalWeight)
//      } else {
//        val mse = predictions.map { case (rating, prediction) =>
//          val err = rating - prediction
//          err * err
//        }.mean()
//        math.sqrt(mse)
//      }
//    assert(rmse < targetRMSE)
//    ()
//  }
//
//  def genExplicitTestData(
//                           numUsers: Int,
//                           numItems: Int,
//                           rank: Int,
//                           noiseStd: Double = 0.0,
//                           seed: Long = 11L): (RDD[Rating[Int]], RDD[Rating[Int]]) = {
//    val trainingFraction = 0.6
//    val testFraction = 0.3
//    val totalFraction = trainingFraction + testFraction
//    val random = new Random(seed)
//    val userFactors = genFactors(numUsers, rank, random)
//    val itemFactors = genFactors(numItems, rank, random)
//    val training = ArrayBuffer.empty[Rating[Int]]
//    val test = ArrayBuffer.empty[Rating[Int]]
//    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
//      val x = random.nextDouble()
//      if (x < totalFraction) {
//        val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
//        if (x < trainingFraction) {
//          val noise = noiseStd * random.nextGaussian()
//          training += Rating(userId, itemId, rating + noise.toFloat)
//        } else {
//          test += Rating(userId, itemId, rating)
//        }
//      }
//    }
//    (sc.parallelize(training, 2), sc.parallelize(test, 2))
//  }
//
//  private def genFactors(
//                          size: Int,
//                          rank: Int,
//                          random: Random,
//                          a: Float = -1.0f,
//                          b: Float = 1.0f): Seq[(Int, Array[Float])] = {
//    require(size > 0 && size < Int.MaxValue / 3)
//    require(b > a)
//    val ids = mutable.Set.empty[Int]
//    while (ids.size < size) {
//      ids += random.nextInt()
//    }
//    val width = b - a
//    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
//  }
//
//}
