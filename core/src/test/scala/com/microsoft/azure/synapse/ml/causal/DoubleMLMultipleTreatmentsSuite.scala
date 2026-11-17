// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.schema.CategoricalUtilities
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.attribute.{BinaryAttribute, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.classification.{LogisticRegression, MultilayerPerceptronClassifier}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, LinearRegression, LinearRegressionModel, Regressor}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import java.nio.file.Files
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/** Full plural contracts; deliberately not Flaky and never a replacement for the legacy suite. */
class DoubleMLMultipleTreatmentsSuite extends TestBase {
  private val missing = "Plural treatment statistics are not available on this model"
  private val mismatch = "Plural treatment statistics do not match fitted treatment columns and width"

  private def names(k: Int): Array[String] = (0 until k).map(i => s"t$i").toArray

  private def close(actual: Array[Double], expected: Array[Double], tolerance: Double = 1e-5): Unit = {
    assert(actual.length == expected.length)
    actual.zip(expected).foreach { case (a, e) =>
      assert(java.lang.Double.isFinite(a))
      assert(math.abs(a - e) <= tolerance * math.max(1.0, math.abs(e)), s"$a != $e")
    }
  }

  private def frame(k: Int, n: Int = 256): DataFrame = {
    val fields = Array(StructField("rowId", LongType, false), StructField("x", DoubleType, false),
      StructField("group", StringType, false)) ++ names(k).map(StructField(_, DoubleType, false)) ++
      Array(StructField("y", DoubleType, false))
    val rows = (0 until n).map { i =>
      val x = math.sin(i * 0.113)
      val group = if (i % 2 == 0) "east" else "west"
      val treatments = (0 until k).map(j => math.sin((i + 1) * (j + 1) * 0.37) +
        math.cos(i * (j + 2) * 0.19) + 0.15 * x).toArray
      val outcome = treatments.zipWithIndex.map { case (t, j) => t * (j + 1.0) }.sum +
        0.4 * x + (if (group == "east") 0.7 else -0.7)
      Row.fromSeq(Seq[Any](i.toLong, x, group) ++ treatments.toSeq ++ Seq(outcome))
    }
    spark.createDataFrame(rows.asJava, StructType(fields))
  }

  private def estimator(k: Int): DoubleMLEstimator = new DoubleMLEstimator()
    .setTreatmentCols(names(k))
    .setOutcomeCol("y")
    .setTreatmentModel(new LinearRegression().setRegParam(0).setTol(1e-10))
    .setOutcomeModel(new LinearRegression().setRegParam(0).setTol(1e-10))

  private def statistics(draws: Array[Array[Double]]): DoubleMLModel = new DoubleMLModel()
    .setTreatmentCols(names(draws.head.length))
    .setFittedTreatmentCols(names(draws.head.length))
    .setRawTreatmentEffectCount(draws.head.length)
    .setRawTreatmentEffectsByIteration(draws.flatten)

  // Independent Gaussian elimination, including an intercept. No production solver or Spark fit is used.
  private def leastSquares(x: Array[Array[Double]], y: Array[Double]): Array[Double] = {
    val design = x.map(row => Array(1.0) ++ row)
    val width = design.head.length
    val augmented = Array.tabulate(width, width + 1) { (i, j) =>
      if (j == width) design.indices.map(r => design(r)(i) * y(r)).sum
      else design.indices.map(r => design(r)(i) * design(r)(j)).sum
    }
    for (pivot <- 0 until width) {
      val best = (pivot until width).maxBy(i => math.abs(augmented(i)(pivot)))
      val saved = augmented(pivot)
      augmented(pivot) = augmented(best)
      augmented(best) = saved
      val divisor = augmented(pivot)(pivot)
      assert(math.abs(divisor) > 1e-14)
      for (j <- pivot to width) augmented(pivot)(j) /= divisor
      for (i <- 0 until width if i != pivot) {
        val factor = augmented(i)(pivot)
        for (j <- pivot to width) augmented(i)(j) -= factor * augmented(pivot)(j)
      }
    }
    augmented.drop(1).map(_(width))
  }

  private def residualFrame(x: Array[Array[Double]], y: Array[Double],
                            original: Option[Array[Array[Double]]] = None): DataFrame = {
    val k = x.head.length
    val fields = names(k).map(StructField(_, DoubleType, false)) ++
      (0 until k).map(j => StructField(s"r$j", DoubleType, false)) ++
      Array(StructField("ry", DoubleType, false))
    val rows = x.indices.map(i => Row.fromSeq(
      original.getOrElse(x)(i).toSeq ++ x(i).toSeq ++ Seq(y(i))))
    spark.createDataFrame(rows.asJava, StructType(fields))
  }

  private def finalCoefficients(x: Array[Array[Double]], y: Array[Double],
                                original: Option[Array[Array[Double]]] = None): Array[Double] = {
    DoubleMLMultipleTreatmentTraining.finalStage(residualFrame(x, y, original), names(x.head.length),
      (0 until x.head.length).map(j => s"r$j").toArray, "ry")
  }

  private def residuals(k: Int): Array[Array[Double]] = (0 until 160).map { i =>
    (0 until k).map(j => math.sin((i + 1) * (j + 1) * 0.37) + math.cos(i * (j + 2) * 0.19)).toArray
  }.toArray

  private def normalLowerTail(z: Double): Double = {
    // Composite Simpson integration is independent of the production NormalDistribution implementation.
    val a = -10.0
    val b = -math.abs(z)
    if (b <= a) 0.0 else {
      val steps = 20000
      val h = (b - a) / steps
      def density(v: Double): Double = math.exp(-v * v / 2) / math.sqrt(2 * math.Pi)
      val middle = (1 until steps).map(i => density(a + i * h) * (if (i % 2 == 0) 2 else 4)).sum
      h * (density(a) + density(b) + middle) / 3
    }
  }

  for (k <- Seq(2, 3, 5)) {
    test(s"K=$k public fit excludes all treatments and supports a string confounder") {
      val model = estimator(k).fit(frame(k))
      close(model.getAvgTreatmentEffects, (1 to k).map(_.toDouble).toArray, 0.03)
      assert(model.getFittedTreatmentCols.sameElements(names(k)))
      assert(model.getRawTreatmentEffectCount == k)
      assert(model.getRawTreatmentEffectRows.length == 1)
    }

    test(s"K=$k joint final stage matches independent least squares with an intercept") {
      val x = residuals(k)
      val y = x.indices.map(i => 2.3 + x(i).zipWithIndex.map {
        case (v, j) => v * (j + 1.0)
      }.sum + 0.03 * math.sin(i * 0.73)).toArray
      close(finalCoefficients(x, y), leastSquares(x, y))
      val permutation = (0 until k).reverse.toArray
      val permuted = x.map(row => permutation.map(row(_)))
      close(finalCoefficients(permuted, y), permutation.map(leastSquares(x, y)(_)))
    }
  }

  test("Public treatment permutation permutes fitted names and coefficients") {
    val order = Array("t2", "t0", "t1")
    val model = estimator(3).setTreatmentCols(order).fit(frame(3))
    assert(model.getFittedTreatmentCols.sameElements(order))
    close(model.getAvgTreatmentEffects, Array(3.0, 1.0, 2.0), 0.03)
  }

  test("Literal dotted and backtick names retain the fitted user order") {
    val data = frame(2).withColumnRenamed("t0", "t.one").withColumnRenamed("t1", "t`two")
      .withColumnRenamed("x", "x.dot").withColumnRenamed("group", "group`name")
    val model = estimator(2).setTreatmentCols(Array("t.one", "t`two")).fit(data)
    assert(model.getFittedTreatmentCols.sameElements(Array("t.one", "t`two")))
    close(model.getAvgTreatmentEffects, Array(1.0, 2.0), 0.03)
  }

  test("User prediction probability and rawPrediction confounders are retained") {
    val data = frame(2).withColumnRenamed("x", "prediction").withColumnRenamed("group", "probability")
      .withColumn("rawPrediction", col("t0") + lit(0.1) * org.apache.spark.sql.functions.sin(col("rowId")))
      .withColumn("y", col("y") + lit(0.8) * col("rawPrediction"))
    close(estimator(2).fit(data).getAvgTreatmentEffects, Array(1.0, 2.0), 0.05)
  }

  test("Continuous treatments retain the supported binary outcome classifier route") {
    for (kind <- Seq("integer", "boolean")) {
      val data = frame(2).withColumn("y", (col("y") > 0).cast(kind))
        .withColumnRenamed("y", "y.`binary")
      val model = estimator(2).setOutcomeCol("y.`binary")
        .setOutcomeModel(new LogisticRegression()).fit(data)
      assert(model.getFittedTreatmentCols.sameElements(names(2)))
      assert(model.getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
    }
  }

  test("Confidence retains thirty plural draws without shrinking the bootstrap") {
    val data = frame(2).withColumn("y", col("y") +
      lit(0.02) * org.apache.spark.sql.functions.sin(col("rowId") * 0.73))
    val model = estimator(2).setMaxIter(30).fit(data)
    assert(model.getRawTreatmentEffectRows.length == 30)
    assert(model.getRawTreatmentEffectsByIteration.length == 60)
    assert(model.getConfidenceIntervals.length == 2)
    model.getRawTreatmentEffectRows.transpose.zip(model.getPValues).foreach { case (values, p) =>
      val mean = values.sum / values.length
      val sd = math.sqrt(values.map(v => math.pow(v - mean, 2)).sum / (values.length - 1))
      if (sd == 0) assert(p.isNaN) else assert(p >= 0 && p <= 1)
    }
  }

  test("Original row identities remain disjoint after within-fold bootstrap") {
    val data = frame(2)
    for (bootstrap <- Seq(false, true); draw <- 1 to 6) {
      val folds = DoubleMLMultipleTreatmentTraining.splitFolds(data, Array(0.5, 0.5), bootstrap)
      assert(folds.length == 2)
      val ids = folds.map(_.select("rowId").collect().map(_.getLong(0)).toSet)
      assert(ids.forall(_.nonEmpty), s"empty fixture fold on draw $draw")
      assert(ids(0).intersect(ids(1)).isEmpty)
      assert(ids.flatten.forall(i => i >= 0 && i < 256))
      if (!bootstrap) assert(ids(0).union(ids(1)).size == 256)
    }
  }

  test("Failed draws retain the smallest iteration cause without dropping siblings") {
    val pool = Executors.newFixedThreadPool(3)
    val context = ExecutionContext.fromExecutorService(pool)
    try {
      val completed = new java.util.concurrent.atomic.AtomicInteger(0)
      val error = intercept[IllegalArgumentException] {
        DoubleMLMultipleTreatmentTraining.runIterations(3, context) { iteration =>
          completed.incrementAndGet()
          if (iteration == 1) {
            Thread.sleep(30)
            throw new IllegalStateException("first marker")
          }
          if (iteration == 2) throw new IllegalArgumentException("second marker")
          Array(3.0, 4.0)
        }
      }
      assert(error.getMessage == "Multiple-treatment draw 1 failed")
      assert(error.getCause.getMessage == "first marker")
      assert(completed.get() == 3)
    } finally {
      context.shutdownNow()
    }
  }

  test("Empty and singleton original datasets fail the entire plural fit") {
    for (count <- Seq(0, 1)) {
      val error = intercept[IllegalArgumentException](estimator(2).fit(frame(2, count)))
      assert(error.getMessage == "Multiple-treatment draw 1 failed")
      assert(error.getCause.getMessage == "requirement failed: Draw 1 has an empty or singleton cross-fit fold")
    }
  }

  test("Interrupted and fatal draw failures escape without becoming missing bootstrap rows") {
    val pool = Executors.newFixedThreadPool(2)
    val context = ExecutionContext.fromExecutorService(pool)
    try {
      val interrupted = new InterruptedException("interrupt control")
      val first = intercept[InterruptedException] {
        DoubleMLMultipleTreatmentTraining.runIterations(2, context) { i =>
          if (i == 1) throw interrupted else Array(1.0, 2.0)
        }
      }
      assert(first eq interrupted)
      val fatal = new LinkageError("fatal control")
      val second = intercept[LinkageError] {
        DoubleMLMultipleTreatmentTraining.runIterations(2, context) { i =>
          if (i == 1) throw fatal else Array(1.0, 2.0)
        }
      }
      assert(second eq fatal)
    } finally {
      context.shutdownNow()
    }
  }

  test("Both neither and ParamMap routes are checked at fit time") {
    val data = frame(2)
    val both = estimator(2).setTreatmentCol("t0")
    assert(intercept[IllegalArgumentException](both.fit(data)).getMessage ==
      "requirement failed: Set exactly one of treatmentCol or treatmentCols")
    val neither = new DoubleMLEstimator().setOutcomeCol("y")
    assert(intercept[IllegalArgumentException](neither.fit(data)).getMessage ==
      "requirement failed: Set exactly one of treatmentCol or treatmentCols")
    val fromMap = new DoubleMLEstimator().setOutcomeCol("y")
      .setTreatmentModel(new LinearRegression()).setOutcomeModel(new LinearRegression())
    val model = fromMap.fit(data, ParamMap(fromMap.treatmentCols -> names(2)))
    assert(model.getFittedTreatmentCols.sameElements(names(2)))
    assert(intercept[IllegalArgumentException] {
      fromMap.fit(data, ParamMap(fromMap.treatmentCols -> names(2), fromMap.treatmentCol -> "t0"))
    }.getMessage == "requirement failed: Set exactly one of treatmentCol or treatmentCols")
  }

  test("Plural input checks reject each distinct invalid branch precisely") {
    val data = frame(2)
    def rejected(e: DoubleMLEstimator, expected: String, df: DataFrame = data): Unit = {
      val error = intercept[IllegalArgumentException](DoubleMLMultipleTreatmentTraining.validate(e, df))
      assert(error.getMessage == "requirement failed: " + expected)
    }
    rejected(estimator(2).setTreatmentCols(Array("t0")),
      "treatmentCols must contain at least two distinct columns")
    rejected(estimator(2).setTreatmentCols(Array("t0", "t0")),
      "treatmentCols must contain at least two distinct columns")
    rejected(estimator(2).setTreatmentCols(Array("t0", "y")),
      "Treatment columns must not include the outcome")
    rejected(estimator(2).setWeightCol("x"), "weightCol is not supported for multiple treatments")
    rejected(new DoubleMLEstimator().setTreatmentCols(names(2)).setOutcomeCol("y"),
      "Multiple treatments require an explicitly supplied Regressor treatmentModel")
    rejected(estimator(2).setTreatmentModel(new LogisticRegression()),
      "Multiple treatments require an explicitly supplied Regressor treatmentModel")
    rejected(estimator(2).setTreatmentCols(Array("t0", "absent")), "Treatment column 'absent' does not exist")
    val wrongType = "Treatment column 't1' must be DoubleType, LongType or IntegerType"
    rejected(estimator(2), wrongType, data.withColumn("t1", col("t1") > 0))
    rejected(estimator(2), wrongType, data.withColumn("t1", lit("text")))
    rejected(estimator(2).setSampleSplitRatio(Array(0.0, 1.0)),
      "Plural sampleSplitRatio must contain two positive finite values")
    rejected(estimator(2).setSampleSplitRatio(Array(Double.PositiveInfinity, 1.0)),
      "Plural sampleSplitRatio must contain two positive finite values")
    DoubleMLMultipleTreatmentTraining.validate(estimator(2), data.withColumn("t1", col("t1").cast("long")))
    DoubleMLMultipleTreatmentTraining.validate(estimator(2), data.withColumn("t1", col("t1").cast("integer")))
  }

  test("Duplicate and case-insensitively ambiguous schemas fail before name resolution") {
    val previous = spark.conf.get("spark.sql.caseSensitive", "false")
    try {
      spark.conf.set("spark.sql.caseSensitive", "false")
      val data = frame(2)
      val message = "requirement failed: Plural input contains duplicate or case-insensitively ambiguous column names"
      val exact = data.toDF("rowId", "x", "group", "t0", "t0", "y")
      assert(intercept[IllegalArgumentException](estimator(2).fit(exact)).getMessage == message)
      val ambiguous = data.withColumnRenamed("t1", "T0")
      assert(intercept[IllegalArgumentException](estimator(2).setTreatmentCols(Array("t0", "T0"))
        .fit(ambiguous)).getMessage == message)
    } finally {
      spark.conf.set("spark.sql.caseSensitive", previous)
    }
  }

  test("Nuisance models are not mutated by a successful plural public fit") {
    val treatment = new LinearRegression().setLabelCol("original-label").setFeaturesCol("original-features")
    val outcome = new LinearRegression().setLabelCol("outcome-label").setFeaturesCol("outcome-features")
    val treatmentBefore = treatment.extractParamMap().toSeq.toSet
    val outcomeBefore = outcome.extractParamMap().toSeq.toSet
    estimator(2).setTreatmentModel(treatment).setOutcomeModel(outcome).fit(frame(2))
    assert(treatment.extractParamMap().toSeq.toSet == treatmentBefore)
    assert(outcome.extractParamMap().toSeq.toSet == outcomeBefore)
  }

  test("Final stage preserves scale and rejects variance rank and nonfinite input branches") {
    val original = residuals(2)
    val scaled = original.map(r => Array(r(0) * 1e-4, r(1) * 1e4))
    val y = original.map(r => 1.5 + 2 * r(0) - 3 * r(1))
    close(finalCoefficients(scaled, y), leastSquares(scaled, y), 1e-4)
    assert(intercept[IllegalArgumentException](finalCoefficients(original.take(2), y.take(2))).getMessage ==
      "requirement failed: Insufficient residual rows for joint regression")
    val constant = original.map(r => Array(0.0, r(1)))
    assert(intercept[IllegalArgumentException](finalCoefficients(constant, y)).getMessage ==
      "requirement failed: Treatment 't0' has nonpositive or nonfinite variance")
    val tiny = original.map(r => Array(r(0) * 1e-12, r(1)))
    assert(intercept[IllegalArgumentException](finalCoefficients(tiny, y, Some(original))).getMessage ==
      "requirement failed: Treatment 't0' has insufficient residual variance")
    val singular = original.map(r => Array(r(0), r(0)))
    assert(intercept[IllegalArgumentException](finalCoefficients(singular, y)).getMessage ==
      "requirement failed: Residual treatments are singular or ill-conditioned")
    val nonfinite = original.map(_.clone())
    Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      nonfinite(0)(0) = value
      assert(intercept[IllegalArgumentException](finalCoefficients(nonfinite, y, Some(original))).getMessage ==
        "requirement failed: Nonfinite treatment or residual values")
    }
    val justBelow = original.map(r => Array(r(0) * 9e-11, r(1)))
    assert(intercept[IllegalArgumentException](finalCoefficients(justBelow, y, Some(original))).getMessage ==
      "requirement failed: Treatment 't0' has insufficient residual variance")
    val justAbove = original.map(r => Array(r(0) * 1.1e-10, r(1)))
    close(finalCoefficients(justAbove, y, Some(original)), Array(2 / 1.1e-10, -3.0), 0.01)
  }

  test("Plural Wald p values follow independent SD formula and not Monte Carlo standard error") {
    val draws = Array(Array(0.2, 1.0), Array(0.5, 1.4), Array(0.8, 0.9), Array(0.1, 1.8))
    val model = statistics(draws)
    val expected = draws.transpose.map { values =>
      val mean = values.sum / values.length
      val sd = math.sqrt(values.map(v => math.pow(v - mean, 2)).sum / (values.length - 1))
      2 * normalLowerTail(mean / sd)
    }
    close(model.getPValues, expected, 1e-7)
    val doubled = statistics(draws ++ draws)
    val b = draws.length.toDouble
    val duplicatedExpected = draws.transpose.map { values =>
      val mean = values.sum / values.length
      val sd = math.sqrt(values.map(v => math.pow(v - mean, 2)).sum / (b - 1)) *
        math.sqrt(2 * (b - 1) / (2 * b - 1))
      2 * normalLowerTail(mean / sd)
    }
    close(doubled.getPValues, duplicatedExpected, 1e-7)
    // Any difference is precisely the B-1 sample-variance correction, not a sqrt(B) factor.
    close(model.getAvgTreatmentEffects, doubled.getAvgTreatmentEffects)
    assert(statistics(Array(Array(1.0, 2.0), Array(1.0, 3.0))).getPValues.head.isNaN)
    assert(!statistics(Array(Array(1.0, 2.0), Array(1.0, 3.0))).getPValues(1).isNaN)
    val overflow = statistics(Array(Array(1e308, 2.0), Array(-1e308, 3.0))).getPValues
    assert(overflow(0).isNaN && java.lang.Double.isFinite(overflow(1)))
    assert(intercept[IllegalArgumentException](statistics(Array(Array(1.0, 2.0))).getPValues).getMessage ==
      "requirement failed: Plural p-values require at least two bootstrap iterations")
  }

  test("Flat rows names and caller arrays have defensive copies") {
    val input = Array(1.0, 2.0, 3.0, 4.0)
    val fitted = Array("t0", "t1")
    val model = statistics(Array(Array(1.0, 2.0), Array(3.0, 4.0)))
      .setRawTreatmentEffectsByIteration(input).setFittedTreatmentCols(fitted)
    input(0) = 99
    fitted(0) = "changed"
    val flat = model.getRawTreatmentEffectsByIteration
    flat(0) = -99
    val rows = model.getRawTreatmentEffectRows
    rows(0)(0) = -88
    val namesCopy = model.getFittedTreatmentCols
    namesCopy(0) = "wrong"
    assert(model.getRawTreatmentEffectsByIteration.sameElements(Array(1.0, 2.0, 3.0, 4.0)))
    assert(model.getRawTreatmentEffectRows.flatten.sameElements(Array(1.0, 2.0, 3.0, 4.0)))
    assert(model.getFittedTreatmentCols.sameElements(Array("t0", "t1")))
  }

  test("Same width rename and corrupted width flat length and missing route fail explicitly") {
    def model(): DoubleMLModel = statistics(Array(Array(1.0, 2.0), Array(3.0, 4.0)))
    assert(intercept[IllegalStateException](new DoubleMLModel().getAvgTreatmentEffects).getMessage == missing)
    assert(intercept[IllegalStateException](model().setTreatmentCols(Array("renamed", "t1"))
      .getAvgTreatmentEffects).getMessage == mismatch)
    assert(intercept[IllegalStateException](model().setRawTreatmentEffectCount(3)
      .getRawTreatmentEffectRows).getMessage == mismatch)
    assert(intercept[IllegalStateException](model().setRawTreatmentEffectsByIteration(Array(1.0, 2.0, 3.0))
      .getRawTreatmentEffectRows).getMessage == mismatch)
    assert(intercept[IllegalStateException](model().setFittedTreatmentCols(Array("t0"))
      .getConfidenceIntervals).getMessage == mismatch)
    assert(intercept[IllegalStateException](model().setRawTreatmentEffectsByIteration(Array.empty[Double])
      .getAvgTreatmentEffects).getMessage == mismatch)
    assert(intercept[IllegalStateException](model().setRawTreatmentEffectsByIteration(Array(Double.NaN, 2.0))
      .getAvgTreatmentEffects).getMessage == mismatch)
  }

  test("Copy and model reload preserve causal statistics not just passthrough transforms") {
    val model = statistics(Array(Array(0.5, 1.2), Array(0.7, 1.6), Array(0.9, 1.8)))
    val path = Files.createTempDirectory(tmpDir, "plural-model-").resolve("model").toString
    model.write.overwrite().save(path)
    val loaded = DoubleMLModel.load(path)
    Seq(model.copy(new ParamMap()), loaded).foreach { other =>
      assert(other.getFittedTreatmentCols.sameElements(model.getFittedTreatmentCols))
      assert(other.getRawTreatmentEffectCount == model.getRawTreatmentEffectCount)
      close(other.getRawTreatmentEffectsByIteration, model.getRawTreatmentEffectsByIteration)
      close(other.getAvgTreatmentEffects, model.getAvgTreatmentEffects)
      close(other.getPValues, model.getPValues)
      other.getConfidenceIntervals.zip(model.getConfidenceIntervals).foreach {
        case (actual, expected) => close(actual, expected)
      }
    }
  }

  test("MLP nuisance layers remain caller-owned across parallel plural draws") {
    val layers = Array(1, 4, 2)
    val before = layers.clone()
    val outcome = new MultilayerPerceptronClassifier().setLayers(layers)
      .setMaxIter(5).setSeed(41L).setBlockSize(32)
    val data = frame(2).drop("rowId").withColumn("y", (col("y") > 0).cast("integer"))
    val model = estimator(2).setOutcomeModel(outcome).setMaxIter(3).setParallelism(2).fit(data)
    assert(layers.sameElements(before))
    assert(outcome.getLayers.sameElements(before))
    assert(outcome.getLayers eq layers)
    assert(model.getFittedTreatmentCols.sameElements(names(2)))
    assert(model.getRawTreatmentEffectRows.length == 3)
    assert(model.getAvgTreatmentEffects.length == 2)
    assert(model.getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
  }

  test("Every plural statistic rejects an ambiguous fitted route including copied and reloaded models") {
    val fitted = estimator(2).setMaxIter(2).fit(frame(2))
    def getters(model: DoubleMLModel): Seq[() => Any] = Seq(
      () => model.getRawTreatmentEffectCount, () => model.getRawTreatmentEffectsByIteration,
      () => model.getRawTreatmentEffectRows, () => model.getFittedTreatmentCols,
      () => model.getAvgTreatmentEffects, () => model.getPValues, () => model.getConfidenceIntervals)
    getters(fitted).foreach(_())
    val validCopy = fitted.copy(ParamMap.empty)
    val validPath = Files.createTempDirectory(tmpDir, "plural-valid-route-").resolve("model").toString
    fitted.write.overwrite().save(validPath)
    Seq(validCopy, DoubleMLModel.load(validPath)).foreach(model => getters(model).foreach(_()))
    fitted.setTreatmentCol("t0")
    val ambiguousPath = Files.createTempDirectory(tmpDir, "plural-ambiguous-route-").resolve("model").toString
    fitted.write.overwrite().save(ambiguousPath)
    Seq(fitted, fitted.copy(ParamMap.empty), DoubleMLModel.load(ambiguousPath)).foreach { model =>
      getters(model).foreach { getter =>
        assert(intercept[IllegalStateException](getter()).getMessage == missing)
      }
    }
  }

  test("Real indexed treatment columns are rejected by public fit without rejecting categorical confounders") {
    val data = frame(2)
    val first = new StringIndexer().setInputCol("group").setOutputCol("indexed0").fit(data).transform(data)
      .drop("group")
    val bothInput = first.withColumn("otherGroup", (col("rowId") % 3).cast("string"))
    val both = new StringIndexer().setInputCol("otherGroup").setOutputCol("indexed1")
      .fit(bothInput).transform(bothInput).drop("otherGroup")
    for (treatments <- Seq(Array("t0", "indexed0"), Array("indexed0", "indexed1"))) {
      val error = intercept[IllegalArgumentException](estimator(2).setTreatmentCols(treatments).fit(both))
      assert(error.getMessage ==
        "requirement failed: Treatment column 'indexed0' is categorical; multiple treatments must be continuous")
    }
    val confounder = new StringIndexer().setInputCol("group").setOutputCol("indexedGroup")
      .fit(data).transform(data).drop("group")
    assert(estimator(2).fit(confounder).getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
  }

  test("Treatment metadata API accepts numeric types and rejects nominal binary and SynapseML categories") {
    val data = frame(2)
    Seq("integer", "long", "double").foreach { kind =>
      DoubleMLMultipleTreatmentTraining.validate(estimator(2), data.withColumn("t0", col("t0").cast(kind)))
    }
    val binaryNumbers = data.withColumn("t0", (col("rowId") % 2).cast("integer"))
    DoubleMLMultipleTreatmentTraining.validate(estimator(2), binaryNumbers)
    val numeric = data.withColumn("t0", col("t0").as("t0", NumericAttribute.defaultAttr.toMetadata()))
    DoubleMLMultipleTreatmentTraining.validate(estimator(2), numeric)
    val nominal = data.withColumn("t0",
      col("t0").as("t0", NominalAttribute.defaultAttr.withNumValues(3).toMetadata()))
    val binary = data.withColumn("t0", col("t0").as("t0", BinaryAttribute.defaultAttr.toMetadata()))
    val synapse = CategoricalUtilities.setLevels(binaryNumbers, "t0", Array(0, 1))
    Seq(nominal, binary, synapse).foreach { categorical =>
      assert(intercept[IllegalArgumentException] {
        DoubleMLMultipleTreatmentTraining.validate(estimator(2), categorical)
      }.getMessage ==
        "requirement failed: Treatment column 't0' is categorical; multiple treatments must be continuous")
    }
  }

  test("Real GLR offset auxiliaries survive literal aliases in both nuisance roles without caller mutation") {
    for (role <- Seq("treatment", "outcome"); offset <- Seq("off.set", "off`set")) {
      val data = frame(2).withColumnRenamed("x", "prediction").withColumnRenamed("group", "probability")
        .withColumn("rawPrediction", lit(0.1) * org.apache.spark.sql.functions.sin(col("rowId")))
        .withColumn(offset, lit(0.2) * org.apache.spark.sql.functions.sin(col("rowId") * 0.23))
      val nuisance = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity")
        .setRegParam(0).setMaxIter(10).setOffsetCol(offset)
      val before = nuisance.extractParamMap().toSeq.toSet
      val e = estimator(2)
      if (role == "treatment") e.setTreatmentModel(nuisance) else e.setOutcomeModel(nuisance)
      val model = e.fit(data)
      assert(model.getFittedTreatmentCols.sameElements(names(2)))
      assert(model.getAvgTreatmentEffects.length == 2)
      assert(model.getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
      assert(nuisance.getOffsetCol == offset)
      assert(nuisance.extractParamMap().toSeq.toSet == before)
    }
  }

  test("Auxiliary resolution preserves case empty and unset semantics and rejects missing or target inputs") {
    val data = frame(2).withColumn("offset", lit(0.2) * org.apache.spark.sql.functions.sin(col("rowId") * 0.23))
    val previous = spark.conf.get("spark.sql.caseSensitive", "false")
    def outcome(offset: String): GeneralizedLinearRegression =
      new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity").setOffsetCol(offset)
    try {
      spark.conf.set("spark.sql.caseSensitive", "false")
      assert(estimator(2).setOutcomeModel(outcome("OFFSET")).fit(data)
        .getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
      spark.conf.set("spark.sql.caseSensitive", "true")
      assert(intercept[IllegalArgumentException] {
        estimator(2).setOutcomeModel(outcome("OFFSET")).fit(data)
      }.getMessage == "requirement failed: Auxiliary column 'OFFSET' from offsetCol does not exist")
      assert(intercept[IllegalArgumentException] {
        estimator(2).setOutcomeModel(outcome("absent")).fit(data)
      }.getMessage == "requirement failed: Auxiliary column 'absent' from offsetCol does not exist")
      for (column <- Seq("y", "t0"); role <- Seq("treatment", "outcome")) {
        val e = estimator(2)
        if (role == "treatment") e.setTreatmentModel(outcome(column)) else e.setOutcomeModel(outcome(column))
        assert(intercept[IllegalArgumentException](e.fit(data)).getMessage ==
          s"requirement failed: Auxiliary parameter 'offsetCol' must not reference " +
            s"outcome or treatment column '$column'")
      }
      for (nuisance <- Seq(outcome(""), new GeneralizedLinearRegression().setFamily("gaussian"))) {
        val before = nuisance.extractParamMap().toSeq.toSet
        assert(estimator(2).setOutcomeModel(nuisance).fit(data)
          .getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
        assert(nuisance.extractParamMap().toSeq.toSet == before)
      }
    } finally {
      spark.conf.set("spark.sql.caseSensitive", previous)
    }
  }

  test("Supported validation and initial-score inputs are remapped for a real delegated regression fit") {
    val data = frame(2).withColumn("validation", col("rowId") % 3 === 0)
      .withColumn("initialScore", org.apache.spark.sql.functions.sin(col("rowId") * 0.31))
    for (role <- Seq("treatment", "outcome")) {
      val nuisance = new TestableAuxiliaryInputRegressor().setValidationIndicatorCol("validation")
        .setInitScoreCol("initialScore").setTag("x")
      val before = nuisance.extractParamMap().toSeq.toSet
      val e = estimator(2)
      if (role == "treatment") e.setTreatmentModel(nuisance) else e.setOutcomeModel(nuisance)
      val model = e.fit(data)
      assert(model.getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
      assert(model.getFittedTreatmentCols.sameElements(names(2)))
      assert(nuisance.extractParamMap().toSeq.toSet == before)
    }
  }

  test("Nuisance weight columns are remapped without enabling weighted plural estimation") {
    val data = frame(2).withColumn("nuisanceWeight",
      lit(1.0) + lit(0.1) * org.apache.spark.sql.functions.sin(col("rowId") * 0.47))
    for (role <- Seq("treatment", "outcome")) {
      val nuisance = new GeneralizedLinearRegression().setFamily("gaussian").setWeightCol("nuisanceWeight")
      val before = nuisance.extractParamMap().toSeq.toSet
      val e = estimator(2)
      if (role == "treatment") e.setTreatmentModel(nuisance) else e.setOutcomeModel(nuisance)
      assert(e.fit(data).getAvgTreatmentEffects.forall(java.lang.Double.isFinite))
      assert(nuisance.getWeightCol == "nuisanceWeight")
      assert(nuisance.extractParamMap().toSeq.toSet == before)
    }
    assert(intercept[IllegalArgumentException](estimator(2).setWeightCol("nuisanceWeight").fit(data))
      .getMessage == "requirement failed: weightCol is not supported for multiple treatments")
  }
}

/** Input-remapping contract only: delegates to real Spark LR, not a native LightGBM proof. */
private[causal] class TestableAuxiliaryInputRegressor(override val uid: String)
  extends Regressor[Vector, TestableAuxiliaryInputRegressor, LinearRegressionModel] {
  def this() = this(Identifiable.randomUID("auxiliaryInputRegressor"))
  val validationIndicatorCol = new Param[String](this, "validationIndicatorCol", "validation input")
  val initScoreCol = new Param[String](this, "initScoreCol", "initial score input")
  val tag = new Param[String](this, "tag", "unrelated scalar string")
  def setValidationIndicatorCol(value: String): this.type = set(validationIndicatorCol, value)
  def setInitScoreCol(value: String): this.type = set(initScoreCol, value)
  def setTag(value: String): this.type = set(tag, value)
  override protected def train(dataset: Dataset[_]): LinearRegressionModel = {
    require(dataset.columns.contains($(validationIndicatorCol)), "validation input was not remapped")
    require(dataset.columns.contains($(initScoreCol)), "initial-score input was not remapped")
    require($(tag) == "x", "unrelated string parameter was rewritten")
    new LinearRegression().setRegParam(0).setTol(1e-10).setLabelCol(getLabelCol)
      .setFeaturesCol(getFeaturesCol).setPredictionCol(getPredictionCol).fit(dataset)
  }
  override def copy(extra: ParamMap): TestableAuxiliaryInputRegressor = defaultCopy(extra)
}
