// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.build.BuildInfo
import com.microsoft.azure.synapse.ml.core.env.FileUtilities
import com.microsoft.azure.synapse.ml.core.test.benchmarks.{Benchmarks}
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => F, types => T}

class VerifyVowpalWabbitGeneric extends Benchmarks with EstimatorFuzzing[VowpalWabbitGeneric] {
  val numPartitions = 2
  import spark.implicits._

  test("Verify VowpalWabbitGeneric from string") {

    val vw = new VowpalWabbitGeneric()
      .setNumPasses(2)

    val dataset = Seq("1 |a b c", "0 |d e f").toDF("value")

    val classifier = vw.fit(dataset)

    val predictionDF = classifier.transform(dataset)

    val labelOneCnt = predictionDF.where($"prediction" > 0.5).count()
    assert(labelOneCnt == 1)
  }

  test("Verify VowpalWabbitGeneric using csoaa") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Cost-Sensitive-One-Against-All-(csoaa)-multi-class-example
    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--csoaa 4")

    val dataset = Seq(
      "1:0 2:1 3:1 4:1 | a b c",
      "1:1 2:1 3:0 4:1 | b c d"
    ).toDF("value")

    val classifier = vw.fit(dataset)

    val actual = classifier.transform(dataset).select("prediction")

    val expected = Seq(1, 3).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa") {

    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--oaa 4")
      .setNumPasses(4)

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d",
      "1 | a c e",
      "4 | b d f",
      "2 | d e f"
    ).toDF("value")

    val classifier = vw.fit(dataset)

    val actual = classifier.transform(dataset).select("prediction")

    val expected = Seq(1, 3, 1, 4, 2).toDF("prediction")
    verifyResult(expected, actual)
  }

  test("Verify VowpalWabbitGeneric using oaa w/ probs") {
    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Multiclass-classification
    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--oaa 4")
      .setNumPasses(4)

    val dataset = Seq(
      "1 | a b c",
      "3 | b c d",
      "1 | a c e",
      "4 | b d f",
      "2 | d e f"
    ).toDF("value")

    val classifier = vw.fit(dataset)

    val pred = classifier
      .setTestArgs("--probabilities")
      .transform(dataset)
      .select($"value", F.posexplode($"predictions"))
      .withColumn("maxProb", F.max("col").over(Window.partitionBy("value")))

    assert(pred.where(F.expr("col < 0 and col > 1")).count() == 0, "predictions must be between 0 and 1")

    val predMatched = pred
      .where($"col" === $"maxProb")
      .withColumn("label", F.substring($"value", 0, 1).cast(T.IntegerType))
      .where($"label" === $"pos" + 1)

    assert(predMatched.count() == 5, "Highest prob prediction must match label")
  }

  test("Verify VowpalWabbitGeneric using CATS") {
    // https://github.com/VowpalWabbit/vowpal_wabbit/wiki/CATS,-CATS-pdf-for-Continuous-Actions
    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--cats_pdf 3 --bandwidth 5000 --min_value 0 --max_value 20000")
      .setUseBarrierExecutionMode(false)

    val dataset = Seq(
      "ca 185.121:0.657567:6.20426e-05 | a b",
      "ca 772.592:0.458316:6.20426e-05 | b c",
      "ca 15140.6:0.31791:6.20426e-05 | d")
      .toDF("value")
      .coalesce(1)

    val classifier = vw.fit(dataset)

    val actual = classifier.transform(dataset.limit(1))
      .select($"value", F.posexplode($"segments"))
      .select(F.expr("col.*"))

    val expected = Seq(
      (0.0, 8333.333, 1.165E-4),
      (8333.333, 20000.0, 2.5E-6))
      .toDF("left", "right", "pdfValue")

    verifyResult(actual, expected)
  }

  test ("Verify VowpalWabbitGeneric using dsjson and cb_adf_explore") {
    val df = loadDSJSON

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--cb_explore_adf --dsjson")

    // fit the model
    val model = vw.fit(df)

    val predictions = model
      .setTestArgs("--dsjson")
      .transform(df)

    val actual = predictions
      .select("predictions")
      .limit(1)
      .select(F.posexplode($"predictions"))
      .select($"pos", $"col.action".alias("action"), $"col.probability".alias("probability"))

    import spark.implicits._

    val expected = Seq(
        (0, 0, 0.9625),
        (1, 2, 0.0125),
        (2, 3, 0.0125),
        (2, 1, 0.0125)
    ).toDF("pos", "action", "probability")

    verifyResult(expected, actual)
  }

  test ("Verify VowpalWabbitGeneric using dsjson and cb_adf") {
    val df = loadDSJSON

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--cb_adf --dsjson")

    // fit the model
    val model = vw.fit(df)

    val predictions = model
      .setTestArgs("--dsjson")
      .transform(df)

    val actual = predictions
      .select("predictions")
      .limit(1)
      .select(F.posexplode($"predictions"))
      .select(
        $"pos",
        $"col.action".alias("action"),
        F.round($"col.score", 2).alias("score"))

    import spark.implicits._

    val expected = Seq(
      (0, 0, -0.61),
      (1, 1, -0.44),
      (2, 2, -0.39),
      (2, 3, -0.32)
    ).toDF("pos", "action", "score")

    verifyResult(expected, actual)
  }

  test ("Verify VowpalWabbitGeneric using Spark coordinated learn") {
    val extractSchema = T.StructType(Seq(
      T.StructField("_label_cost", T.FloatType, false),
      T.StructField("_label_probability", T.FloatType, false),
      T.StructField("_label_Action", T.IntegerType, false),
      T.StructField("_labelIndex", T.IntegerType, false),
      T.StructField("Timestamp", T.StringType, false),
      T.StructField("EventId", T.StringType, false)
    ))

    val df = loadDSJSON
      .repartition(2)
      .withColumn("splitId", F.monotonically_increasing_id().mod(F.lit(2)))
      .withColumn("json", F.from_json(F.col("value"), extractSchema))
      .withColumn("EventId", $"json.EventId")

    val vw = new VowpalWabbitGeneric()
      .setPassThroughArgs("--cb_adf --dsjson")
      .setSplitCol("splitId")
      .setPredictionIdCol("EventId")

    // let's have the cake and eat it (model + 1-step ahead predictions)
    val model = vw.fit(df)

    val actual = model.getOneStepAheadPredictions
      .where($"EventId".startsWith("2c53de8bf44749789"))
      .select(F.posexplode($"predictions"))
      .select(
        $"pos",
        $"col.action".alias("action"),
        F.round($"col.score", 2).alias("score"))

    import spark.implicits._

    val expected = Seq(
      (0, 0, -0.31),
      (1, 1, -0.71),
      (2, 3, -0.32),
      (2, 2,  0.03)
    ).toDF("pos", "action", "score")

    verifyResult(expected, actual)
  }

  private def loadDSJSON = {
    val fileLocation = FileUtilities.join(BuildInfo.datasetDir,
      "VowpalWabbit", "Train", "dsjson_cb_part1.json").toString
    spark.read.text(fileLocation)
  }

  override def reader: MLReadable[_] = VowpalWabbitGeneric

  override def modelReader: MLReadable[_] = VowpalWabbitGenericModel

  override def testObjects(): Seq[TestObject[VowpalWabbitGeneric]] = {
    val dataset = Seq("1 |a b c", "0 |d e f").toDF("value")
    Seq(new TestObject(
      new VowpalWabbitGeneric(),
      dataset))
  }
}
