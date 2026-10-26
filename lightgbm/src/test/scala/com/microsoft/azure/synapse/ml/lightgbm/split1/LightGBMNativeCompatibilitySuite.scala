// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.usingSource
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMRegressionModel, LightGBMRegressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import spray.json.DefaultJsonProtocol.{DoubleJsonFormat, StringJsonFormat, vectorFormat}
import spray.json.{JsObject, enrichString}

import java.util.Base64
import scala.io.Source

class LightGBMNativeCompatibilitySuite extends TestBase {
  private val categories = 101
  private val selectedCategories = 30
  private val rowCount = categories * 100
  private val distributedWorkers = 2
  private val iterations = 10
  private val maxRootMeanSquaredError = 0.02

  private lazy val legacyFixture: JsObject =
    usingSource(Source.fromResource("native-3.3.510.json"))(_.mkString).get.parseJson.asJsObject

  private def data(partitions: Int): DataFrame = {
    val input = spark.range(0L, rowCount.toLong, 1L, partitions)
      .withColumn("category", (col("id") % categories).cast("double"))
      .withColumn("label", col("category").between(1, selectedCategories).cast("double"))
    new VectorAssembler().setInputCols(Array("category")).setOutputCol("features").transform(input)
  }

  private def regressor(mode: String, matrix: String, workers: Int): LightGBMRegressor =
    new LightGBMRegressor()
      .setDataTransferMode(mode)
      .setMatrixType(matrix)
      .setUseSingleDatasetMode(false)
      .setUseBarrierExecutionMode(false)
      .setNumTasks(workers)
      .setNumThreads(1)
      .setMaxStreamingOMPThreads(1)
      .setNumLeaves(2)
      .setNumIterations(iterations)
      .setLearningRate(0.3)
      .setCategoricalSlotIndexes(Array(0))
      .setMaxCatThreshold(32)
      .setMinDataPerGroup(1)
      .setCatSmooth(0.0)
      .setCatl2(0.0)
      .setSeed(731)
      .setTimeout(60)

  private def predictions(model: LightGBMRegressionModel, input: DataFrame): Array[Double] = {
    val scored = model.transform(input).orderBy("id").select("label", "prediction").collect()
    assert(scored.length == rowCount)
    val values = scored.map(_.getDouble(1))
    assert(values.forall(value => !value.isNaN && !value.isInfinity))
    val squaredError = scored.map(row => math.pow(row.getDouble(0) - row.getDouble(1), 2)).sum
    assert(math.sqrt(squaredError / rowCount) < maxRootMeanSquaredError)
    values
  }

  private def assertCategoricalSplit(model: LightGBMRegressionModel): Unit = {
    val firstTree = model.getNativeModel().split("Tree=1")(0)
    val thresholds = firstTree.split("\n").find(_.startsWith("cat_threshold="))
    assert(thresholds.isDefined, "The regression must exercise a categorical split")
    val selected = thresholds.get.stripPrefix("cat_threshold=").trim.split(" ")
      .map(value => java.lang.Long.bitCount(value.toLong)).sum
    assert(selected == selectedCategories, s"Expected $selectedCategories selected categories, got $selected")
  }

  private def checkRoundTrip(model: LightGBMRegressionModel,
                            input: DataFrame,
                            expected: Array[Double],
                            name: String): Unit = {
    val path = tmpDir.resolve(name).toString
    model.write.overwrite().save(path)
    val restored = LightGBMRegressionModel.load(path)
    try {
      assert(predictions(restored, input).sameElements(expected))
    } finally {
      restored.getModel.freeNativeMemory()
    }
  }

  Seq("dense", "sparse").foreach { matrix =>
    test(s"distributed $matrix categorical training survives a split larger than maxCatThreshold minus four") {
      val input = data(distributedWorkers).cache()
      try {
        assert(input.count() == rowCount)
        val estimator = regressor("bulk", matrix, distributedWorkers)
        val model = estimator.fit(input)
        try {
          assertCategoricalSplit(model)
          val measures = estimator.getPerformanceMeasures.get
          assert(measures.getTaskMeasures.count(_.isActiveTrainingTask) == distributedWorkers)
          val expected = predictions(model, input)
          checkRoundTrip(model, input, expected, s"bulk-$matrix")
          val repeated = estimator.fit(input)
          try {
            assert(predictions(repeated, input).sameElements(expected))
          } finally {
            repeated.getModel.freeNativeMemory()
          }
        } finally {
          model.getModel.freeNativeMemory()
        }
      } finally {
        input.unpersist()
      }
    }

    test(s"$matrix streaming retains reference datasets across repeated fits") {
      val input = data(1).cache()
      try {
        assert(input.count() == rowCount)
        val estimator = regressor("streaming", matrix, 1)
        val model = estimator.fit(input)
        try {
          val reference = estimator.getReferenceDataset.toVector
          assert(reference.nonEmpty)
          val expected = predictions(model, input)
          checkRoundTrip(model, input, expected, s"streaming-$matrix")
          val copied = estimator.copy(ParamMap.empty)
          assert(copied.getReferenceDataset.sameElements(reference))
          val repeated = copied.fit(input)
          try {
            assert(copied.getReferenceDataset.sameElements(reference))
            assert(predictions(repeated, input).sameElements(expected))
          } finally {
            repeated.getModel.freeNativeMemory()
          }
        } finally {
          model.getModel.freeNativeMemory()
        }
      } finally {
        input.unpersist()
      }
    }
  }

  test("models and reference datasets from lightgbmlib 3.3.510 preserve predictions") {
    val input = data(1).cache()
    try {
      val cycle = legacyFixture.fields("predictions").convertTo[Vector[Double]]
      assert(cycle.length == categories)
      val expected = Array.tabulate(rowCount)(index => cycle(index % categories))
      val nativeModel = legacyFixture.fields("model").convertTo[String]
      val restored = LightGBMRegressionModel.loadNativeModelFromString(nativeModel)
      try {
        assert(predictions(restored, input).sameElements(expected))
        checkRoundTrip(restored, input, expected, "legacy-model")
      } finally {
        restored.getModel.freeNativeMemory()
      }
      val reference = Base64.getDecoder.decode(legacyFixture.fields("reference").convertTo[String])
      assert(reference.nonEmpty)
      Seq("dense", "sparse").foreach { matrix =>
        val estimator = regressor("streaming", matrix, 1).setReferenceDataset(reference)
        val model = estimator.fit(input)
        try {
          assert(estimator.getReferenceDataset.sameElements(reference))
          assert(predictions(model, input).zip(expected).forall { case (actual, prior) =>
            math.abs(actual - prior) < 1e-12
          })
        } finally {
          model.getModel.freeNativeMemory()
        }
      }
    } finally {
      input.unpersist()
    }
  }

  test("distributed categorical classifier produces valid probabilities") {
    val input = data(distributedWorkers).cache()
    try {
      assert(input.count() == rowCount)
      val estimator = new LightGBMClassifier()
        .setDataTransferMode("bulk")
        .setUseSingleDatasetMode(false)
        .setNumTasks(distributedWorkers)
        .setNumThreads(1)
        .setNumLeaves(2)
        .setNumIterations(iterations)
        .setLearningRate(0.3)
        .setCategoricalSlotIndexes(Array(0))
        .setMaxCatThreshold(32)
        .setMinDataPerGroup(1)
        .setCatSmooth(0.0)
        .setCatl2(0.0)
        .setTimeout(60)
      val model = estimator.fit(input)
      try {
        val scored = model.transform(input).select("label", "prediction", "probability").collect()
        assert(scored.length == rowCount)
        assert(scored.forall(row => row.getDouble(0) == row.getDouble(1)))
        scored.foreach { row =>
          val probabilities = row.getAs[org.apache.spark.ml.linalg.Vector](2).toArray
          assert(probabilities.length == 2)
          assert(probabilities.forall(value => !value.isNaN && value >= 0 && value <= 1))
          assert(math.abs(probabilities.sum - 1.0) < 1e-10)
        }
        assert(estimator.getPerformanceMeasures.get.getTaskMeasures.count(_.isActiveTrainingTask) ==
          distributedWorkers)
      } finally {
        model.getModel.freeNativeMemory()
      }
    } finally {
      input.unpersist()
    }
  }
}
