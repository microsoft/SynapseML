// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressionModel, LightGBMRegressor, LightGBMUtils}
import com.microsoft.azure.synapse.ml.lightgbm.dataset.SampledData
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

class StreamingFeatureSizeSuite extends LightGBMTestUtils {
  private val rowCount = 64
  private val sampleCount = 16

  private def data(featureSize: Int, sparse: Boolean, validation: Boolean, invalidIndex: Int = 63): DataFrame = {
    import spark.implicits._
    (0 until rowCount).map { index =>
      val invalid = index == invalidIndex
      val size = if (invalid) featureSize else 2
      val values = Array.tabulate(size)(column => ((index + column) % 7).toDouble)
      val features: Vector = if (sparse) Vectors.dense(values).toSparse else Vectors.dense(values)
      (index, (index % 2).toDouble, invalid && validation, features)
    }.toDF("id", labelCol, validationCol, featuresCol).orderBy("id").coalesce(1)
  }

  private def estimator(matrixType: String, validation: Boolean): LightGBMRegressor = {
    val learner = new LightGBMRegressor()
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setDefaultListenPort(getAndIncrementPort())
      .setDataTransferMode("streaming")
      .setMatrixType(matrixType)
      .setNumTasks(1)
      .setNumThreads(1)
      .setMaxStreamingOMPThreads(1)
      .setMicroBatchSize(8)
      .setSamplingMode("fixed")
      .setBinSampleCount(sampleCount)
      .setMinDataInLeaf(1)
      .setMinDataPerBin(1)
      .setNumLeaves(4)
      .setNumIterations(1)
    if (validation) learner.setValidationIndicatorCol(validationCol) else learner
  }

  Seq("dense", "sparse").foreach { matrixType =>
    Seq(false, true).foreach { validation =>
      val source = if (validation) "validation" else "training"
      Seq(0, 1, 3).foreach { size =>
        val kind = if (size < 2) "undersized" else "oversized"
        test(s"streaming rejects $kind $matrixType $source vectors of size $size outside the reference sample") {
          val input = data(size, sparse = matrixType == "sparse", validation = validation).cache()
          try {
            assert(input.count() == rowCount)
            val error = intercept[Exception](estimator(matrixType, validation).fit(input))
            assert(error.getMessage.contains(s"Expected feature vector size 2 but found $size"))
          } finally {
            input.unpersist()
          }
        }
      }
    }

    test(s"streaming checks dimensions before converting vectors to $matrixType") {
      Seq(1, 3).foreach { size =>
        val input = data(size, sparse = matrixType == "dense", validation = false)
        val error = intercept[Exception](estimator(matrixType, validation = false).fit(input))
        assert(error.getMessage.contains(s"Expected feature vector size 2 but found $size"))
      }
    }

    test(s"streaming rejects $matrixType vectors within the reference sample and at micro-batch boundaries") {
      Seq(7, 8, 15, 16, 17).foreach { index =>
        val input = data(1, sparse = matrixType == "sparse", validation = false, invalidIndex = index)
        val error = intercept[Exception](estimator(matrixType, validation = false).fit(input))
        assert(error.getMessage.contains("Expected feature vector size 2 but found 1"))
      }
    }
  }

  test("sampled data rejects undersized dense and sparse vectors before changing buffers") {
    LightGBMUtils.initializeNativeLibrary()
    val sample = SampledData(3, 2)
    try {
      sample.pushRow(Array(1.0, 2.0), 0)
      def checkRejected(size: Int)(push: => Unit): Unit = {
        val error = intercept[IllegalArgumentException](push)
        assert(error.getMessage.contains(s"Expected feature vector size 2 but found $size"))
        assert(sample.rowCounts.getItem(0) == 1)
        assert(sample.rowCounts.getItem(1) == 1)
      }
      Seq(0, 1, 3).foreach { size =>
        checkRejected(size)(sample.pushRow(Array.fill(size)(1.0), 1))
        checkRejected(size)(sample.pushRow(Vectors.dense(Array.fill(size)(1.0)).toDense, 1))
        checkRejected(size)(sample.pushRow(Vectors.sparse(size, Array.empty[Int], Array.empty[Double]).toSparse, 1))
      }
      sample.pushRow(Vectors.sparse(2, Array.empty[Int], Array.empty[Double]).toSparse, 1)
      sample.pushRow(Vectors.dense(3.0, 4.0).toDense, 2)
      assert(sample.rowCounts.getItem(0) == 2)
      assert(sample.rowCounts.getItem(1) == 2)
    } finally {
      sample.delete()
    }
  }

  test("valid dense and sparse streaming inputs preserve predictions through copy and save load") {
    Seq("dense", "sparse").foreach { matrixType =>
      val input = data(2, sparse = matrixType == "sparse", validation = false)
      val learner = estimator(matrixType, validation = false)
      val model = learner.copy(ParamMap.empty).fit(input)
      val expected = model.transform(input).orderBy("id").select("prediction").collect().map(_.getDouble(0))
      assert(expected.length == rowCount)
      assert(expected.forall(value => !value.isNaN && !value.isInfinity))
      val path = tmpDir.resolve(s"$matrixType-model").toString
      model.write.overwrite().save(path)
      val loaded = LightGBMRegressionModel.load(path)
      assert(loaded.uid == model.uid)
      assert(loaded.transformSchema(input.schema) == model.transformSchema(input.schema))
      Seq(loaded, model.copy(ParamMap.empty)).foreach { copy =>
        val actual = copy.transform(input).orderBy("id").select("prediction").collect().map(_.getDouble(0))
        assert(actual.toSeq == expected.toSeq)
      }
    }
  }
}
