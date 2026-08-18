// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassifier, LightGBMConstants}
import org.apache.spark.ml.linalg.Vectors

// scalastyle:off magic.number
class StreamingDatasetLifecycleSuite extends LightGBMTestUtils {
  test("streaming estimator supports repeated fits with validation data") {
    import spark.implicits._

    val data = (0 until 128).map { index =>
      val label = (index % 2).toDouble
      val validation = index % 5 == 0
      val features = Vectors.dense(index % 7, (index * 3) % 11, label)
      (label, validation, features)
    }.toDF("label", "validation", "features").repartition(1).cache()

    try {
      val estimator = new LightGBMClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setValidationIndicatorCol("validation")
        .setDataTransferMode(LightGBMConstants.StreamingDataTransferMode)
        .setNumTasks(1)
        .setNumThreads(1)
        .setMaxStreamingOMPThreads(1)
        .setMicroBatchSize(16)
        .setNumLeaves(4)
        .setNumIterations(1)

      (0 until 2).foreach { _ =>
        estimator.setDefaultListenPort(getAndIncrementPort())
        assert(estimator.fit(data).transform(data).count() == 128)
      }
    } finally {
      data.unpersist()
    }
  }
}
