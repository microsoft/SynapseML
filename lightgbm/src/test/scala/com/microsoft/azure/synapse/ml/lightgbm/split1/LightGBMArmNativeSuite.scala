// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.env.NativeLoader
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor
import org.apache.spark.ml.linalg.Vectors

class LightGBMArmNativeSuite extends TestBase {
  test("fit reports unsupported ARM natives before scheduling training") {
    import spark.implicits._
    val data = Seq((0.0, Vectors.dense(0.0)), (1.0, Vectors.dense(1.0))).toDF("label", "features")
    val originalArch = System.getProperty("os.arch")
    try {
      System.setProperty("os.arch", "aarch64")
      val error = intercept[UnsupportedOperationException] {
        new LightGBMRegressor().fit(data)
      }
      assert(error.getMessage.contains("LightGBM is not supported on ARM"))
      assert(error.getMessage.contains("x64 compute"))
    } finally {
      System.setProperty("os.arch", originalArch)
    }
    if (!NativeLoader.isArm64) {
      val model = new LightGBMRegressor().setNumIterations(1).setNumThreads(1).setNumTasks(1)
        .fit(data.repartition(1))
      assert(model.transform(data).select("prediction").collect().length == 2)
    }
  }
}
