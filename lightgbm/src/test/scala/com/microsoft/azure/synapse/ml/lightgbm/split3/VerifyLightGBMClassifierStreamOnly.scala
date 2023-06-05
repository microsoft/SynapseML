// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split3

import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.split1._
import org.apache.spark.sql.functions._

// scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM module in streaming mode. */
class VerifyLightGBMClassifierStream extends LightGBMClassifierTestData {

  test("Verify LightGBMClassifier handles global sample mode correctly") {
    val df = loadBinary(breastCancerFile, "Label")
    val model = baseModel
      .setBoostingType("gbdt")
      .setSamplingMode("global")

    val fitModel = model.fit(df)
    fitModel.transform(df)
  }

  test("Verify LightGBMClassifier handles fixed sample mode correctly") {
    val df = loadBinary(breastCancerFile, "Label")
    val model = baseModel
      .setBoostingType("gbdt")
      .setSamplingMode("fixed")

    val fitModel = model.fit(df)
    fitModel.transform(df)
  }

  test("Verify LightGBMClassifier handles subset sample mode correctly") {
    boostingTypes.foreach { boostingType =>
      val df = loadBinary(breastCancerFile, "Label")
      val model = baseModel
        .setBoostingType("gbdt")
        .setSamplingMode("subset")

      val fitModel = model.fit(df)
      fitModel.transform(df)
    }
  }


}
