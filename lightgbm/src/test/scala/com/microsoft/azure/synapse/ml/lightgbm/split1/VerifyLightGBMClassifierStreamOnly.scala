// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

// scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM module in streaming mode. */
class VerifyLightGBMClassifierStreamOnly extends LightGBMClassifierTestData {
  override def ignoreSerializationFuzzing: Boolean = true
  override def ignoreExperimentFuzzing: Boolean = true

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

  test("Verify LightGBMClassifier can use cached reference dataset") {
    val baseClassifier = baseModel
    assert(baseClassifier.getReferenceDataset.isEmpty)

    val model1 = baseClassifier.fit(pimaDF)

    // Assert the generated reference dataset was saved
    assert(baseClassifier.getReferenceDataset.nonEmpty)

    // Assert we use the same reference data and get same result
    val model2 = baseModel.fit(pimaDF)
    assert(model1.getModel.modelStr == model2.getModel.modelStr)
  }
}
