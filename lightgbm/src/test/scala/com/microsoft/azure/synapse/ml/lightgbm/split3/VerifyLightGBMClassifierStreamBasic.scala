// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split3

import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMClassificationModel, LightGBMConstants}
import com.microsoft.azure.synapse.ml.lightgbm.split1._
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

// scalastyle:off magic.number
/** Tests to validate the functionality of LightGBM module in streaming mode. */
class VerifyLightGBMClassifierStreamBasic extends LightGBMClassifierTestData {
  /* TODO Figure out why abalone has such poor score
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.MulticlassObjective, abaloneFile, executionMode)) {
    verifyLearnerOnMulticlassCsvFile(abaloneFile, "Rings", 2)
  } */
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.MulticlassObjective, breastTissueFile, executionMode)) {
    verifyLearnerOnMulticlassCsvFile(breastTissueFile, "Class", .07)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.MulticlassObjective, carEvaluationFile, executionMode)) {
    verifyLearnerOnMulticlassCsvFile(carEvaluationFile, "Col7", 2)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, pimaIndianFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(pimaIndianFile, "Diabetes mellitus", 1)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, banknoteFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(banknoteFile, "class", 1)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, taskFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(taskFile, "TaskFailed10", 1)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, breastCancerFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(breastCancerFile, "Label", 1)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, randomForestFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(randomForestFile, "#Malignant", 1)
  }
  test(verifyLearnerTitleTemplate.format(LightGBMConstants.BinaryObjective, transfusionFile, executionMode)) {
    verifyLearnerOnBinaryCsvFile(transfusionFile, "Donated", 1)
  }

  test("Verify LightGBMClassifier save booster to " + pimaIndianFile + executionModeSuffix) {
    verifySaveBooster(
      fileName = pimaIndianFile,
      labelColumnName = "Diabetes mellitus",
      outputFileName = "model.txt",
      colsToVerify = Array("Diabetes pedigree function", "Age (years)"))
  }

  test("Compare benchmark results file to generated file" + executionModeSuffix) {
    verifyBenchmarks()
  }

  test("Verify LightGBM Classifier can be run with TrainValidationSplit" + executionModeSuffix) {
    val model = baseModel.setUseBarrierExecutionMode(true)

    val paramGrid = new ParamGridBuilder()
      .addGrid(model.numLeaves, Array(5, 10))
      .addGrid(model.numIterations, Array(10, 20))
      .addGrid(model.lambdaL1, Array(0.1, 0.5))
      .addGrid(model.lambdaL2, Array(0.1, 0.5))
      .build()

    val fitModel = new TrainValidationSplit()
      .setEstimator(model)
      .setEvaluator(binaryEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)
      .setParallelism(2)
      .fit(pimaDF)

    fitModel.transform(pimaDF)
    assert(fitModel != null)

    // Validate lambda parameters set on model
    val modelStr = fitModel.bestModel.asInstanceOf[LightGBMClassificationModel].getModel.modelStr.get
    assert(modelStr.contains("[lambda_l1: 0.1]") || modelStr.contains("[lambda_l1: 0.5]"))
    assert(modelStr.contains("[lambda_l2: 0.1]") || modelStr.contains("[lambda_l2: 0.5]"))
  }
}
