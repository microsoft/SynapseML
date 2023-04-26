// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable

class VerifyOrthoForestVariableTranformer extends TransformerFuzzing[OrthoForestVariableTransformer] {

  private lazy val mockDataset = spark.createDataFrame(Seq(
    (1,0.1d,1.1d),
    (2,0.2d,1.2d),
    (3,0.3d,1.3d),
    (4,0.4d,1.4d),
    (5,0.5d,1.5d),
    (6,0.6d,1.6d),
    (7,0.7d,1.7d)))
    .toDF("label","TResid", "OResid")

  private lazy val expectedDF = spark.createDataFrame(Seq(
    (1,0.1d, 1.1d,11d,0.01d),
    (2,0.2d, 1.2d,6d, 0.04d),
    (3,0.3d, 1.3d,4.3333d,0.09d),
    (4,0.4d, 1.4d,3.5d,0.16d),
    (5,0.5d, 1.5d,3d,0.25),
    (6,0.6d, 1.6d,2.66667d,0.36d),
    (7,0.7d, 1.7d,2.428571d,0.49d)))
    .toDF("label","TResid", "OResid","_tmp_tsOutcome","_tmp_twOutcome")

  test("Compute Transformation") {
    val computeResiduals = new OrthoForestVariableTransformer()

    val processedDF = computeResiduals.transform(mockDataset)

    assert(verifyResult(expectedDF, processedDF))
  }

  override def testObjects(): Seq[TestObject[OrthoForestVariableTransformer]] = Seq(new TestObject(
    new OrthoForestVariableTransformer(), mockDataset))

  override def reader: MLReadable[_] = OrthoForestVariableTransformer
}
