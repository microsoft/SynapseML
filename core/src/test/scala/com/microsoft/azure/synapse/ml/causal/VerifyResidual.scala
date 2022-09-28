package com.microsoft.azure.synapse.ml.causal


import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame

class VerifyResidual extends TestBase {

  val mockLabelColumn = "Label"

  private lazy val mockDataset = spark.createDataFrame(Seq(
    (0, 2d, 0.50d, true, 0, 0.toByte, 12F),
    (1, 3d, 0.40d, false, 1, 100.toByte, 30F),
    (0, 4d, 0.78d, true, 2, 50.toByte, 12F),
    (1, 5d, 0.12d, false, 3, 0.toByte, 12F),
    (0, 1d, 0.50d, true, 0, 0.toByte, 30F),
    (1, 3d, 0.40d, false, 1, 10.toByte, 12F),
    (0, 3d, 0.78d, false, 2, 0.toByte, 12F),
    (1, 4d, 0.12d, false, 3, 0.toByte, 12F),
    (0, 0d, 0.50d, true, 0, 0.toByte, 12F),
    (1, 2d, 0.40d, false, 1, 127.toByte, 30F),
    (0, 3d, 0.78d, true, 2, -128.toByte, 12F),
    (1, 4d, 0.12d, false, 3, 0.toByte, 12F)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5", "col6")

  private lazy val expectedDF = spark.createDataFrame(Seq(
    (0, 2d, 0.50d, true, 0, 0.toByte, 12F,                    1.5),
    (1, 3d, 0.40d, false, 1, 100.toByte, 30F,                 2.6),
    (0, 4d, 0.78d,  true, 2, 50.toByte, 12F,   3.2199999999999998),
    (1, 5d, 0.12d,  false, 3, 0.toByte, 12F,                 4.88),
    (0, 1d, 0.50d, true, 0, 0.toByte, 30F,                    0.5),
    (1, 3d, 0.40d, false, 1, 10.toByte, 12F,                  2.6),
    (0, 3d, 0.78d,  false, 2, 0.toByte, 12F,   2.2199999999999998),
    (1, 4d, 0.12d,  false, 3, 0.toByte, 12F,                 3.88),
    (0, 0d, 0.50d,  true, 0, 0.toByte, 12F,                  -0.5),
    (1, 2d, 0.40d, false, 1, 127.toByte, 30F,                 1.6),
    (0, 3d, 0.78d,  true, 2, -128.toByte, 12F, 2.2199999999999998),
    (1, 4d, 0.12d,  false, 3, 0.toByte, 12F,                 3.88)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5", "col6", "diff")


  test("Compute residual") {
    val computeResiduals = new ComputeResidualTransformer()
      .setObservedCol("col1")
      .setOutputCol("diff")
      .setPredictedCol("col2")

    var processedDF = computeResiduals.transform(mockDataset)

    assert(verifyResult(expectedDF, processedDF))
  }
}
