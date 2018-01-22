// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

/** Tests to validate the functionality of LightGBM module.
  */
class VerifyLightGBM extends Benchmarks {
  val mockLabelColumn = "Label"

  verifyLearnerOnBinaryCsvFile("PimaIndian.csv", "Diabetes mellitus", 2, true)

  def verifyLearnerOnBinaryCsvFile(fileName: String,
                                   labelColumnName: String,
                                   decimals: Int,
                                   includeNaiveBayes: Boolean): Unit = {
    test("Verify LightGBM can be trained and scored on " + fileName, TestBase.Extended) {
      val fileLocation = ClassifierTestUtils.classificationTrainFile(fileName).toString
      val dataset: DataFrame =
        session.read.format("com.databricks.spark.csv")
          .option("header", "true").option("inferSchema", "true")
          .option("treatEmptyValuesAsNulls", "false")
          .option("delimiter", if (fileName.endsWith(".csv")) "," else "\t")
          .load(fileLocation)
      val model = new LightGBM().setLabelCol(labelColumnName).fit(dataset)
    }
  }

  test("Verify call to LGBM_DatasetCreateFromMat") {
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm")
    val nodesKeys = session.sparkContext.getExecutorMemoryStatus.keys
    val nodes = nodesKeys.mkString(",")
    val numNodes = nodesKeys.count((node: String) => true)
    LightGBMUtils.validate(lightgbmlib.LGBM_NetworkInit(nodes, LightGBM.defaultListenTimeout,
      LightGBM.defaultLocalListenPort, numNodes), "Network Init")
    val random = new Random(0)
    val rows = (0 to 10).toArray
      .map(_ => Row(new DenseVector(Array(random.nextDouble(), random.nextDouble(), random.nextDouble(),
        random.nextDouble()))))
    val numRows = rows.length
    val numRowsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numRowsIntPtr, numRows)
    val numRows_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numRowsIntPtr)
    val rowsAsDoubleArray = rows.map(row => row.get(0) match {
      case dense: DenseVector => dense.toArray
      case spase: SparseVector => spase.toDense.toArray
    })
    val numCols = rowsAsDoubleArray.head.length
    val data = lightgbmlib.new_doubleArray(numCols * numRows)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
      ri._1.zipWithIndex.foreach(value =>
        lightgbmlib.doubleArray_setitem(data, value._2 + (ri._2 * numCols), value._1)))
    val dataAsVoidPtr = lightgbmlib.double_to_voidp_ptr(data)
    val isRowMajor = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(isRowMajor, 1)
    val isRowMajor_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(isRowMajor)
    val numColsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numColsIntPtr, numCols)
    val numCols_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numColsIntPtr)
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(dataAsVoidPtr, lightgbmlibConstants.C_API_DTYPE_FLOAT64,
      numRows_int32_tPtr, numCols_int32_tPtr, 1, "max_bin=15 min_data=1", null, datasetOutPtr), "Dataset create")
  }
  override val moduleName: String = "LightGBM"
}
