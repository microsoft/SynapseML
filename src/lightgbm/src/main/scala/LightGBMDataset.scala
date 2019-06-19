// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm._

/** Represents a LightGBM dataset.
  * Wraps the native implementation.
  * @param dataset The native representation of the dataset.
  */
class LightGBMDataset(val dataset: SWIGTYPE_p_void) extends AutoCloseable {
  var featureNames: Option[SWIGTYPE_p_p_char] = None
  var featureNamesOpt2: Option[Array[String]] = None

  def validateDataset(): Unit = {
    // Validate num rows
    val numDataPtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumData(dataset, numDataPtr), "DatasetGetNumData")
    val numData = lightgbmlib.intp_value(numDataPtr)
    if (numData <= 0) {
      throw new Exception("Unexpected num data: " + numData)
    }

    // Validate num cols
    val numFeaturePtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_DatasetGetNumFeature(dataset, numFeaturePtr),
      "DatasetGetNumFeature")
    val numFeature = lightgbmlib.intp_value(numFeaturePtr)
    if (numFeature <= 0) {
      throw new Exception("Unexpected num feature: " + numFeature)
    }
  }

  def addFloatField(field: Array[Double], fieldName: String, numRows: Int): Unit = {
    // Generate the column and add to dataset
    var colArray: Option[SWIGTYPE_p_float] = None
    try {
      colArray = Some(lightgbmlib.new_floatArray(numRows))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.floatArray_setitem(colArray.get, ri._2, ri._1.toFloat))
      val colAsVoidPtr = lightgbmlib.float_to_voidp_ptr(colArray.get)
      val data32bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT32
      LightGBMUtils.validate(
        lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data32bitType),
        "DatasetSetField")
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_floatArray(_))
    }
  }

  def addDoubleField(field: Array[Double], fieldName: String, numRows: Int): Unit = {
    // Generate the column and add to dataset
    var colArray: Option[SWIGTYPE_p_double] = None
    try {
      colArray = Some(lightgbmlib.new_doubleArray(numRows))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.doubleArray_setitem(colArray.get, ri._2, ri._1))
      val colAsVoidPtr = lightgbmlib.double_to_voidp_ptr(colArray.get)
      val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
      LightGBMUtils.validate(
        lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data64bitType),
        "DatasetSetField")
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_doubleArray(_))
    }
  }

  def addIntField(field: Array[Int], fieldName: String, numRows: Int): Unit = {
    // Generate the column and add to dataset
    var colArray: Option[SWIGTYPE_p_int] = None
    try {
      colArray = Some(lightgbmlib.new_intArray(numRows))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.intArray_setitem(colArray.get, ri._2, ri._1))
      val colAsVoidPtr = lightgbmlib.int_to_voidp_ptr(colArray.get)
      val data32bitType = lightgbmlibConstants.C_API_DTYPE_INT32
      LightGBMUtils.validate(
        lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data32bitType),
        "DatasetSetField")
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_intArray(_))
    }
  }

  def setFeatureNames(featureNamesOpt: Option[Array[String]], numCols: Int): Unit = {
    // Add in slot names if they exist
    featureNamesOpt.foreach { featureNamesVal =>
      if (featureNamesVal.nonEmpty) {
        LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSetFeatureNames(dataset, featureNamesVal, numCols),
          "Dataset set feature names")
      }
    }
  }

  override def close(): Unit = {
    // Free dataset
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetFree(dataset), "Finalize Dataset")
  }
}
