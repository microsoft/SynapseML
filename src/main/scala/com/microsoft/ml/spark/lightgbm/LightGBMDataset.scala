// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm.{floatChunkedArray, _}

/** Represents a LightGBM dataset.
  * Wraps the native implementation.
  * @param dataset The native representation of the dataset.
  */
class LightGBMDataset(val dataset: SWIGTYPE_p_void) extends AutoCloseable {
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
      colArray = Some(lightgbmlib.new_floatArray(numRows.toLong))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.floatArray_setitem(colArray.get, ri._2.toLong, ri._1.toFloat))
      this.addFloatField(colArray.get, fieldName, numRows)
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_floatArray)
    }
  }

  def addFloatField(field: floatChunkedArray, fieldName: String, numRows: Int): Unit = {
    val coalescedLabelArray = lightgbmlib.new_floatArray(field.get_add_count())
    try {
      field.coalesce_to(coalescedLabelArray)
      this.addFloatField(coalescedLabelArray, fieldName, numRows)
    } finally {
      // Free column
      lightgbmlib.delete_floatArray(coalescedLabelArray)
    }
  }

  def addFloatField(field: SWIGTYPE_p_float, fieldName: String, numRows: Int): Unit = {
    // Add the column to dataset
    val colAsVoidPtr = lightgbmlib.float_to_voidp_ptr(field)
    val data32bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT32
    LightGBMUtils.validate(
      lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data32bitType),
      "DatasetSetField")
  }

  def addDoubleField(field: Array[Double], fieldName: String, numRows: Int): Unit = {
    // Generate the column and add to dataset
    var colArray: Option[SWIGTYPE_p_double] = None
    try {
      colArray = Some(lightgbmlib.new_doubleArray(field.length.toLong))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.doubleArray_setitem(colArray.get, ri._2.toLong, ri._1))
      this.addDoubleField(colArray.get, fieldName, numRows)
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_doubleArray)
    }
  }

  def addDoubleField(field: doubleChunkedArray, fieldName: String, numRows: Int): Unit = {
    val coalescedLabelArray = lightgbmlib.new_doubleArray(field.get_add_count())
    try {
      field.coalesce_to(coalescedLabelArray)
      this.addDoubleField(coalescedLabelArray, fieldName, numRows)
    } finally {
      // Free column
      lightgbmlib.delete_doubleArray(coalescedLabelArray)
    }
  }

  def addDoubleField(field: SWIGTYPE_p_double, fieldName: String, numRows: Int): Unit = {
    // Add the column to dataset
    val colAsVoidPtr = lightgbmlib.double_to_voidp_ptr(field)
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64
    LightGBMUtils.validate(
      lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data64bitType),
      "DatasetSetField")
  }

  def addIntField(field: Array[Int], fieldName: String, numRows: Int): Unit = {
    // Generate the column and add to dataset
    var colArray: Option[SWIGTYPE_p_int] = None
    try {
      colArray = Some(lightgbmlib.new_intArray(numRows.toLong))
      field.zipWithIndex.foreach(ri =>
        lightgbmlib.intArray_setitem(colArray.get, ri._2.toLong, ri._1))
      val colAsVoidPtr = lightgbmlib.int_to_voidp_ptr(colArray.get)
      val data32bitType = lightgbmlibConstants.C_API_DTYPE_INT32
      LightGBMUtils.validate(
        lightgbmlib.LGBM_DatasetSetField(dataset, fieldName, colAsVoidPtr, numRows, data32bitType),
        "DatasetSetField")
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_intArray)
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
