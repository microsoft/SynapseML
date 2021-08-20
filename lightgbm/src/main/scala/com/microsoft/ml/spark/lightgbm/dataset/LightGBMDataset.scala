// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.dataset

import com.microsoft.lightgbm.SwigPtrWrapper
import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.lightgbm.LightGBMUtils
import com.microsoft.ml.spark.lightgbm.dataset.DatasetUtils.countCardinality

import scala.reflect.ClassTag

/** Represents a LightGBM dataset.
  * Wraps the native implementation.
  * @param datasetPtr The native representation of the dataset.
  */
class LightGBMDataset(val datasetPtr: SWIGTYPE_p_void) extends AutoCloseable {
  def getLabel(): Array[Float] = {
    getField[Float]("label")
  }

  def getField[T: ClassTag](fieldName: String): Array[T] = {
    // The result length
    val tmpOutLenPtr = lightgbmlib.new_int32_tp()
    // The type of the result array
    val outTypePtr = lightgbmlib.new_int32_tp()
    // The pointer to the result
    val outArray = lightgbmlib.new_voidpp()
    lightgbmlib.LGBM_DatasetGetField(datasetPtr, fieldName, tmpOutLenPtr, outArray, outTypePtr)
    val outType = lightgbmlib.int32_tp_value(outTypePtr)
    val outLength = lightgbmlib.int32_tp_value(tmpOutLenPtr)
    // Note: hacky workaround for now until new pointer manipulation functions are added
    val voidptr = lightgbmlib.voidpp_value(outArray)
    val address = new SwigPtrWrapper(voidptr).getCPtrValue()
    if (outType == lightgbmlibConstants.C_API_DTYPE_INT32) {
      (0 until outLength).map(index =>
        lightgbmlibJNI.intArray_getitem(address, index).asInstanceOf[T]).toArray
    } else if (outType == lightgbmlibConstants.C_API_DTYPE_FLOAT32) {
      (0 until outLength).map(index =>
        lightgbmlibJNI.floatArray_getitem(address, index).asInstanceOf[T]).toArray
    } else if (outType == lightgbmlibConstants.C_API_DTYPE_FLOAT64) {
      (0 until outLength).map(index =>
        lightgbmlibJNI.doubleArray_getitem(address, index).asInstanceOf[T]).toArray
    } else {
      throw new Exception("Unknown type returned from native lightgbm in LightGBMDataset getField")
    }
  }

  /** Get the number of rows in the Dataset.
    * @return The number of rows.
    */
  def numData(): Int = {
    val numDataPtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumData(datasetPtr, numDataPtr), "DatasetGetNumData")
    val numData = lightgbmlib.intp_value(numDataPtr)
    lightgbmlib.delete_intp(numDataPtr)
    numData
  }

  /** Get the number of features in the Dataset.
    * @return The number of features.
    */
  def numFeature(): Int = {
    val numFeaturePtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumFeature(datasetPtr, numFeaturePtr), "DatasetGetNumFeature")
    val numFeature = lightgbmlib.intp_value(numFeaturePtr)
    lightgbmlib.delete_intp(numFeaturePtr)
    numFeature
  }

  def validateDataset(): Unit = {
    // Validate num rows
    val numData = this.numData()
    if (numData <= 0) {
      throw new Exception("Unexpected num data: " + numData)
    }

    // Validate num cols
    val numFeature = this.numFeature()
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
      lightgbmlib.LGBM_DatasetSetField(datasetPtr, fieldName, colAsVoidPtr, numRows, data32bitType),
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
      lightgbmlib.LGBM_DatasetSetField(datasetPtr, fieldName, colAsVoidPtr, numRows, data64bitType),
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
        lightgbmlib.LGBM_DatasetSetField(datasetPtr, fieldName, colAsVoidPtr, numRows, data32bitType),
        "DatasetSetField")
    } finally {
      // Free column
      colArray.foreach(lightgbmlib.delete_intArray)
    }
  }

  def addGroupColumn[T](rows: Array[T]): Unit = {
    // Convert to distinct count (note ranker should have sorted within partition by group id)
    // We use a triplet of a list of cardinalities, last unique value and unique value count
    val groupCardinality = countCardinality(rows)
    addIntField(groupCardinality, "group", groupCardinality.length)
  }

  def setFeatureNames(featureNamesOpt: Option[Array[String]], numCols: Int): Unit = {
    // Add in slot names if they exist
    featureNamesOpt.foreach { featureNamesVal =>
      if (featureNamesVal.nonEmpty) {
        LightGBMUtils.validate(lightgbmlib.LGBM_DatasetSetFeatureNames(datasetPtr, featureNamesVal, numCols),
          "Dataset set feature names")
      }
    }
  }

  override def close(): Unit = {
    // Free dataset
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetFree(datasetPtr), "Finalize Dataset")
  }
}
