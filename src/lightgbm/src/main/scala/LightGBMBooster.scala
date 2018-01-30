// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_void, lightgbmlib, lightgbmlibConstants}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}

/** Represents a LightGBM Booster learner
  * @param model The string serialized representation of the learner
  */
class LightGBMBooster(val model: String) extends Serializable {
  /**
    * Transient variable containing local machine's pointer to native booster
    */
  @transient
  var boosterPtr: SWIGTYPE_p_void = null

  def numClasses(): Int = {
    if (boosterPtr == null) {
      LightGBMUtils.initializeNativeLibrary()
      boosterPtr = getModel()
    }
    val numClasses = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterGetNumClasses(boosterPtr, numClasses), "Booster GetNumClasses")
    lightgbmlib.intp_value(numClasses)
  }

  def scoreRaw(features: Vector): Double = {
    // Reload booster on each node
    if (boosterPtr == null) {
      LightGBMUtils.initializeNativeLibrary()
      boosterPtr = getModel()
    }
    val numRows = 1
    val rowsAsDoubleArray = features match {
      case dense: DenseVector => dense.toArray
      case sparse: SparseVector => sparse.toDense.toArray
    }
    val dataPtr = LightGBMUtils.generateData(numRows, Array(rowsAsDoubleArray))
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    val numRowsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numRowsIntPtr, numRows)
    val numRows_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numRowsIntPtr)
    val scoredDataLengthLongPtr = lightgbmlib.new_longp()
    lightgbmlib.longp_assign(scoredDataLengthLongPtr, numRows)
    val scoredDataLength_int64_tPtr = lightgbmlib.long_to_int64_t_ptr(scoredDataLengthLongPtr)

    val numCols = rowsAsDoubleArray.length
    val isRowMajor = 1
    val numColsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numColsIntPtr, numCols)
    val numCols_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numColsIntPtr)
    val scoredDataOutPtr = lightgbmlib.new_doubleArray(numRows)
    val datasetParams = "max_bin=255"
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterPredictForMat(boosterPtr, dataPtr, data64bitType,
      numRows_int32_tPtr, numCols_int32_tPtr, isRowMajor, lightgbmlibConstants.C_API_PREDICT_RAW_SCORE,
      -1, datasetParams, scoredDataLength_int64_tPtr, scoredDataOutPtr), "Booster Predict")
    lightgbmlib.doubleArray_getitem(scoredDataOutPtr, 0)
  }

  protected def getModel(): SWIGTYPE_p_void = {
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    val numItersOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterLoadModelFromString(model, numItersOut, boosterOutPtr),
      "Booster LoadFromString")
    lightgbmlib.voidpp_value(boosterOutPtr)
  }

  def mergeModels(newModel: LightGBMBooster): LightGBMBooster = {
    // Merges models to first handle
    if (model != null) {
      LightGBMUtils.validate(lightgbmlib.LGBM_BoosterMerge(getModel(), newModel.getModel()), "Booster merge")
    }
    new LightGBMBooster(model)
  }
}