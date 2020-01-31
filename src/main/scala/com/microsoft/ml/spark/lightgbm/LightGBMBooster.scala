// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.lightgbm.LightGBMUtils.getBoosterPtrFromModelString
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Represents a LightGBM Booster learner
  * @param model The string serialized representation of the learner
  */
@SerialVersionUID(777L)
class LightGBMBooster(val model: String) extends Serializable {
  /** Transient variable containing local machine's pointer to native booster
    */
  @transient
  lazy val boosterPtr: SWIGTYPE_p_void = {
    LightGBMUtils.initializeNativeLibrary()
    getBoosterPtrFromModelString(model)
  }

  def score(features: Vector, raw: Boolean, classification: Boolean): Array[Double] = {
    // Reload booster on each node
    val kind =
      if (raw) lightgbmlibConstants.C_API_PREDICT_RAW_SCORE
      else lightgbmlibConstants.C_API_PREDICT_NORMAL
    features match {
      case dense: DenseVector => predictScoreForMat(dense.toArray, kind, classification)
      case sparse: SparseVector => predictScoreForCSR(sparse, kind, classification)
    }
  }

  def predictLeaf(features: Vector): Array[Double] = {
    // Reload booster on each node
    features match {
      case dense: DenseVector => predictLeafForMat(dense.toArray)
      case sparse: SparseVector => predictLeafForCSR(sparse)
    }
  }

  lazy val numClasses: Int = getNumClasses()

  lazy val numTotalModel: Int = getNumTotalModel

  lazy val numModelPerIteration: Int = getNumModelPerIteration

  lazy val numIterations: Int = numTotalModel / numModelPerIteration

  @transient
  lazy val scoredDataOutPtr: SWIGTYPE_p_double = {
    lightgbmlib.new_doubleArray(numClasses)
  }

  @transient
  lazy val scoredDataLengthLongPtr: SWIGTYPE_p_long_long = {
    val dataLongLengthPtr = lightgbmlib.new_int64_tp()
    lightgbmlib.int64_tp_assign(dataLongLengthPtr, 1)
    dataLongLengthPtr
  }

  @transient
  lazy val leafIndexDataOutPtr: SWIGTYPE_p_double = lightgbmlib.new_doubleArray(numTotalModel)

  @transient
  lazy val leafIndexDataLengthLongPtr: SWIGTYPE_p_long_long = {
    val dataLongLengthPtr = lightgbmlib.new_int64_tp()
    lightgbmlib.int64_tp_assign(dataLongLengthPtr, numTotalModel)
    dataLongLengthPtr
  }

  override protected def finalize(): Unit = {
    if (scoredDataLengthLongPtr != null)
      lightgbmlib.delete_int64_tp(scoredDataLengthLongPtr)
    if (scoredDataOutPtr != null)
      lightgbmlib.delete_doubleArray(scoredDataOutPtr)
    if (leafIndexDataLengthLongPtr != null)
      lightgbmlib.delete_int64_tp(leafIndexDataLengthLongPtr)
    if (leafIndexDataOutPtr != null)
      lightgbmlib.delete_doubleArray(leafIndexDataOutPtr)
  }

  protected def predictScoreForCSR(sparseVector: SparseVector, kind: Int, classification: Boolean): Array[Double] = {
    val numCols = sparseVector.size

    val datasetParams = "max_bin=255"
    val dataInt32bitType = lightgbmlibConstants.C_API_DTYPE_INT32
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForCSRSingle(
        sparseVector.indices, sparseVector.values,
        sparseVector.numNonzeros,
        boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
        kind, -1, datasetParams,
        scoredDataLengthLongPtr, scoredDataOutPtr), "Booster Predict")

    predScoreToArray(classification, scoredDataOutPtr, kind)
  }

  protected def predictScoreForMat(row: Array[Double], kind: Int, classification: Boolean): Array[Double] = {
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    val numCols = row.length
    val isRowMajor = 1

    val datasetParams = "max_bin=255"

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForMatSingle(
        row, boosterPtr, data64bitType,
        numCols,
        isRowMajor, kind,
        -1, datasetParams, scoredDataLengthLongPtr, scoredDataOutPtr),
      "Booster Predict")
    predScoreToArray(classification, scoredDataOutPtr, kind)
  }

  protected def predictLeafForCSR(sparseVector: SparseVector): Array[Double] = {
    val numCols = sparseVector.size

    val datasetParams = "max_bin=255 is_pre_partition=True"
    val dataInt32bitType = lightgbmlibConstants.C_API_DTYPE_INT32
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForCSRSingle(
        sparseVector.indices, sparseVector.values,
        sparseVector.numNonzeros,
        boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
        lightgbmlibConstants.C_API_PREDICT_LEAF_INDEX, -1, datasetParams,
        leafIndexDataLengthLongPtr, leafIndexDataOutPtr), "Booster Predict Leaf")

    predLeafToArray(leafIndexDataOutPtr)
  }

  protected def predictLeafForMat(row: Array[Double]): Array[Double] = {
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    val numCols = row.length
    val isRowMajor = 1

    val datasetParams = "max_bin=255"

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForMatSingle(
        row, boosterPtr, data64bitType,
        numCols,
        isRowMajor, lightgbmlibConstants.C_API_PREDICT_LEAF_INDEX,
        -1, datasetParams, leafIndexDataLengthLongPtr, leafIndexDataOutPtr),
      "Booster Predict Leaf")

    predLeafToArray(leafIndexDataOutPtr)
  }

  def saveNativeModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
    if (filename == null || filename.isEmpty) {
      throw new IllegalArgumentException("filename should not be empty or null.")
    }
    val rdd = session.sparkContext.parallelize(Seq(model))
    import session.sqlContext.implicits._
    val dataset = session.sqlContext.createDataset(rdd)
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataset.coalesce(1).write.mode(mode).text(filename)
  }

  def dumpModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
    val json = lightgbmlib.LGBM_BoosterDumpModelSWIG(boosterPtr, 0, 0, 1, lightgbmlib.new_int64_tp())
    val rdd = session.sparkContext.parallelize(Seq(json))
    import session.sqlContext.implicits._
    val dataset = session.sqlContext.createDataset(rdd)
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataset.coalesce(1).write.mode(mode).text(filename)
  }

  /**
    * Calls into LightGBM to retrieve the feature importances.
    * @param importanceType Can be "split" or "gain"
    * @return The feature importance values as an array.
    */
  def getFeatureImportances(importanceType: String): Array[Double] = {
    val importanceTypeNum = if (importanceType.toLowerCase.trim == "gain") 1 else 0
    val numFeaturesOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterGetNumFeature(boosterPtr, numFeaturesOut),
      "Booster NumFeature")
    val numFeatures = lightgbmlib.intp_value(numFeaturesOut)
    val featureImportances = lightgbmlib.new_doubleArray(numFeatures)
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterFeatureImportance(boosterPtr, -1, importanceTypeNum, featureImportances),
      "Booster FeatureImportance")
    (0 until numFeatures).map(lightgbmlib.doubleArray_getitem(featureImportances, _)).toArray
  }

  /**
    * Retrieve the number of classes from LightGBM Booster
    * @return The number of classes.
    */
  def getNumClasses(): Int = {
    val numClassesOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterGetNumClasses(boosterPtr, numClassesOut),
      "Booster NumClasses")
    lightgbmlib.intp_value(numClassesOut)
  }

  /**
    * Retrieve the number of models per each iteration from LightGBM Booster
    * @return The number of models per iteration.
    */
  def getNumModelPerIteration: Int = {
    val numModelPerIterationOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterNumModelPerIteration(boosterPtr, numModelPerIterationOut),
      "Booster models per iteration")
    lightgbmlib.intp_value(numModelPerIterationOut)
  }

  /**
    * Retrieve the number of total models from LightGBM Booster
    * @return The number of total models.
    */
  def getNumTotalModel: Int = {
    val numModelOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterNumberOfTotalModel(boosterPtr, numModelOut),
      "Booster total models")
    lightgbmlib.intp_value(numModelOut)
  }

  private def predScoreToArray(classification: Boolean, scoredDataOutPtr: SWIGTYPE_p_double,
                               kind: Int): Array[Double] = {
    if (classification && numClasses == 1) {
      // Binary classification scenario - LightGBM only returns the value for the positive class
      val pred = lightgbmlib.doubleArray_getitem(scoredDataOutPtr, 0)
      if (kind == lightgbmlibConstants.C_API_PREDICT_RAW_SCORE) {
        // Return the raw score for binary classification
        Array(-pred, pred)
      } else {
        // Return the probability for binary classification
        Array(1 - pred, pred)
      }
    } else {
      (0 until numClasses).map(classNum =>
        lightgbmlib.doubleArray_getitem(scoredDataOutPtr, classNum)).toArray
    }
  }

  private def predLeafToArray(leafIndexDataOutPtr: SWIGTYPE_p_double): Array[Double] = {
    (0 until numTotalModel).map(modelNum =>
      lightgbmlib.doubleArray_getitem(leafIndexDataOutPtr, modelNum)).toArray
  }
}
