// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.booster

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.lightgbm.{LightGBMConstants, LightGBMUtils}
import com.microsoft.ml.spark.lightgbm.dataset.LightGBMDataset
import com.microsoft.ml.spark.lightgbm.swig.SwigUtils
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.{SaveMode, SparkSession}

//scalastyle:off
protected abstract class NativePtrHandler[T](val ptr: T) {
  protected def freeNativePtr(): Unit
  override def finalize(): Unit = {
    if (ptr != null) {
      freeNativePtr()
    }
  }
}

protected class DoubleNativePtrHandler(ptr: SWIGTYPE_p_double) extends NativePtrHandler[SWIGTYPE_p_double](ptr) {
  override protected def freeNativePtr(): Unit = {
    lightgbmlib.delete_doubleArray(ptr)
  }
}

protected class LongLongNativePtrHandler(ptr: SWIGTYPE_p_long_long) extends NativePtrHandler[SWIGTYPE_p_long_long](ptr) {
  override protected def freeNativePtr(): Unit = {
    lightgbmlib.delete_int64_tp(ptr)
  }
}

protected object BoosterHandler {
  /**
    * Creates the native booster from the given string representation by calling LGBM_BoosterLoadModelFromString.
    * @param lgbModelString The string representation of the model.
    * @return The SWIG pointer to the native representation of the booster.
    */
  private def createBoosterPtrFromModelString(lgbModelString: String): SWIGTYPE_p_void = {
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    val numItersOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterLoadModelFromString(lgbModelString, numItersOut, boosterOutPtr),
      "Booster LoadFromString")
    lightgbmlib.voidpp_value(boosterOutPtr)
  }
}

/** Wraps the boosterPtr and guarantees that Native library is initialized
 * everytime it is needed
 * @param boosterPtr The pointer to the native lightgbm booster
 */
protected class BoosterHandler(var boosterPtr: SWIGTYPE_p_void) {

  /** Wraps the boosterPtr and guarantees that Native library is initialized
    * everytime it is needed
    *
    * @param model The string serialized representation of the learner
    */
  def this(model: String) = {
    this(BoosterHandler.createBoosterPtrFromModelString(model))
  }

  val scoredDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numClasses.toLong))
      }
    }
  }

  val scoredDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
    new ThreadLocal[LongLongNativePtrHandler] {
      override def initialValue(): LongLongNativePtrHandler = {
        val dataLongLengthPtr = lightgbmlib.new_int64_tp()
        lightgbmlib.int64_tp_assign(dataLongLengthPtr, 1)
        new LongLongNativePtrHandler(dataLongLengthPtr)
      }
    }
  }

  val leafIndexDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numTotalModel.toLong))
      }
    }
  }

  val leafIndexDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
    new ThreadLocal[LongLongNativePtrHandler] {
      override def initialValue(): LongLongNativePtrHandler = {
        val dataLongLengthPtr = lightgbmlib.new_int64_tp()
        lightgbmlib.int64_tp_assign(dataLongLengthPtr, numTotalModel)
        new LongLongNativePtrHandler(dataLongLengthPtr)
      }
    }
  }

  // Note for binary case LightGBM only outputs the SHAP values for the positive class
  val shapOutputShape: Long = if (numClasses > 2) (numFeatures + 1) * numClasses else numFeatures + 1

  val shapDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(shapOutputShape))
      }
    }
  }

  val shapDataLengthLongPtr: ThreadLocal[LongLongNativePtrHandler] = {
    new ThreadLocal[LongLongNativePtrHandler] {
      override def initialValue(): LongLongNativePtrHandler = {
        val dataLongLengthPtr = lightgbmlib.new_int64_tp()
        lightgbmlib.int64_tp_assign(dataLongLengthPtr, shapOutputShape)
        new LongLongNativePtrHandler(dataLongLengthPtr)
      }
    }
  }

  val featureImportanceOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numFeatures.toLong))
      }
    }
  }

  val dumpModelOutPtr: ThreadLocal[LongLongNativePtrHandler] = {
    new ThreadLocal[LongLongNativePtrHandler] {
      override def initialValue(): LongLongNativePtrHandler = {
        new LongLongNativePtrHandler(lightgbmlib.new_int64_tp())
      }
    }
  }

  lazy val numClasses: Int = getNumClasses
  lazy val numFeatures: Int = getNumFeatures
  lazy val numTotalModel: Int = getNumTotalModel
  lazy val numTotalModelPerIteration: Int = getNumModelPerIteration

  lazy val rawScoreConstant: Int = lightgbmlibConstants.C_API_PREDICT_RAW_SCORE
  lazy val normalScoreConstant: Int = lightgbmlibConstants.C_API_PREDICT_NORMAL
  lazy val leafIndexPredictConstant: Int = lightgbmlibConstants.C_API_PREDICT_LEAF_INDEX
  lazy val contribPredictConstant = lightgbmlibConstants.C_API_PREDICT_CONTRIB

  lazy val dataInt32bitType: Int = lightgbmlibConstants.C_API_DTYPE_INT32
  lazy val data64bitType: Int = lightgbmlibConstants.C_API_DTYPE_FLOAT64

  def freeNativeMemory(): Unit = {
    if (boosterPtr != null) {
      LightGBMUtils.validate(lightgbmlib.LGBM_BoosterFree(boosterPtr), "Finalize Booster")
      boosterPtr = null
    }
  }

  private def getNumClasses: Int = {
    val numClassesOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterGetNumClasses(boosterPtr, numClassesOut),
      "Booster NumClasses")
    val out = lightgbmlib.intp_value(numClassesOut)
    lightgbmlib.delete_intp(numClassesOut)
    out
  }

  private def getNumModelPerIteration: Int = {
    val numModelPerIterationOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterNumModelPerIteration(boosterPtr, numModelPerIterationOut),
      "Booster models per iteration")
    val out = lightgbmlib.intp_value(numModelPerIterationOut)
    lightgbmlib.delete_intp(numModelPerIterationOut)
    out
  }

  private def getNumTotalModel: Int = {
    val numModelOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterNumberOfTotalModel(boosterPtr, numModelOut),
      "Booster total models")
    val out = lightgbmlib.intp_value(numModelOut)
    lightgbmlib.delete_intp(numModelOut)
    out
  }

  private def getNumFeatures: Int = {
    val numFeaturesOut = lightgbmlib.new_intp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterGetNumFeature(boosterPtr, numFeaturesOut),
      "Booster NumFeature")
    val out = lightgbmlib.intp_value(numFeaturesOut)
    lightgbmlib.delete_intp(numFeaturesOut)
    out
  }

  override protected def finalize(): Unit = {
    freeNativeMemory()
    super.finalize()
  }
}

/** Represents a LightGBM Booster learner
  * @param trainDataset The training dataset
  * @param parameters The booster initialization parameters
  * @param modelStr Optional parameter with the string serialized representation of the learner
  */
@SerialVersionUID(777L)
class LightGBMBooster(val trainDataset: Option[LightGBMDataset] = None, val parameters: Option[String] = None,
                      val modelStr: Option[String] = None) extends Serializable {

  /** Represents a LightGBM Booster learner
    * @param trainDataset The training dataset
    * @param parameters The booster initialization parameters
    */
  def this(trainDataset: LightGBMDataset, parameters: String) = {
    this(Some(trainDataset), Some(parameters))
  }

  /** Represents a LightGBM Booster learner
    * @param model The string serialized representation of the learner
    */
  def this(model: String) = {
    this(modelStr = Some(model))
  }

  @transient
  lazy val boosterHandler: BoosterHandler = {
    LightGBMUtils.initializeNativeLibrary()
    if (trainDataset.isEmpty && modelStr.isEmpty) {
      throw new IllegalArgumentException("One of training dataset or serialized model parameters must be specified")
    }
    if (trainDataset.isEmpty) {
      new BoosterHandler(modelStr.get)
    } else {
      val boosterOutPtr = lightgbmlib.voidpp_handle()
      LightGBMUtils.validate(lightgbmlib.LGBM_BoosterCreate(trainDataset.map(_.datasetPtr).get,
        parameters.get, boosterOutPtr), "Booster")
      new BoosterHandler(lightgbmlib.voidpp_value(boosterOutPtr))
    }
  }

  var bestIteration: Int = -1
  private var startIteration: Int = 0
  private var numIterations: Int = -1

  /** Merges this Booster with the specified model.
    * @param model The string serialized representation of the learner to merge.
    */
  def mergeBooster(model: String): Unit = {
    val mergedBooster = new BoosterHandler(model)
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterMerge(boosterHandler.boosterPtr, mergedBooster.boosterPtr),
      "Booster Merge")
  }

  /** Adds the specified LightGBMDataset to be the validation dataset.
    * @param dataset The LightGBMDataset to add as the validation dataset.
    */
  def addValidationDataset(dataset: LightGBMDataset): Unit = {
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterAddValidData(boosterHandler.boosterPtr,
      dataset.datasetPtr), "Add Validation Dataset")
  }

  /** Saves the booster to string representation.
    * @return The serialized string representation of the Booster.
    */
  def saveToString(): String = {
      val bufferLength = LightGBMConstants.DefaultBufferLength
      val bufferOutLengthPtr = lightgbmlib.new_int64_tp()
      lightgbmlib.LGBM_BoosterSaveModelToStringSWIG(boosterHandler.boosterPtr,
        0, -1, 0, bufferLength, bufferOutLengthPtr)
  }

  /** Get the evaluation dataset column names from the native booster.
    * @return The evaluation dataset column names.
    */
  def getEvalNames(): Array[String] = {
    // Need to keep track of best scores for each metric, see callback.py in lightgbm for reference
    // For debugging, can get metric names
    val stringArrayHandle = lightgbmlib.LGBM_BoosterGetEvalNamesSWIG(boosterHandler.boosterPtr)
    LightGBMUtils.validateArray(stringArrayHandle, "Booster Get Eval Names")
    val evalNames = lightgbmlib.StringArrayHandle_get_strings(stringArrayHandle)
    lightgbmlib.StringArrayHandle_free(stringArrayHandle)
    evalNames
  }

  /** Get the evaluation for the training data and validation data.
    *
    * @param evalNames      The names of the evaluation metrics.
    * @param dataIndex Index of data, 0: training data, 1: 1st validation
    *                  data, 2: 2nd validation data and so on.
    * @return Array of tuples containing the evaluation metric name and metric value.
    */
  def getEvalResults(evalNames: Array[String], dataIndex: Int): Array[(String, Double)] = {
    val evalResults = lightgbmlib.new_doubleArray(evalNames.length.toLong)
    val dummyEvalCountsPtr = lightgbmlib.new_intp()
    val resultEval = lightgbmlib.LGBM_BoosterGetEval(boosterHandler.boosterPtr, dataIndex,
      dummyEvalCountsPtr, evalResults)
    lightgbmlib.delete_intp(dummyEvalCountsPtr)
    LightGBMUtils.validate(resultEval, s"Booster Get Eval Results for data index: ${dataIndex}")

    val results: Array[(String, Double)] = evalNames.zipWithIndex.map { case (evalName, index) =>
      val score = lightgbmlib.doubleArray_getitem(evalResults, index.toLong)
      (evalName, score)
    }
    lightgbmlib.delete_doubleArray(evalResults)
    results
  }

  /** Reset the specified parameters on the native booster.
    * @param newParameters The new parameters to set.
    */
  def resetParameter(newParameters: String) = {
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterResetParameter(boosterHandler.boosterPtr,
      newParameters), "Booster Reset learning_rate Param")
  }

  /** Get predictions for the training and evaluation data on the booster.
    * @param dataIndex Index of data, 0: training data, 1: 1st validation
    *                  data, 2: 2nd validation data and so on.
    * @param classification Whether this is a classification scenario or not.
    * @return The predictions as a 2D array where first level is for row index
    *         and second level is optional if there are classes.
    */
  def innerPredict(dataIndex: Int, classification: Boolean): Array[Array[Double]] = {
    val numRows = this.trainDataset.get.numData()
    val scoredDataOutPtr = lightgbmlib.new_doubleArray(numClasses.toLong * numRows)
    val scoredDataLengthPtr = lightgbmlib.new_int64_tp()
    lightgbmlib.int64_tp_assign(scoredDataLengthPtr, 1)
    lightgbmlib.LGBM_BoosterGetPredict(boosterHandler.boosterPtr, dataIndex,
      scoredDataLengthPtr, scoredDataOutPtr)
    val scoredDataLength = lightgbmlib.int64_tp_value(scoredDataLengthPtr)
    if (classification && numClasses == 1) {
      (0L until scoredDataLength).map(index =>
        Array(lightgbmlib.doubleArray_getitem(scoredDataOutPtr, index))).toArray
    } else {
      val numRows = scoredDataLength / numClasses
      (0L until numRows).map(rowIndex => {
        val startIndex = rowIndex * numClasses
        (0 until numClasses).map(classIndex =>
          lightgbmlib.doubleArray_getitem(scoredDataOutPtr, startIndex + classIndex)).toArray
      }).toArray
    }
  }

  /** Updates the booster for one iteration.
    * @return True if terminated training early.
    */
  def updateOneIteration(): Boolean = {
    val isFinishedPtr = lightgbmlib.new_intp()
    try {
      LightGBMUtils.validate(
        lightgbmlib.LGBM_BoosterUpdateOneIter(boosterHandler.boosterPtr, isFinishedPtr),
        "Booster Update One Iter")
      lightgbmlib.intp_value(isFinishedPtr) == 1
    } finally {
      lightgbmlib.delete_intp(isFinishedPtr)
    }
  }

  /** Updates the booster with custom loss function for one iteration.
    * @param gradient The gradient from custom loss function.
    * @param hessian The hessian matrix from custom loss function.
    * @return True if terminated training early.
    */
  def updateOneIterationCustom(gradient: Array[Float], hessian: Array[Float]): Boolean = {
    var isFinishedPtrOpt: Option[SWIGTYPE_p_int] = None
    var gradientPtrOpt: Option[SWIGTYPE_p_float] = None
    var hessianPtrOpt: Option[SWIGTYPE_p_float] = None
    try {
      val gradPtr = SwigUtils.floatArrayToNative(gradient)
      gradientPtrOpt = Some(gradPtr)
      val hessPtr = SwigUtils.floatArrayToNative(hessian)
      hessianPtrOpt = Some(hessPtr)
      val isFinishedPtr = lightgbmlib.new_intp()
      isFinishedPtrOpt = Some(isFinishedPtr)
      LightGBMUtils.validate(
        lightgbmlib.LGBM_BoosterUpdateOneIterCustom(boosterHandler.boosterPtr,
          gradPtr, hessPtr, isFinishedPtr), "Booster Update One Iter Custom")
      lightgbmlib.intp_value(isFinishedPtr) == 1
    } finally {
      isFinishedPtrOpt.foreach(lightgbmlib.delete_intp(_))
      gradientPtrOpt.foreach(lightgbmlib.delete_floatArray(_))
      hessianPtrOpt.foreach(lightgbmlib.delete_floatArray(_))
    }
  }

  def score(features: Vector, raw: Boolean, classification: Boolean): Array[Double] = {
    val kind =
      if (raw) boosterHandler.rawScoreConstant
      else boosterHandler.normalScoreConstant
    features match {
      case dense: DenseVector => predictForMat(dense.toArray, kind,
        boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr)
      case sparse: SparseVector => predictForCSR(sparse, kind,
        boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr)
    }
    predScoreToArray(classification, boosterHandler.scoredDataOutPtr.get().ptr, kind)
  }

  def predictLeaf(features: Vector): Array[Double] = {
    val kind = boosterHandler.leafIndexPredictConstant
    features match {
      case dense: DenseVector => predictForMat(dense.toArray, kind,
        boosterHandler.leafIndexDataLengthLongPtr.get().ptr, boosterHandler.leafIndexDataOutPtr.get().ptr)
      case sparse: SparseVector => predictForCSR(sparse, kind,
        boosterHandler.leafIndexDataLengthLongPtr.get().ptr, boosterHandler.leafIndexDataOutPtr.get().ptr)
    }
    predLeafToArray(boosterHandler.leafIndexDataOutPtr.get().ptr)
  }

  def featuresShap(features: Vector): Array[Double] = {
    val kind = boosterHandler.contribPredictConstant
    features match {
      case dense: DenseVector => predictForMat(dense.toArray, kind,
        boosterHandler.shapDataLengthLongPtr.get().ptr, boosterHandler.shapDataOutPtr.get().ptr)
      case sparse: SparseVector => predictForCSR(sparse, kind,
        boosterHandler.shapDataLengthLongPtr.get().ptr, boosterHandler.shapDataOutPtr.get().ptr)
    }
    shapToArray(boosterHandler.shapDataOutPtr.get().ptr)
  }

  /** Sets the start index of the iteration to predict.
    * If <= 0, starts from the first iteration.
    * @param startIteration The start index of the iteration to predict.
    */
  def setStartIteration(startIteration: Int): Unit = {
    this.startIteration = startIteration
  }

  /** Sets the total number of iterations used in the prediction.
    * If <= 0, all iterations from ``start_iteration`` are used (no limits).
    * @param numIterations The total number of iterations used in the prediction.
    */
  def setNumIterations(numIterations: Int): Unit = {
    this.numIterations = numIterations
  }

  /** Sets the best iteration and also the numIterations to be the best iteration.
    * @param bestIteration The best iteration computed by early stopping.
    */
  def setBestIteration(bestIteration: Int): Unit = {
    this.bestIteration = bestIteration
    this.numIterations = bestIteration
  }

  /** Saves the native model serialized representation to file.
    * @param session The spark session
    * @param filename The name of the file to save the model to
    * @param overwrite Whether to overwrite if the file already exists
    */
  def saveNativeModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
    if (filename == null || filename.isEmpty) {
      throw new IllegalArgumentException("filename should not be empty or null.")
    }
    val rdd = session.sparkContext.parallelize(Seq(modelStr.get))
    import session.sqlContext.implicits._
    val dataset = session.sqlContext.createDataset(rdd)
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataset.coalesce(1).write.mode(mode).text(filename)
  }

  /** Dumps the native model pointer to file.
    * @param session The spark session
    * @param filename The name of the file to save the model to
    * @param overwrite Whether to overwrite if the file already exists
    */
  def dumpModel(session: SparkSession, filename: String, overwrite: Boolean): Unit = {
    val json = lightgbmlib.LGBM_BoosterDumpModelSWIG(boosterHandler.boosterPtr, 0, -1, 0, 1,
      boosterHandler.dumpModelOutPtr.get().ptr)
    val rdd = session.sparkContext.parallelize(Seq(json))
    import session.sqlContext.implicits._
    val dataset = session.sqlContext.createDataset(rdd)
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    dataset.coalesce(1).write.mode(mode).text(filename)
  }

  /** Frees any native memory held by the underlying booster pointer.
    */
  def freeNativeMemory(): Unit = {
    boosterHandler.freeNativeMemory()
  }

  /**
    * Calls into LightGBM to retrieve the feature importances.
    * @param importanceType Can be "split" or "gain"
    * @return The feature importance values as an array.
    */
  def getFeatureImportances(importanceType: String): Array[Double] = {
    val importanceTypeNum = if (importanceType.toLowerCase.trim == "gain") 1 else 0
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterFeatureImportance(boosterHandler.boosterPtr, -1,
        importanceTypeNum, boosterHandler.featureImportanceOutPtr.get().ptr),
      "Booster FeatureImportance")
    (0L until numFeatures.toLong).map(lightgbmlib.doubleArray_getitem(boosterHandler.featureImportanceOutPtr.get().ptr, _)).toArray
  }

  lazy val numClasses: Int = boosterHandler.numClasses

  lazy val numFeatures: Int = boosterHandler.numFeatures

  lazy val numTotalModel: Int = boosterHandler.numTotalModel

  lazy val numModelPerIteration: Int = boosterHandler.numTotalModelPerIteration

  lazy val numTotalIterations: Int = numTotalModel / numModelPerIteration

  protected def predictForCSR(sparseVector: SparseVector, kind: Int,
                              dataLengthLongPtr: SWIGTYPE_p_long_long,
                              dataOutPtr: SWIGTYPE_p_double): Unit = {
    val numCols = sparseVector.size

    val datasetParams = "max_bin=255"
    val dataInt32bitType = boosterHandler.dataInt32bitType
    val data64bitType = boosterHandler.data64bitType

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForCSRSingle(
        sparseVector.indices, sparseVector.values,
        sparseVector.numNonzeros,
        boosterHandler.boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
        kind, this.startIteration, this.numIterations, datasetParams,
        dataLengthLongPtr, dataOutPtr), "Booster Predict")
  }

  protected def predictForMat(row: Array[Double], kind: Int,
                              dataLengthLongPtr: SWIGTYPE_p_long_long,
                              dataOutPtr: SWIGTYPE_p_double): Unit = {
    val data64bitType = boosterHandler.data64bitType

    val numCols = row.length
    val isRowMajor = 1

    val datasetParams = "max_bin=255"

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForMatSingle(
        row, boosterHandler.boosterPtr, data64bitType,
        numCols,
        isRowMajor, kind,
        this.startIteration, this.numIterations, datasetParams, dataLengthLongPtr, dataOutPtr),
      "Booster Predict")
  }

  private def predScoreToArray(classification: Boolean, scoredDataOutPtr: SWIGTYPE_p_double,
                               kind: Int): Array[Double] = {
    if (classification && numClasses == 1) {
      // Binary classification scenario - LightGBM only returns the value for the positive class
      val pred = lightgbmlib.doubleArray_getitem(scoredDataOutPtr, 0L)
      if (kind == boosterHandler.rawScoreConstant) {
        // Return the raw score for binary classification
        Array(-pred, pred)
      } else {
        // Return the probability for binary classification
        Array(1 - pred, pred)
      }
    } else {
      (0 until numClasses).map(classNum =>
        lightgbmlib.doubleArray_getitem(scoredDataOutPtr, classNum.toLong)).toArray
    }
  }

  private def predLeafToArray(leafIndexDataOutPtr: SWIGTYPE_p_double): Array[Double] = {
    (0 until numTotalModel).map(modelNum =>
      lightgbmlib.doubleArray_getitem(leafIndexDataOutPtr, modelNum.toLong)).toArray
  }

  private def shapToArray(shapDataOutPtr: SWIGTYPE_p_double): Array[Double] = {
    (0L until boosterHandler.shapOutputShape).map(featNum =>
      lightgbmlib.doubleArray_getitem(shapDataOutPtr, featNum)).toArray
  }
}
