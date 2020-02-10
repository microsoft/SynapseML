// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm

import com.microsoft.ml.lightgbm._
import com.microsoft.ml.spark.lightgbm.LightGBMUtils.getBoosterPtrFromModelString
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

/** Wraps the boosterPtr and guarantees that Native library is initialized
 * everytime it is needed
 * @param model The string serialized representation of the learner
 */
protected class BoosterHandler(model: String) {
  LightGBMUtils.initializeNativeLibrary()

  var boosterPtr: SWIGTYPE_p_void = {
    getBoosterPtrFromModelString(model)
  }

  val scoredDataOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numClasses))
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
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numTotalModel))
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

  val featureImportanceOutPtr: ThreadLocal[DoubleNativePtrHandler] = {
    new ThreadLocal[DoubleNativePtrHandler] {
      override def initialValue(): DoubleNativePtrHandler = {
        new DoubleNativePtrHandler(lightgbmlib.new_doubleArray(numFeatures))
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

  lazy val dataInt32bitType: Int = lightgbmlibConstants.C_API_DTYPE_INT32
  lazy val data64bitType: Int = lightgbmlibConstants.C_API_DTYPE_FLOAT64

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

  private def freeNativeMemory(): Unit = {
    if (boosterPtr != null) {
      LightGBMUtils.validate(lightgbmlib.LGBM_BoosterFree(boosterPtr), "Finalize Booster")
      boosterPtr = null
    }
  }

  override protected def finalize(): Unit = {
    freeNativeMemory()
    super.finalize()
  }
}

/** Represents a LightGBM Booster learner
  * @param model The string serialized representation of the learner
  */
@SerialVersionUID(777L)
class LightGBMBooster(val model: String) extends Serializable {
  /** Transient variable containing local machine's pointer to native booster
    */
  @transient
  lazy val boosterHandler: BoosterHandler = {
    new BoosterHandler(model)
  }

  def score(features: Vector, raw: Boolean, classification: Boolean): Array[Double] = {
    val kind =
      if (raw) boosterHandler.rawScoreConstant
      else boosterHandler.normalScoreConstant
    features match {
      case dense: DenseVector => predictScoreForMat(dense.toArray, kind, classification)
      case sparse: SparseVector => predictScoreForCSR(sparse, kind, classification)
    }
  }

  def predictLeaf(features: Vector): Array[Double] = {
    features match {
      case dense: DenseVector => predictLeafForMat(dense.toArray)
      case sparse: SparseVector => predictLeafForCSR(sparse)
    }
  }

  lazy val numClasses: Int = boosterHandler.numClasses

  lazy val numFeatures: Int = boosterHandler.numFeatures

  lazy val numTotalModel: Int = boosterHandler.numTotalModel

  lazy val numModelPerIteration: Int = boosterHandler.numTotalModelPerIteration

  lazy val numIterations: Int = numTotalModel / numModelPerIteration

  protected def predictScoreForCSR(sparseVector: SparseVector, kind: Int, classification: Boolean): Array[Double] = {
    val numCols = sparseVector.size

    val datasetParams = "max_bin=255"
    val dataInt32bitType = boosterHandler.dataInt32bitType
    val data64bitType = boosterHandler.data64bitType

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForCSRSingle(
        sparseVector.indices, sparseVector.values,
        sparseVector.numNonzeros,
        boosterHandler.boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
        kind, -1, datasetParams,
        boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr), "Booster Predict")

    predScoreToArray(classification, boosterHandler.scoredDataOutPtr.get().ptr, kind)
  }

  protected def predictScoreForMat(row: Array[Double], kind: Int, classification: Boolean): Array[Double] = {
    val data64bitType = boosterHandler.data64bitType

    val numCols = row.length
    val isRowMajor = 1

    val datasetParams = "max_bin=255"

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForMatSingle(
        row, boosterHandler.boosterPtr, data64bitType,
        numCols,
        isRowMajor, kind,
        -1, datasetParams, boosterHandler.scoredDataLengthLongPtr.get().ptr, boosterHandler.scoredDataOutPtr.get().ptr),
      "Booster Predict")
    predScoreToArray(classification, boosterHandler.scoredDataOutPtr.get().ptr, kind)
  }

  protected def predictLeafForCSR(sparseVector: SparseVector): Array[Double] = {
    val numCols = sparseVector.size

    val datasetParams = "max_bin=255 is_pre_partition=True"
    val dataInt32bitType = boosterHandler.dataInt32bitType
    val data64bitType = boosterHandler.data64bitType

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForCSRSingle(
        sparseVector.indices, sparseVector.values,
        sparseVector.numNonzeros,
        boosterHandler.boosterPtr, dataInt32bitType, data64bitType, 2, numCols,
        boosterHandler.leafIndexPredictConstant, -1, datasetParams,
        boosterHandler.leafIndexDataLengthLongPtr.get().ptr,
        boosterHandler.leafIndexDataOutPtr.get().ptr), "Booster Predict Leaf")

    predLeafToArray(boosterHandler.leafIndexDataOutPtr.get().ptr)
  }

  protected def predictLeafForMat(row: Array[Double]): Array[Double] = {
    val data64bitType = boosterHandler.data64bitType

    val numCols = row.length
    val isRowMajor = 1

    val datasetParams = "max_bin=255"

    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterPredictForMatSingle(
        row, boosterHandler.boosterPtr, data64bitType,
        numCols,
        isRowMajor, boosterHandler.leafIndexPredictConstant,
        -1, datasetParams, boosterHandler.leafIndexDataLengthLongPtr.get().ptr,
        boosterHandler.leafIndexDataOutPtr.get().ptr),
      "Booster Predict Leaf")

    predLeafToArray(boosterHandler.leafIndexDataOutPtr.get().ptr)
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
    val json = lightgbmlib.LGBM_BoosterDumpModelSWIG(boosterHandler.boosterPtr, 0, 0, 1,
      boosterHandler.dumpModelOutPtr.get().ptr)
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
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterFeatureImportance(boosterHandler.boosterPtr, -1,
        importanceTypeNum, boosterHandler.featureImportanceOutPtr.get().ptr),
      "Booster FeatureImportance")
    (0 until numFeatures).map(lightgbmlib.doubleArray_getitem(boosterHandler.featureImportanceOutPtr.get().ptr, _)).toArray
  }

  private def predScoreToArray(classification: Boolean, scoredDataOutPtr: SWIGTYPE_p_double,
                               kind: Int): Array[Double] = {
    if (classification && numClasses == 1) {
      // Binary classification scenario - LightGBM only returns the value for the positive class
      val pred = lightgbmlib.doubleArray_getitem(scoredDataOutPtr, 0)
      if (kind == boosterHandler.rawScoreConstant) {
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
