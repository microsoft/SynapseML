// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_void, lightgbmlib, lightgbmlibConstants}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.{TypeTag, typeTag}

object LightGBM extends DefaultParamsReadable[LightGBM] {
  /** The default port for LightGBM network initialization
    */
  val defaultLocalListenPort = 12400
  /** The default timeout for LightGBM network initialization
    */
  val defaultListenTimeout = 120
}

/** Represents a LightGBM Booster learner
  * @param model
  */
class LightGBMBooster(val model: String) extends Serializable {
  protected def getModel(): SWIGTYPE_p_void = {
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    val numIters = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterLoadModelFromString(model, numIters, boosterOutPtr), "Booster load")
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

/** Trains a LightGBM model.
  */
class LightGBM(override val uid: String) extends Estimator[LightGBMModel]
  with HasLabelCol with MMLParams {

  def this() = this(Identifiable.randomUID("LightGBM"))

  val featuresColumn = this.uid + "_features"

  /** Fits the LightGBM model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  override def fit(dataset: Dataset[_]): LightGBMModel = {
    // Featurize the input dataset
    val oneHotEncodeCategoricals = true
    val featuresToHashTo = FeaturizeUtilities.numFeaturesTreeOrNNBased
    val featureColumns = dataset.columns.filter(col => col != getLabelCol).toSeq
    val featurizer = new Featurize()
      .setFeatureColumns(Map(featuresColumn -> featureColumns))
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumberOfFeatures(featuresToHashTo)
    val featurizedModel = featurizer.fit(dataset)
    val processedData = featurizedModel.transform(dataset)
    processedData.cache()

    val nodesKeys = processedData.sparkSession.sparkContext.getExecutorMemoryStatus.keys
    val nodes = nodesKeys.mkString(",")
    val numNodes = nodesKeys.count((node: String) => true)
    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]

    val lightGBMBooster = processedData
      .mapPartitions(TrainUtils.trainLightGBM(nodes, numNodes, featuresColumn))(encoder)
      .reduce((booster1, booster2) => booster1.mergeModels(booster2))
    // Run map partitions to train and merge models
    new LightGBMModel(uid, lightGBMBooster) // pass in featurizeModel
  }

  override def copy(extra: ParamMap): Estimator[LightGBMModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMUtils {
  def validate(result: Int, component: String): Unit = {
    if (result == -1) {
      throw new Exception(component + " create failed in LightGBM with error: " + lightgbmlib.LGBM_GetLastError())
    }
  }
}

private object TrainUtils extends java.io.Serializable {
  def translate(featuresColumn: String, inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    if (!inputRows.hasNext)
      List[LightGBMBooster]().toIterator
    val rows = inputRows.toArray
    val numRows = rows.length
    val numRowsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numRowsIntPtr, numRows)
    val numRows_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numRowsIntPtr)
    val rowsAsDoubleArray = rows.map(row => row.get(row.fieldIndex(featuresColumn)) match {
      case dense: DenseVector => dense.toArray
      case spase: SparseVector => spase.toDense.toArray
    })
    val numCols = rowsAsDoubleArray.head.length
    val data = lightgbmlib.new_doubleArray(numCols * numRows)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
      ri._1.zipWithIndex.foreach(value =>
        lightgbmlib.doubleArray_setitem(data, value._2 + (ri._2 * numCols), value._1)))
    val dataAsVoidPtr = lightgbmlib.double_to_voidp_ptr(data)
    val isRowMajor = 1
    val numColsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numColsIntPtr, numCols)
    val numCols_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numColsIntPtr)
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(dataAsVoidPtr, lightgbmlibConstants.C_API_DTYPE_FLOAT64,
      numRows_int32_tPtr, numCols_int32_tPtr, isRowMajor, "max_bin=15", null, datasetOutPtr), "Dataset create")
    val datasetPtr = lightgbmlib.voidpp_value(datasetOutPtr)
    val numData = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumData(datasetPtr, numData), "DatasetGetNumData")
    println("Num data: " + lightgbmlib.intp_value(numData))
    val numFeature = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumFeature(datasetPtr, numFeature), "DatasetGetNumFeature")
    println("Num features: " + lightgbmlib.intp_value(numFeature))
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterCreate(datasetPtr, "", boosterOutPtr), "Booster")
    val boosterPtr = lightgbmlib.voidpp_value(boosterOutPtr)
    val bufferLength = 1000
    val buffer = new String(new Array[Char](bufferLength))
    val bufferLengthPtr = lightgbmlib.new_longp()
    lightgbmlib.longp_assign(bufferLengthPtr, bufferLength)
    val bufferLengthPtrInt64 = lightgbmlib.long_to_int64_t_ptr(bufferLengthPtr)
    val bufferOutLengthPtr = lightgbmlib.new_int64_tp()
    LightGBMUtils.validate(
      lightgbmlib.LGBM_BoosterSaveModelToString(boosterPtr, -1, bufferLengthPtrInt64, bufferOutLengthPtr, buffer),
      "BoosterSaveModel")
    List[LightGBMBooster](new LightGBMBooster(buffer)).toIterator
  }

  def trainLightGBM(nodes: String, numNodes: Int, featuresColumn: String)
    (inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    // Initialize the native library
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm")
    // Initialize the network communication
    val result = lightgbmlib.LGBM_NetworkInit(nodes, LightGBM.defaultListenTimeout,
      LightGBM.defaultLocalListenPort, numNodes)
    if (result != 0) {
      throw new Exception("Network initialization of LightGBM failed")
    }
    translate(featuresColumn, inputRows)
  }
}

/** Model produced by [[LightGBM]]. */
class LightGBMModel(val uid: String, model: LightGBMBooster)
    extends Model[LightGBMModel] with ConstructorWritable[LightGBMModel] {

  val ttag: TypeTag[LightGBMModel] = typeTag[LightGBMModel]
  val objectsToSave: List[AnyRef] = List(uid)

  override def copy(extra: ParamMap): LightGBMModel =
    new LightGBMModel(uid, model)

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMModel extends ConstructorReadable[LightGBMModel]
