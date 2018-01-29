// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_void, lightgbmlib, lightgbmlibConstants}
import org.apache.spark.TaskContext
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

  val parallelism = StringParam(this, "parallelism",
    "Tree learner parallelism, can be set to data_parallel or voting_parallel", "data_parallel")

  def getParallelism: String = $(parallelism)
  def setParallelism(value: String): this.type = set(parallelism, value)

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
    val spark = processedData.sparkSession.sparkContext
    val nodesKeys = spark.getExecutorMemoryStatus.keys
    val numNodes = dataset.rdd.getNumPartitions
    val driverHost = spark.getConf.get("spark.driver.host")
    val keysWithoutDriver = nodesKeys.filter(key => key != driverHost)
    val executors =
      if (keysWithoutDriver.isEmpty) {
        // Running in local mode
        List.fill(numNodes)(driverHost)
      } else {
        keysWithoutDriver
      }
    val nodes = executors.zipWithIndex
      .map(node => (node._1.split(":")(0) + ":" + (LightGBM.defaultLocalListenPort + node._2))).mkString(",")
    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]

    val lightGBMBooster = processedData
      .mapPartitions(TrainUtils.trainLightGBM(nodes, numNodes, getLabelCol, featuresColumn, getParallelism))(encoder)
      .first()
    new LightGBMModel(uid, featurizedModel, lightGBMBooster)
  }

  override def copy(extra: ParamMap): Estimator[LightGBMModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMUtils {
  def validate(result: Int, component: String): Unit = {
    if (result == -1) {
      throw new Exception(component + " call failed in LightGBM with error: " + lightgbmlib.LGBM_GetLastError())
    }
  }
}

private object TrainUtils extends java.io.Serializable {
  def translate(labelColumn: String, featuresColumn: String, parallelism: String, inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    if (!inputRows.hasNext)
      List[LightGBMBooster]().toIterator
    val rows = inputRows.toArray
    val numRows = rows.length
    val numRowsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numRowsIntPtr, numRows)
    val numRows_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numRowsIntPtr)
    val rowsAsDoubleArray = rows.map(row => (row.get(row.fieldIndex(featuresColumn)) match {
      case dense: DenseVector => dense.toArray
      case sparse: SparseVector => sparse.toDense.toArray
    }, row.getInt(row.fieldIndex(labelColumn))))
    val numCols = rowsAsDoubleArray.head._1.length
    val data = lightgbmlib.new_doubleArray(numCols * numRows)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
      ri._1._1.zipWithIndex.foreach(value =>
        lightgbmlib.doubleArray_setitem(data, value._2 + (ri._2 * numCols), value._1)))
    val dataAsVoidPtr = lightgbmlib.double_to_voidp_ptr(data)
    val isRowMajor = 1
    val numColsIntPtr = lightgbmlib.new_intp()
    lightgbmlib.intp_assign(numColsIntPtr, numCols)
    val numCols_int32_tPtr = lightgbmlib.int_to_int32_t_ptr(numColsIntPtr)
    val datasetOutPtr = lightgbmlib.voidpp_handle()
    val datasetParams = "max_bin=255 is_pre_partition=True"
    val data64bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT64

    // Generate the dataset for features
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetCreateFromMat(dataAsVoidPtr, data64bitType,
      numRows_int32_tPtr, numCols_int32_tPtr, isRowMajor, datasetParams, null, datasetOutPtr), "Dataset create")
    val datasetPtr = lightgbmlib.voidpp_value(datasetOutPtr)

    // Validate num rows
    val numDataPtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumData(datasetPtr, numDataPtr), "DatasetGetNumData")
    val numData = lightgbmlib.intp_value(numDataPtr)
    if (numData <= 0) {
      throw new Exception("Unexpected num data: " + numData)
    }

    // Validate num cols
    val numFeaturePtr = lightgbmlib.new_intp()
    LightGBMUtils.validate(lightgbmlib.LGBM_DatasetGetNumFeature(datasetPtr, numFeaturePtr), "DatasetGetNumFeature")
    val numFeature = lightgbmlib.intp_value(numFeature)
    if (numFeature <= 0) {
      throw new Exception("Unexpected num feature: " + numFeature)
    }

    // Generate the label column and add to dataset
    val labelColArray = lightgbmlib.new_floatArray(numRows)
    rowsAsDoubleArray.zipWithIndex.foreach(ri =>
       lightgbmlib.floatArray_setitem(labelColArray, ri._2, ri._1._2.toFloat))
    val labelAsVoidPtr = lightgbmlib.float_to_voidp_ptr(labelColArray)
    val data32bitType = lightgbmlibConstants.C_API_DTYPE_FLOAT32
    LightGBMUtils.validate(
      lightgbmlib.LGBM_DatasetSetField(datasetPtr, "label", labelAsVoidPtr, numRows, data32bitType), "DatasetSetField")

    // Create the booster
    val boosterOutPtr = lightgbmlib.voidpp_handle()
    val parameters = "is_pre_partition=True tree_learner=" + parallelism + " boosting_type=gbdt " +
      "objective=binary metric=binary_logloss,auc num_trees=10 learning_rate=0.1 num_leaves=5"
    LightGBMUtils.validate(lightgbmlib.LGBM_BoosterCreate(datasetPtr, parameters, boosterOutPtr), "Booster")
    val boosterPtr = lightgbmlib.voidpp_value(boosterOutPtr)
    val isFinishedPtr = lightgbmlib.new_intp()
    var isFinised = 0
    var iters = 0
    while (isFinised == 0 && iters < 10) {
      val result = lightgbmlib.LGBM_BoosterUpdateOneIter(boosterPtr, isFinishedPtr)
      LightGBMUtils.validate(result, "Booster Update One Iter")
      isFinised = lightgbmlib.intp_value(isFinishedPtr)
      println("Running iteration: " + iters + " with result: " + result + " and is finished: " + isFinised)
      iters = iters + 1
    }
    val bufferLength = 10000
    val bufferLengthPtr = lightgbmlib.new_longp()
    lightgbmlib.longp_assign(bufferLengthPtr, bufferLength)
    val bufferLengthPtrInt64 = lightgbmlib.long_to_int64_t_ptr(bufferLengthPtr)
    val bufferOutLengthPtr = lightgbmlib.new_int64_tp()
    val model = lightgbmlib.LGBM_BoosterSaveModelToStringSWIG(boosterPtr, -1, bufferLengthPtrInt64, bufferOutLengthPtr)
    // Finalize network when done
    LightGBMUtils.validate(lightgbmlib.LGBM_NetworkFree(), "Finalize network")
    println("Buffer length:" + lightgbmlib.longp_value(lightgbmlib.int64_t_to_long_ptr(bufferOutLengthPtr)))
    List[LightGBMBooster](new LightGBMBooster(model)).toIterator
  }

  def trainLightGBM(nodes: String, numNodes: Int, labelColumn: String, featuresColumn: String, parallelism: String)
    (inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    // Initialize the native library
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm")
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm_swig")
    // Initialize the network communication
    val partitionId = TaskContext.getPartitionId()
    val localListenPort = LightGBM.defaultLocalListenPort + partitionId
    val result = lightgbmlib.LGBM_NetworkInit(nodes, localListenPort, LightGBM.defaultListenTimeout,
      numNodes)
    if (result != 0) {
      throw new Exception("Network initialization of LightGBM failed")
    }
    translate(labelColumn, featuresColumn, parallelism, inputRows)
  }
}

/** Model produced by [[LightGBM]]. */
class LightGBMModel(val uid: String, featurizer: PipelineModel, model: LightGBMBooster)
    extends Model[LightGBMModel] with ConstructorWritable[LightGBMModel] {

  val ttag: TypeTag[LightGBMModel] = typeTag[LightGBMModel]
  val objectsToSave: List[AnyRef] = List(uid, featurizer, model)

  override def copy(extra: ParamMap): LightGBMModel =
    new LightGBMModel(uid, featurizer, model)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // lightgbmlib.LGBM_BoosterLoadModelFromString()
    dataset.toDF()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMModel extends ConstructorReadable[LightGBMModel]

