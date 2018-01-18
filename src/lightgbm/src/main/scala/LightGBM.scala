// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_void, _}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeTag}

object LightGBM extends DefaultParamsReadable[LightGBM] {
  /**
    * The default port for LightGBM network initialization
    */
  val defaultLocalListenPort = 12400
  /**
    * The default timeout for LightGBM network initialization
    */
  val defaultListenTimeout = 120
}

class LightGBMBooster(val isEmpty: Boolean, val model: SWIGTYPE_p_void) {
  def mergeModels(newModel: SWIGTYPE_p_void): LightGBMBooster = {
    if (isEmpty) {
      new LightGBMBooster(false, newModel)
    } else {
      // Merges models to first handle
      lightgbmlib.LGBM_BoosterMerge(model, newModel)
      new LightGBMBooster(false, model)
    }
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
    // Run a parallel job via map partitions to initialize the native library and network
    LightGBMUtils.initialize(processedData.rdd, nodes, numNodes)
    // Run map partitions to train and merge models
    new LightGBMModel(uid) // pass in featurizeModel
  }

  override def copy(extra: ParamMap): Estimator[LightGBMModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

private object LightGBMUtils extends java.io.Serializable {
  private[spark] def initialize[T: ClassTag](rdd: RDD[T], nodes: String, numNodes: Int): RDD[T] = {
    def perPartition(it: Iterator[T]):Iterator[T] = {
      // Initialize the native library
      new NativeLoader("/com/microsoft/ml/lightgbm/lib.linux").loadLibraryByName("lib_lightgbm.so")
      // Initialize the network communication
      lightgbmlib.LGBM_NetworkInit(nodes, LightGBM.defaultListenTimeout, LightGBM.defaultLocalListenPort, numNodes)
      it
    }
    rdd.mapPartitions(perPartition, preservesPartitioning = true)
  }
}

/** Model produced by [[LightGBM]]. */
class LightGBMModel(val uid: String)
    extends Model[LightGBMModel] with ConstructorWritable[LightGBMModel] {

  val ttag: TypeTag[LightGBMModel] = typeTag[LightGBMModel]
  val objectsToSave: List[AnyRef] = List(uid)

  override def copy(extra: ParamMap): LightGBMModel =
    new LightGBMModel(uid)

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

object LightGBMModel extends ConstructorReadable[LightGBMModel]
