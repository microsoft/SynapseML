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

/**
  * Represents a LightGBM Booster learner
  * @param model
  */
class LightGBMBooster(val model: SWIGTYPE_p_void) extends Serializable {
  def mergeModels(newModel: LightGBMBooster): LightGBMBooster = {
    // Merges models to first handle
    if (model != null)
      lightgbmlib.LGBM_BoosterMerge(model, newModel.model)
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
    val lightGBMBooster = processedData.mapPartitions(LightGBMUtils.trainLightGBM(nodes, numNodes))(encoder)
      .reduce((booster1, booster2) => booster1.mergeModels(booster2))
    // Run map partitions to train and merge models
    new LightGBMModel(uid, lightGBMBooster) // pass in featurizeModel
  }

  override def copy(extra: ParamMap): Estimator[LightGBMModel] = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

private object LightGBMUtils extends java.io.Serializable {
  def initialize(inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    new Iterator[LightGBMBooster] {
      def hasNext: Boolean = inputRows.hasNext

      def next(): LightGBMBooster = {
        // Get all the rows and translate to LightGBM dataset
        inputRows.next()
        new LightGBMBooster(null)
      }
    }
  }

  def trainLightGBM(nodes: String, numNodes: Int)(inputRows: Iterator[Row]): Iterator[LightGBMBooster] = {
    // Initialize the native library
    new NativeLoader("/com/microsoft/ml/lightgbm").loadLibraryByName("_lightgbm")
    // Initialize the network communication
    lightgbmlib.LGBM_NetworkInit(nodes, LightGBM.defaultListenTimeout, LightGBM.defaultLocalListenPort, numNodes)
    initialize(inputRows)
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
