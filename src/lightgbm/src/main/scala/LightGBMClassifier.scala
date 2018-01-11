// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.sql._

import scala.reflect.runtime.universe.{TypeTag, typeTag}

object LightGBMClassifier extends DefaultParamsReadable[LightGBMClassifier] {
  /** The default port for LightGBM network initialization
    */
  val defaultLocalListenPort = 12400
  /** The default timeout for LightGBM network initialization
    */
  val defaultListenTimeout = 120
}

class LightGBMClassifier(override val uid: String)
  extends ProbabilisticClassifier[Vector, LightGBMClassifier, LightGBMClassificationModel]
  with MMLParams {
  def this() = this(Identifiable.randomUID("LightGBMClassifier"))

  val parallelism = StringParam(this, "parallelism",
    "Tree learner parallelism, can be set to data_parallel or voting_parallel", "data_parallel")

  def getParallelism: String = $(parallelism)
  def setParallelism(value: String): this.type = set(parallelism, value)

  /** Trains the LightGBM Classification model.
    *
    * @param dataset The input dataset to train.
    * @return The trained model.
    */
  override protected def train(dataset: Dataset[_]): LightGBMClassificationModel = {
    val df = dataset.toDF()
    df.cache()
    val (nodes, numNodes) = LightGBMUtils.getNodes(dataset)
    /* Run a parallel job via map partitions to initialize the native library and network,
     * translate the data to the LightGBM in-memory representation and train the models
     */
    val encoder = Encoders.kryo[LightGBMBooster]

    println("nodes: " + nodes)
    println("numNodes: " + numNodes)
    val lightGBMBooster = df
      .mapPartitions(TrainUtils.trainLightGBM(nodes, numNodes, getLabelCol, getFeaturesCol, getParallelism))(encoder)
      .first()
    new LightGBMClassificationModel(uid, getFeaturesCol, lightGBMBooster)
  }

  override def copy(extra: ParamMap): LightGBMClassifier = defaultCopy(extra)
}

/** Model produced by [[LightGBMClassifier]]. */
class LightGBMClassificationModel(override val uid: String, featuresColumn: String, model: LightGBMBooster)
  extends ProbabilisticClassificationModel[Vector, LightGBMClassificationModel]
    with ConstructorWritable[LightGBMClassificationModel] {
  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        dv.values(0) = 1.0 / (1.0 + math.exp(-2.0 * dv.values(0)))
        dv.values(1) = 1.0 - dv.values(0)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LightGBMClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def numClasses: Int = model.numClasses()

  override protected def predictRaw(features: Vector): Vector = {
    val prediction = model.scoreRaw(features)
    Vectors.dense(Array(-prediction, prediction))
  }

  override def copy(extra: ParamMap): LightGBMClassificationModel = defaultCopy(extra)

  private val loss = LogLoss
  override val ttag: TypeTag[LightGBMClassificationModel] = typeTag[LightGBMClassificationModel]

  override def objectsToSave: List[Any] = List(uid, featuresColumn, model)
}

object LightGBMClassificationModel extends ConstructorReadable[LightGBMClassificationModel]
