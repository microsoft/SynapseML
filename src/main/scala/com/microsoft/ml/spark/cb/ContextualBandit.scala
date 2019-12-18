// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.cb

import com.microsoft.ml.spark.core.env.{InternalWrapper, StreamUtilities}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml.{BaseRegressor, ComplexParamsReadable, Estimator, Model, PredictionModel, Predictor}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, element_at, lit, udf, when, sum, size}
import org.vowpalwabbit.spark.VowpalWabbitExample
import com.microsoft.ml.spark.core.schema.DatasetExtensions._
import org.apache.spark.ml.param.shared.HasProbabilityCol
import org.apache.spark.sql.types.StructType

import scala.math.exp

object ContextualBandit extends DefaultParamsReadable[ContextualBandit]

@InternalWrapper
class ContextualBandit(override val uid: String)
  extends Predictor[Seq[Vector], ContextualBandit, ContextualBanditModel] {

  def this() = this(Identifiable.randomUID("ContextualBandit"))

  val clipProbability = new DoubleParam(this, "clipProbability", "Clip probability at this value")
  def getClipProbability: Double = $(clipProbability)
  def setClipProbability(value: Double): this.type = set(clipProbability, value)

  val probabilityCol = new Param[String](this, "probabilityCol", "Column name of probability of chosen action")
  def getProbabilityCol: String = $(probabilityCol)
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
  setDefault(probabilityCol -> "probability")

  val actionCol = new Param[String](this, "actionCol", "Column name of chosen action")
  def getActionCol: String = $(actionCol)
  def setActionCol(value: String): this.type = set(actionCol, value)
  setDefault(actionCol -> "chosenAction")

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator to reduce too")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)
  def getEstimator: Estimator[_] = ${estimator}

  override def copy(extra: ParamMap): ContextualBandit = defaultCopy(extra)

  // TODO: fix validation
  override def transformSchema(schema: StructType): StructType = schema

  override protected def train(dataset: Dataset[_]): ContextualBanditModel = {
    // for MTR training the selected action is enough

    // eventSum / sum(#actions)
    val averageNumActionsInv = dataset.count().toDouble /
      dataset.select(sum(size(col(getFeaturesCol)))).collect()(0).getLong(0)

//    println(s"Average num actions: ${1.0 / averageNumActionsInv}")

    val stage1 = if (!get(clipProbability).isEmpty)
      // implement probability clipping
      dataset.withColumn("CB_prob",
        when(col(getProbabilityCol) > lit(getClipProbability), lit(getClipProbability))
          .otherwise(col(getProbabilityCol)))
      else
        dataset.withColumn("CB_prob", col(getProbabilityCol))

    val stage2 = stage1
      .withColumn("CB_weight", lit(1.0) / col("CB_prob") * lit(averageNumActionsInv))

    // extract selected action
    val stage3 = stage2.withColumn("CB_features", element_at(col(getFeaturesCol), col(getActionCol)))

//    stage3.printSchema
//    stage3.show(5, false)

    val est = getEstimator
    val model = est
      // TODO: use HasFeaturesCol, HasLabelCol, HasWeightCol as estimator constraint
      .set(est.getParam("featuresCol"), "CB_features")
      .set(est.getParam("labelCol"), getLabelCol)
      .set(est.getParam("weightCol"), "CB_weight")
      .fit(stage3)

    new ContextualBanditModel(uid)
      .setModel(model.asInstanceOf[Model[_]])
      .setFeaturesCol(getFeaturesCol)
  }
}

trait ExplorationStrategy {
  def generate(actionScores: Seq[Double]): Seq[Double]
}

class PassThrough extends ExplorationStrategy with Serializable {
  def generate(actionScores: Seq[Double]): Seq[Double] = actionScores
}

class EpsilonGreedy(val epsilon: Double) extends ExplorationStrategy with Serializable {
  def generate(actionScores: Seq[Double]): Seq[Double] = {
    // see generate_epsilon_greedy
    val prob = epsilon / actionScores.length
    val pmf = Array.fill[Double](actionScores.length)(prob)

    val bestActionIndex = actionScores.zipWithIndex.maxBy(_._1)._2
    pmf(bestActionIndex) += 1.0 - epsilon

    pmf
  }
}

class SoftMax(val lambda: Double) extends ExplorationStrategy with Serializable {
  def generate(actionScores: Seq[Double]): Seq[Double] = {
    val maxScore = if (lambda > 0) actionScores.max else actionScores.min

    val pmf = actionScores.map(s => exp(lambda  * (s - maxScore)))

    // normalize
    val sum = pmf.sum
    pmf.map(_ / sum)
  }
}

// TODO: enforce minimum prob

@InternalWrapper
class ContextualBanditModel(override val uid: String)
extends PredictionModel[Seq[Vector], ContextualBanditModel]
  with HasProbabilityCol
{
  val model: Param[Model[_]] = new Param(this, "model", "model")
  def setModel(value: Model[_]): this.type = set(model, value)
  def getModel: Model[_] = ${model}

  val explorationStrategy: Param[ExplorationStrategy] = new Param(this, "explorationStrategy", "exploration strategy")
  def setExplorationStrategy(value: ExplorationStrategy): this.type = set(explorationStrategy, value)
  def getExplorationStrategy: ExplorationStrategy = ${explorationStrategy}

  override def transform(dataset: Dataset[_]): DataFrame = {

    // support both predict(Row/Vector) - need Row for namespace interactions

    // TODO: how to deal with shared and multiple namespaces

    // apply underlying model to each action
    // apply exploration strategy (scores -> probabilities)
    val predictiveModel = getModel.asInstanceOf[PredictionModel[Vector, _]]
    val predictionUdf = udf {
      (actions: Seq[Vector]) => getExplorationStrategy.generate(actions.map(predictiveModel.predict(_)))
    }

    dataset.toDF.withColumn($(predictionCol), predictionUdf(col($(featuresCol))))
  }

  override def predict(features: Seq[Vector]): Double = {
    throw new NotImplementedError("Not implement")
    // return IPS?
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object ContextualBanditModel extends ComplexParamsReadable[ContextualBanditModel]
