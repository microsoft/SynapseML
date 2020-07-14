// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.vw

import com.microsoft.ml.spark.core.env.{InternalWrapper, StreamUtilities}
import com.microsoft.ml.spark.core.schema.DatasetExtensions
import org.apache.spark.ml.{ComplexParamsReadable, PredictionModel, Predictor}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, struct, udf}
import org.vowpalwabbit.spark.{VowpalWabbitExample, VowpalWabbitNative}
import com.microsoft.ml.spark.core.schema.DatasetExtensions._
import org.apache.spark.sql.types.StructType
import vowpalWabbit.responses.ActionProbs

import collection.mutable.Stack
import scala.math.exp
import scala.math.max

object VowpalWabbitContextualBandit extends DefaultParamsReadable[VowpalWabbitContextualBandit]

class ExampleStack(val vw: VowpalWabbitNative) {
  val stack = new Stack[VowpalWabbitExample]

  def getOrCreateExample(): VowpalWabbitExample = {
    val ex = if (stack.isEmpty)
      vw.createExample
    else
      stack.pop

    ex.clear
    ex.setDefaultLabel

    ex
  }

  def returnExample(ex: VowpalWabbitExample): Unit = {
    //        ex.clear
    stack.push(ex)
  }

  def close(): Unit =
    while (stack.nonEmpty)
      stack.pop.close()
}

// https://github.com/VowpalWabbit/estimators/blob/master/ips_snips.py
class Estimator
{
  var n: Float = 0
  var N: Float = 0
  var d: Float = 0
  var Ne: Float = 0
  var c: Float = 0

  def add_example(p_log: Float, r:  Float, p_pred: Float, count: Int = 1): Unit = {
    N += count
    if (p_pred > 0) {
      val p_over_p = p_pred/p_log
      d += p_over_p*count
      Ne += count
      if (r != 0) {
          n += r*p_over_p*count
          c = max(c, r*p_over_p)
      }
    }
  }

  def get_snips_estimate(): Float = {
    n/N
  }

  def get_ips_estimate: Float = {
    n/d
  }
}

@InternalWrapper
trait VowpalWabbitContextualBanditBase extends VowpalWabbitBase {
  val sharedCol = new Param[String](this, "sharedCol", "Column name of shared features")
  def getSharedCol: String = $(sharedCol)
  def setSharedCol(value: String): this.type = set(sharedCol, value)
  setDefault(sharedCol -> "shared")
}

@InternalWrapper
class VowpalWabbitContextualBandit(override val uid: String)
  extends Predictor[Row, VowpalWabbitContextualBandit, VowpalWabbitContextualBanditModel]
    with VowpalWabbitContextualBanditBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitContextualBandit"))

  val probabilityCol = new Param[String](this, "probabilityCol", "Column name of probability of chosen action")
  def getProbabilityCol: String = $(probabilityCol)
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
  setDefault(probabilityCol -> "probability")

  val chosenActionCol = new Param[String](this, "chosenActionCol", "Column name of chosen action")
  def getChosenActionCol: String = $(chosenActionCol)
  def setChosenActionCol(value: String): this.type = set(chosenActionCol, value)
  setDefault(chosenActionCol -> "chosenAction")

  val additionalSharedFeatures = new StringArrayParam(this, "additionalSharedFeatures", "Additional feature columns")
  def getAdditionalSharedFeatures: Array[String] = $(additionalSharedFeatures)
  def setAdditionalSharedFeatures(value: Array[String]): this.type = set(additionalSharedFeatures, value)

  val estimator: Estimator = new Estimator

  // Used in the base class to remove unneeded columns
  protected override def getAdditionalColumns(): Seq[String] = Seq(getChosenActionCol, getProbabilityCol, getSharedCol)

  // TODO: fix validation
  override def transformSchema(schema: StructType): StructType = schema

  protected override def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ) = {
    val allActionFeatureColumns = Seq(getFeaturesCol) ++ getAdditionalFeatures
    val allSharedFeatureColumns = Seq(getSharedCol) ++ getAdditionalSharedFeatures

    val actionNamespaceInfos = VowpalWabbitUtil.generateNamespaceInfos(
      schema,
      getHashSeed,
      allActionFeatureColumns)

    val sharedNamespaceInfos = VowpalWabbitUtil.generateNamespaceInfos(schema, getHashSeed, allSharedFeatureColumns);
    val chosenActionColIdx = schema.fieldIndex(getChosenActionCol)
    val labelIdx = schema.fieldIndex(getLabelCol)
    val probabilityIdx = schema.fieldIndex(getProbabilityCol)

    val exampleStack = new ExampleStack(ctx.vw)

    for (row <- inputRows) {
      VowpalWabbitUtil.prepareMultilineExample(row, actionNamespaceInfos, sharedNamespaceInfos, ctx.vw, exampleStack,
        examples => {
          // it's one-based but we need to skip the shared example anyway
          val selectedActionIdx = row.getInt(chosenActionColIdx)
          val reward = row.getDouble(labelIdx)
          val cost = -1 * reward
          val loggedProbability = row.getDouble(probabilityIdx)

          // Set the label for learning
          examples(selectedActionIdx).setContextualBanditLabel(
            selectedActionIdx,
            cost,
            loggedProbability)

          // Learn from the examples
          ctx.vw.learn(examples)
          val pred: ActionProbs = examples(0).getPrediction.asInstanceOf[ActionProbs]
          println(pred)
          val probs = pred.getActionProbs

          val selectedActionIdxZeroBased = selectedActionIdx-1

          estimator.add_example(loggedProbability.asInstanceOf[Float], cost.asInstanceOf[Float], probs.apply(selectedActionIdxZeroBased).getProbability)
        })
    }
   }

  override protected def train(dataset: Dataset[_]): VowpalWabbitContextualBanditModel = {
    val model = new VowpalWabbitContextualBanditModel(uid)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
      .setSharedCol(getSharedCol)
      .setPredictionCol(getPredictionCol)

    trainInternal(dataset, model)
    model.estimator = estimator
    model
  }

  override def copy(extra: ParamMap): VowpalWabbitContextualBandit = defaultCopy(extra)
}

@InternalWrapper
class VowpalWabbitContextualBanditModel(override val uid: String)
  extends PredictionModel[Row, VowpalWabbitContextualBanditModel]
    with VowpalWabbitBaseModel with VowpalWabbitContextualBanditBase {

  override def transformSchema(schema: StructType): StructType = schema


  lazy val exampleStack = new ExampleStack(vw)

  var estimator: Estimator = _
  def get_estimate() : Float = {
   estimator.get_ips_estimate
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
//    val featureColumnNames = Seq(getFeaturesCol) ++ getAdditionalFeatures + getSharedCol
//    val featureColumns = dataset.schema.filter({ f => featureColumnNames.contains(f.name) })
//
//    val featureSchema = StructType(featureColumns)
//
//    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
//      featureSchema,
//      vwArgs.getHashSeed,
//      Seq(getFeaturesCol) ++ getAdditionalFeatures)
//
//    val sharedNs = VowpalWabbitUtil.generateNamespaceInfo(featureSchema, getHashSeed, getSharedCol)
//
//    val predictUDF = udf { (row: Row) => {
//      VowpalWabbitUtil.prepareMultilineExample(row, featureColIndices, sharedNs, vw, exampleStack,
//        examples =>
//          vw.predict(examples)
//            .asInstanceOf[vowpalWabbit.responses.ActionProbs]
//            .getActionProbs
//            .sortBy { _.getAction }
//            .map { _.getProbability.toDouble }
//        )
//    }}
//
//    dataset.withColumn(
//      $(predictionCol),
//      predictUDF(struct(featureColumns.map(f => col(f.name)): _*)))
    dataset.toDF
  }

  protected override def transformImpl(dataset: Dataset[_]): DataFrame = {
    transformImplInternal(dataset)
      .withColumn($(predictionCol), col($(rawPredictionCol)))
  }

  override def predict(features: Row): Double = {
    throw new NotImplementedError("Not implement")
    // return IPS?
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VowpalWabbitContextualBanditModel extends ComplexParamsReadable[VowpalWabbitContextualBanditModel]
