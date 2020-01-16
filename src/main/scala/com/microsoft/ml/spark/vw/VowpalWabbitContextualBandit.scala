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

import collection.mutable.Stack
import scala.math.exp

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

  val actionCol = new Param[String](this, "actionCol", "Column name of chosen action")
  def getActionCol: String = $(actionCol)
  def setActionCol(value: String): this.type = set(actionCol, value)
  setDefault(actionCol -> "chosenAction")

  protected override def getAdditionalColumns(): Seq[String] = Seq(getActionCol, getProbabilityCol, getSharedCol)

//  override def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
//    if (getArgs.contains("--ccb_explore_adf"))
//      throw new UnsupportedOperationException("TODO")
//    else if (getArgs.contains("--cb_explore_adf"))
//      createContextualBanditLabelSetter(schema)
//    else
//      throw new UnsupportedOperationException("Must either be cb_explore_adf or ccb_explore_adf mode")
//  }
//
//  private def createContextualBanditLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
//    val actionIdx = schema.fieldIndex(getActionCol)
//    val labelIdx = schema.fieldIndex(getLabelCol)
//    val probabilityIdx = schema.fieldIndex(getProbabilityCol)
//
//    // TODO: update jar
//    (row: Row, ex: VowpalWabbitExample, idx: Int) => {
//      if (idx == 0)
//        ex.setSharedLabel
//      else if (row.getInt(actionIdx) == idx)
//        // cost = -label (reward)
//        ex.setContextualBanditLabel(idx, -row.getDouble(labelIdx), row.getDouble(probabilityIdx))
//    }
//  }

  // TODO: fix validation
  override def transformSchema(schema: StructType): StructType = schema

  protected override def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ) = {
    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      schema,
      getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    val sharedNs = VowpalWabbitUtil.generateNamespaceInfo(schema, getHashSeed, getSharedCol)
    val actionColIdx = schema.fieldIndex(getActionCol)
    val labelIdx = schema.fieldIndex(getLabelCol)
    val probabilityIdx = schema.fieldIndex(getProbabilityCol)

    val exampleStack = new ExampleStack(ctx.vw)

    for (row <- inputRows) {
      VowpalWabbitUtil.prepareMultilineExample(row, featureColIndices, sharedNs, ctx.vw, exampleStack,
        examples => {
          // it's one-based but we need to skip the shared example anyway
          val selectedActionIdx = row.getInt(actionColIdx)

          // set the label for learning
          examples(selectedActionIdx).setContextualBanditLabel(
            selectedActionIdx,
            -row.getDouble(labelIdx),
            row.getDouble(probabilityIdx))

          // learn from the examples
          ctx.vw.learn(examples)
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
  }

  override def copy(extra: ParamMap): VowpalWabbitContextualBandit = defaultCopy(extra)
}

@InternalWrapper
class VowpalWabbitContextualBanditModel(override val uid: String)
  extends PredictionModel[Row, VowpalWabbitContextualBanditModel]
    with VowpalWabbitBaseModel with VowpalWabbitContextualBanditBase {

  override def transformSchema(schema: StructType): StructType = schema

  lazy val exampleStack = new ExampleStack(vw)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val featureColumnNames = Seq(getFeaturesCol) ++ getAdditionalFeatures + getSharedCol
    val featureColumns = dataset.schema.filter({ f => featureColumnNames.contains(f.name) })

    val featureSchema = StructType(featureColumns)

    val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
      featureSchema,
      vwArgs.getHashSeed,
      Seq(getFeaturesCol) ++ getAdditionalFeatures)

    val sharedNs = VowpalWabbitUtil.generateNamespaceInfo(featureSchema, getHashSeed, getSharedCol)

    val predictUDF = udf { (row: Row) => {
      VowpalWabbitUtil.prepareMultilineExample(row, featureColIndices, sharedNs, vw, exampleStack,
        examples =>
          vw.predict(examples)
            .asInstanceOf[vowpalWabbit.responses.ActionProbs]
            .getActionProbs
            .sortBy { _.getAction }
            .map { _.getProbability.toDouble }
        )
    }}

    dataset.withColumn(
      $(predictionCol),
      predictUDF(struct(featureColumns.map(f => col(f.name)): _*)))
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
