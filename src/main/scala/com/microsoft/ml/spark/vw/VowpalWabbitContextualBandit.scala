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
import org.apache.spark.sql.functions.{col, udf}
import org.vowpalwabbit.spark.VowpalWabbitExample
import com.microsoft.ml.spark.core.schema.DatasetExtensions._
import org.apache.spark.sql.types.StructType
import collection.mutable.Stack
import scala.math.exp

object VowpalWabbitContextualBandit extends DefaultParamsReadable[VowpalWabbitContextualBandit]

@InternalWrapper
class VowpalWabbitContextualBandit(override val uid: String)
  extends Predictor[Row, VowpalWabbitContextualBandit, VowpalWabbitContextualBanditModel]
    with VowpalWabbitBase
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

  val sharedCol = new Param[String](this, "sharedCol", "Column name of shared features")
  def getSharedCol: String = $(sharedCol)
  def setSharedCol(value: String): this.type = set(sharedCol, value)
  setDefault(sharedCol -> "shared")

  protected override def getAdditionalColumns(): Seq[String] = Seq(getActionCol, getProbabilityCol, getSharedCol)

  override def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
    if (getArgs.contains("--ccb_explore_adf"))
      throw new UnsupportedOperationException("TODO")
    else if (getArgs.contains("--cb_explore_adf"))
      createContextualBanditLabelSetter(schema)
    else
      throw new UnsupportedOperationException("Must either be cb_explore_adf or ccb_explore_adf mode")
  }

  private def createContextualBanditLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
    val actionIdx = schema.fieldIndex(getActionCol)
    val labelIdx = schema.fieldIndex(getLabelCol)
    val probabilityIdx = schema.fieldIndex(getProbabilityCol)

    // TODO: update jar
    (row: Row, ex: VowpalWabbitExample, idx: Int) => {
      if (idx == 0)
        ex.setSharedLabel
      else if (row.getInt(actionIdx) == idx)
        // cost = -label (reward)
        ex.setContextualBanditLabel(idx, -row.getDouble(labelIdx), row.getDouble(probabilityIdx))
    }
  }

  // TODO: fix validation
  override def transformSchema(schema: StructType): StructType = schema

  protected override def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ) = {
//    val applyLabel = createLabelSetter(schema)
    val featureColIndices = generateNamespaceInfos(schema)
    val sharedNs = generateNamespaceInfo(schema, getSharedCol)
    val actionColIdx = schema.fieldIndex(getActionCol)
    val labelIdx = schema.fieldIndex(getLabelCol)
    val probabilityIdx = schema.fieldIndex(getProbabilityCol)

    class ExampleStack {
      val stack = new Stack[VowpalWabbitExample]

      def getOrCreateExample(): VowpalWabbitExample = {
        val ex = if (stack.isEmpty)
          ctx.vw.createExample
        else
          stack.pop

        ex.clear
        ex.setDefaultLabel

        ex
      }

      def returnExample(ex: VowpalWabbitExample) = {
//        ex.clear
        stack.push(ex)
      }

      def close() =
        while (stack.nonEmpty)
          stack.pop.close()
    }

    val exampleStack = new ExampleStack

    for (row <- inputRows) {
      val sharedExample = exampleStack.getOrCreateExample
      // transfer label
      sharedExample.setSharedLabel
      // TODO: support multiple shared feature columns
      addFeaturesToExample(row.getAs[Vector](sharedNs.colIdx), sharedExample, sharedNs)

      // transfer actions
      val actions0 = row.getAs[Seq[Vector]](featureColIndices(0).colIdx)

      // each features column is a Seq[Vector]
      // first index  ... namespaces
      // second index ... actions
      val actionFeatures = featureColIndices.map(ns => row.getAs[Seq[Vector]](ns.colIdx).toArray)

      val selectedActionIdx = row.getInt(actionColIdx) - 1

      // loop over actions
      val examples = (for (actionIdx <- 0 until actions0.length) yield {
        val ex = exampleStack.getOrCreateExample

        if (actionIdx == selectedActionIdx) {
          ex.setContextualBanditLabel(actionIdx + 1, -row.getDouble(labelIdx), row.getDouble(probabilityIdx))
        }

        // loop over namespaces
        for ((ns, i) <- featureColIndices.zipWithIndex) {
          val features = actionFeatures(i)(actionIdx)

          addFeaturesToExample(features, ex, ns)
        }

        ex
      }).toArray // make sure it materializes

      // signal end of multi-line
      val newLineEx = exampleStack.getOrCreateExample
      newLineEx.makeEmpty

      val allExamples = sharedExample +: examples :+ newLineEx

//      val exStr = allExamples.mkString("\n")
//      println(s"Examples: $exStr")
      ctx.vw.learn(allExamples)

      for (ex <- allExamples)
        exampleStack.returnExample(ex)
    }
  }

  override protected def train(dataset: Dataset[_]): VowpalWabbitContextualBanditModel = {
    val model = new VowpalWabbitContextualBanditModel(uid)
      .setFeaturesCol(getFeaturesCol)
      .setAdditionalFeatures(getAdditionalFeatures)
      .setPredictionCol(getPredictionCol)

    trainInternal(dataset, model)
  }

  override def copy(extra: ParamMap): VowpalWabbitContextualBandit = defaultCopy(extra)
}

// Preparation for multi-class learning, though it no fun as numClasses is spread around multiple reductions
@InternalWrapper
class VowpalWabbitContextualBanditModel(override val uid: String)
  extends PredictionModel[Row, VowpalWabbitContextualBanditModel]
    with VowpalWabbitBaseModel {

//  override def transform(dataset: Dataset[_]): DataFrame = {
//    val df = transformImplInternal(dataset)
//    throw new UnsupportedOperationException("asdf")
//    // which mode one wants to use depends a bit on how this should be deployed
//    // 1. if you stay in spark w/o link=logistic is probably more convenient as it also returns the raw prediction
//    // 2. if you want to export the model *and* get probabilities at scoring term w/ link=logistic is preferable
//
//    // convert raw prediction to probability (if needed)
////    val probabilityUdf = if (vwArgs.getArgs.contains("--link logistic"))
////      udf { (pred: Double) => Vectors.dense(Array(1 - pred, pred)) }
////    else
////      udf { (pred: Double) => {
////        val prob = 1.0 / (1.0 + exp(-pred))
////        Vectors.dense(Array(1 - prob, prob))
////      } }
////
////    val df2 = df.withColumn($(probabilityCol), probabilityUdf(col($(rawPredictionCol))))
////
////    // convert probability to prediction
////    val probability2predictionUdf = udf(probability2prediction _)
////    df2.withColumn($(predictionCol), probability2predictionUdf(col($(probabilityCol))))
//  }

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
