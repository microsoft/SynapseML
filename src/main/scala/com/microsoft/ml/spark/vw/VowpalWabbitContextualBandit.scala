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

import scala.math.exp

object VowpalWabbitContextualBandit extends DefaultParamsReadable[VowpalWabbitContextualBandit]

@InternalWrapper
class VowpalWabbitContextualBandit(override val uid: String)
  extends Predictor[Row, VowpalWabbitContextualBandit, VowpalWabbitContextualBanditModel]
    with VowpalWabbitBase
{
  def this() = this(Identifiable.randomUID("VowpalWabbitContextualBandit"))

  override def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
    if (getArgs.contains("--ccb_explore_adf"))
      throw new UnsupportedOperationException("TODO")
    else if (getArgs.contains("--cb_explore_adf"))
      createContextualBanditLabelSetter(schema)
    else
      throw new UnsupportedOperationException("Must either be cb_explore_adf or ccb_explore_adf mode")
  }

  private def createContextualBanditLabelSetter(schema: StructType): (Row, VowpalWabbitExample, Int) => Unit = {
    val labelIdx = schema.fieldIndex("_labelIndex")
    val costIdx = schema.fieldIndex("_label_cost")
    val probabilityIdx = schema.fieldIndex("_label_probability")

    (row: Row, ex: VowpalWabbitExample, idx: Int) => {
      if (idx == 0)
        ex.setSharedLabel
      else if (row.getInt(labelIdx) == idx)
        ex.setContextualBanditLabel(idx, row.getDouble(costIdx), row.getDouble(probabilityIdx))
    }
  }

  protected override def trainRow(schema: StructType,
                         inputRows: Iterator[Row],
                         ctx: TrainContext
                        ) = {
    val applyLabel = createLabelSetter(schema)
    val featureColIndices = generateNamespaceInfos(schema)
    val contextIdx = schema.fieldIndex("c")

    // TODO: shared features column
    // TODO: actions (e.g. Array(Row[Vector])

    StreamUtilities.using(ctx.vw.createExample()) { ex =>
      for (row <- inputRows) {
        ctx.nativeIngestTime.measure {

          // TODO: go on here
          // transfer label
          applyLabel(row, ex, 0)

          // transfer features
          for (ns <- featureColIndices)
            row.get(ns.colIdx) match {
              case dense: DenseVector => ex.addToNamespaceDense(ns.featureGroup,
                ns.hash, dense.values)
              case sparse: SparseVector => ex.addToNamespaceSparse(ns.featureGroup,
                sparse.indices, sparse.values)
            }
        }

        ctx.learnTime.measure {
          ex.learn()
          ex.clear()
        }
      }
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
