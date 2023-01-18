// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.vowpalwabbit.spark.VowpalWabbitExample

import scala.math.exp

/**
  * VowpalWabbit exposed as SparkML classifier.
  */
class VowpalWabbitClassifier(override val uid: String)
  extends ProbabilisticClassifier[Row, VowpalWabbitClassifier, VowpalWabbitClassificationModel]
    with VowpalWabbitBaseSpark
    with ComplexParamsWritable
    with SynapseMLLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitClassifier"))

  // to support Grid search we need to replicate the parameters here...
  val labelConversion = new BooleanParam(this, "labelConversion",
    "Convert 0/1 Spark ML style labels to -1/1 VW style labels. Defaults to false.")
  setDefault(labelConversion -> false)
  def getLabelConversion: Boolean = $(labelConversion)
  def setLabelConversion(value: Boolean): this.type = set(labelConversion, value)

  val numClasses = new IntParam(this, "numClasses",
    "Number of classes. Defaults to binary. Needs to match oaa/csoaa/multilabel_oaa/...")
  setDefault(numClasses -> 2)
  def getNumClasses: Int = $(numClasses)
  def setNumClasses(value: Int): this.type = set(numClasses, value)

  override protected def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample) => Unit = {
    if (getNumClasses == 2)
      super.createLabelSetter(schema)
    else {
      val labelColIdx = schema.fieldIndex(getLabelCol)

      val weightGetter = getWeightGetter(schema)

      // for Predictors the label is always going to be a Double
      (row: Row, ex: VowpalWabbitExample) =>
        ex.setMulticlassLabel(weightGetter(row), row.getDouble(labelColIdx).toInt)
    }
  }

  override protected def train(dataset: Dataset[_]): VowpalWabbitClassificationModel = {
    logTrain({
      val model = new VowpalWabbitClassificationModel(uid)
        .setFeaturesCol(getFeaturesCol)
        .setAdditionalFeatures(getAdditionalFeatures)
        .setPredictionCol(getPredictionCol)
        .setProbabilityCol(getProbabilityCol)
        .setRawPredictionCol(getRawPredictionCol)
        .setNumClassesModel(getNumClasses)

      val finalDataset = if (!getLabelConversion)
        dataset.toDF
      else {
        val inputLabelCol = dataset.withDerivativeCol("label")
        dataset
          .withColumnRenamed(getLabelCol, inputLabelCol)
          .withColumn(getLabelCol, col(inputLabelCol) * 2 - 1)
          .toDF
      }

      trainInternal(finalDataset, model)
    })
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object VowpalWabbitClassifier extends ComplexParamsReadable[VowpalWabbitClassifier]

class VowpalWabbitClassificationModel(override val uid: String)
  extends ProbabilisticClassificationModel[Row, VowpalWabbitClassificationModel]
    with VowpalWabbitBaseModelSpark
    with ComplexParamsWritable with Wrappable with SynapseMLLogging {
  logClass()

  def this() = this(Identifiable.randomUID("VowpalWabbitClassificationModel"))

  override protected lazy val pyInternalWrapper = true

  // need to name differently from numClasses
  val numClassesModel = new IntParam(this, "numClassesModel",
    "Number of classes.")

  def getNumClassesModel: Int = $(numClassesModel)
  def setNumClassesModel(value: Int): this.type = set(numClassesModel, value)

  def numClasses: Int = getNumClassesModel

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = transformImplInternal(dataset)

      if (getNumClassesModel == 2) {
        // which mode one wants to use depends a bit on how this should be deployed
        // 1. if you stay in spark w/o link=logistic is probably more convenient as it also returns the raw prediction
        // 2. if you want to export the model *and* get probabilities at scoring term w/ link=logistic is preferable

        // convert raw prediction to probability (if needed)
        val probabilityUdf = if (vwArgs.getArgs.contains("--link logistic"))
          udf { (pred: Double) => Vectors.dense(Array(1 - pred, pred)) }
        else
          udf { (pred: Double) => {
            val prob = 1.0 / (1.0 + exp(-pred))
            Vectors.dense(Array(1 - prob, prob))
          }}

        val probability2predictionUdf = udf(probability2prediction _)

        df
          .withColumn($(rawPredictionCol),
            col(vowpalWabbitPredictionCol).getField("prediction").cast(DoubleType))
          .withColumn($(probabilityCol), probabilityUdf(col($(rawPredictionCol))))
          // convert probability to prediction
          .withColumn($(predictionCol), probability2predictionUdf(col($(probabilityCol))))
      }
      else {
        val outputsProbs = vw.getOutputPredictionType.equals("prediction_type_t::scalars")

        if (outputsProbs) {
          // find prediction based on highest prob
          val argMaxUDF = udf { (probs: Vector) => probs.argmax.toDouble }
          val arrayToVector = udf { (probs: Seq[Float]) => Vectors.dense(probs.toArray.map(_.toDouble)) }

          df
            .withColumn($(rawPredictionCol), col(vowpalWabbitPredictionCol).getField("predictions"))
            .withColumn($(probabilityCol), arrayToVector(col($(rawPredictionCol))))
            .withColumn($(predictionCol), argMaxUDF(col($(probabilityCol))))
        }
        else
          df
            .withColumn($(rawPredictionCol),
              col(vowpalWabbitPredictionCol).getField("prediction").cast(DoubleType))
            .withColumn($(predictionCol), col($(rawPredictionCol)))
      }
    })
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector =
  {
    throw new NotImplementedError("Not implemented")
  }

  override def predictRaw(features: Row): Vector =
  {
    throw new NotImplementedError("Not implemented")
  }
}

object VowpalWabbitClassificationModel extends ComplexParamsReadable[VowpalWabbitClassificationModel]
