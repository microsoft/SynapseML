// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions._
import com.microsoft.azure.synapse.ml.core.utils.ParamsStringBuilder
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.VowpalWabbitExample

object VowpalWabbitMulticlassClassifier extends ComplexParamsReadable[VowpalWabbitClassifier]

class VowpalWabbitMulticlassClassifier(override val uid: String)
  extends ProbabilisticClassifier[Row, VowpalWabbitMulticlassClassifier, VowpalWabbitMulticlassClassificationModel]
    with VowpalWabbitBaseSpark
    with ComplexParamsWritable with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("VowpalWabbitMulticlassClassifier"))

  val numClasses = new IntParam(this, "numClasses", "Number of classes. Passed via --oaa")

  def getNumClasses: Int = $(numClasses)
  def setNumClasses(value: Int): this.type = set(numClasses, value)

  protected override def appendExtraParams(sb: ParamsStringBuilder): ParamsStringBuilder =
  {
    sb.appendParamValueIfNotThere("oaa", "oaa", numClasses)
      .append("--indexing 0")
  }

  protected override def createLabelSetter(schema: StructType): (Row, VowpalWabbitExample) => Unit = {
    val labelColIdx = schema.fieldIndex(getLabelCol)

    val labelGetter = getAsInt(schema, labelColIdx)
    if (get(weightCol).isDefined) {
      val weightGetter = getAsFloat(schema, schema.fieldIndex(getWeightCol))
      (row: Row, ex: VowpalWabbitExample) =>
        ex.setMulticlassLabel(weightGetter(row), labelGetter(row))
    }
    else
      (row: Row, ex: VowpalWabbitExample) =>
        ex.setMulticlassLabel(labelGetter(row))
  }

  override protected def train(dataset: Dataset[_]): VowpalWabbitMulticlassClassificationModel = {
    logTrain({
      val model = new VowpalWabbitMulticlassClassificationModel(uid)
        .setFeaturesCol(getFeaturesCol)
        .setAdditionalFeatures(getAdditionalFeatures)
        .setPredictionCol(getPredictionCol)
        .setProbabilityCol(getProbabilityCol)
        .setRawPredictionCol(getRawPredictionCol)

      trainInternal(dataset, model)
        .setNumClasses(getNumClasses)
    })
  }

  override def copy(extra: ParamMap): VowpalWabbitMulticlassClassifier = defaultCopy(extra)
}

class VowpalWabbitMulticlassClassificationModel(override val uid: String)
  extends ProbabilisticClassificationModel[Row, VowpalWabbitMulticlassClassificationModel]
    with VowpalWabbitBaseModelSpark
    with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("VowpalWabbitClassificationModel"))

  override protected lazy val pyInternalWrapper = true

  def numClasses: Int = getNumClasses

  val numClassesParam = new IntParam(this, "numClasses", "Number of classes")

  def getNumClasses: Int = $(numClassesParam)

  def setNumClasses(value: Int): this.type = set(numClassesParam, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      // select the columns we care for
      val featureColumnNames = Seq(getFeaturesCol) ++ getAdditionalFeatures
      val featureColumns = dataset.schema.filter({ f => featureColumnNames.contains(f.name) })

      // pre-compute namespace hashes
      val featureColIndices = VowpalWabbitUtil.generateNamespaceInfos(
        StructType(featureColumns),
        vwArgs.getHashSeed,
        Seq(getFeaturesCol) ++ getAdditionalFeatures)

      // scalars or multiclass
      val outputsProbs = vw.getOutputPredictionType.equals("prediction_type_t::scalars")

      // prediction column must be double for evaluator to work
      val predictUDF = if (outputsProbs)
        udf { (namespaces: Row) => predictProbs(featureColIndices, namespaces) }
      else {
        udf { (namespaces: Row) => predictClass(featureColIndices, namespaces).toDouble }
      }

      // add prediction column
      val datasetWithRaw = dataset.withColumn(
        $(rawPredictionCol),
        predictUDF(struct(featureColumns.map(f => col(f.name)): _*)))

      if (outputsProbs) {
        // find prediction based on highest prob
        val argMaxUDF = udf { (probs: Vector) => probs.argmax.toDouble }

        datasetWithRaw
          .withColumn($(probabilityCol), col($(rawPredictionCol)))
          .withColumn($(predictionCol), argMaxUDF(col($(probabilityCol))))
      }
      else
        datasetWithRaw
          .withColumn($(predictionCol), col($(rawPredictionCol)))
    })
  }

  private def predictClass(featureColIndices: Seq[NamespaceInfo], row: Row): Int = {
    example.clear()

    // transfer features
    VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)

    example.predict.asInstanceOf[Int]
  }

  private def predictProbs(featureColIndices: Seq[NamespaceInfo], row: Row): Vector = {
    example.clear()

    // transfer features
    VowpalWabbitUtil.addFeaturesToExample(featureColIndices, row, example)

    Vectors.dense(example.predict.asInstanceOf[Array[Float]].map(_.toDouble))
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

object VowpalWabbitMulticlassClassificationModel extends ComplexParamsReadable[VowpalWabbitMulticlassClassificationModel]
