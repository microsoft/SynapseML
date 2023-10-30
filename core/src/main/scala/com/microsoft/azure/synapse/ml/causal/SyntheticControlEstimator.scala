// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Pipeline}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class SyntheticControlEstimator(override val uid: String)
  extends BaseDiffInDiffEstimator(uid)
    with SyntheticEstimator
    with SyntheticEstimatorParams
    with ComplexParamsWritable
    with Wrappable {

  logClass(FeatureNames.Causal)

  import SyntheticEstimator._

  def this() = this(Identifiable.randomUID("SyntheticControlEstimator"))

  // scalastyle:off method.length
  override def fit(dataset: Dataset[_]): DiffInDiffModel = logFit({
    val df = dataset
      .withColumn(getTreatmentCol, treatment.cast(BooleanType))
      .withColumn(getPostTreatmentCol, postTreatment.cast(BooleanType))
      .toDF
    val controlDf = df.filter(not(treatment)).cache
    val preDf = df.filter(not(postTreatment)).cache
    val timeIdx = createIndex(preDf, getTimeCol, TimeIdxCol).cache
    val unitIdx = createIndex(controlDf, getUnitCol, UnitIdxCol).cache
    val size = (timeIdx.count, unitIdx.count)

    // indexing
    val indexedPreDf = preDf.join(timeIdx, preDf(getTimeCol) === timeIdx(getTimeCol), "left_outer")
      .join(unitIdx, preDf(getUnitCol) === unitIdx(getUnitCol), "left_outer")
      .select(UnitIdxCol, TimeIdxCol, getTreatmentCol, getPostTreatmentCol, getOutcomeCol)
      .localCheckpoint(true)

    val (unitWeights, unitIntercept, lossHistory) = fitUnitWeights(
      handleMissingOutcomes(indexedPreDf, timeIdx.count.toInt),
      zeta = 0d,
      fitIntercept = false,
      size
    )

    // join weights
    val Row(u: Long) = df.agg(
      countDistinct(when(treatment, col(getUnitCol)))
    ).head

    val indexedDf = df.join(unitIdx, df(getUnitCol) === unitIdx(getUnitCol), "left_outer")

    val didData = indexedDf.select(
        col(getTimeCol),
        col(UnitIdxCol),
        postTreatment.cast(IntegerType).as(getPostTreatmentCol),
        treatment.cast(IntegerType).as(getTreatmentCol),
        outcome
      ).as("l")
      .join(unitWeights.as("u"), col(s"l.$UnitIdxCol") === col("u.i"), "left_outer")
      .select(
        col(getTimeCol),
        postTreatment,
        treatment,
        outcome,
        (
          coalesce(col("u.value"), lit(1d / u)) + // unit weights
            lit(epsilon) // avoid zero weights
          ).as(weightsCol),
        (treatment * postTreatment).as(interactionCol)
      )

    val timeEncoded = encodeTimeEffect(didData)

    val linearModel = fitLinearModel(
      timeEncoded,
      Array(interactionCol, getTimeCol + "_enc"),
      fitIntercept = true,
      Some(weightsCol)
    )

    val treatmentEffect = linearModel.coefficients(0)
    val standardError = linearModel.summary.coefficientStandardErrors(0)

    val summary = DiffInDiffSummary(
      treatmentEffect,
      standardError,
      unitWeights = Some(unitWeights),
      unitIntercept = Some(unitIntercept),
      lossHistoryUnitWeights = Some(lossHistory.toList)
    )

    copyValues(new DiffInDiffModel(this.uid))
      .setSummary(Some(summary))
      .setParent(this)
      .setTimeIndex(timeIdx)
      .setUnitIndex(unitIdx)
  }, dataset.columns.length)

  private def encodeTimeEffect(didData: DataFrame): DataFrame = {
    val stringIndexer = new StringIndexer()
      .setInputCol(getTimeCol)
      .setOutputCol(getTimeCol + "idx")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol(getTimeCol + "idx")
      .setOutputCol(getTimeCol + "_enc")
      .setDropLast(false)

    new Pipeline()
      .setStages(Array(stringIndexer, oneHotEncoder))
      .fit(didData)
      .transform(didData)
  }
}

object SyntheticControlEstimator extends ComplexParamsReadable[SyntheticControlEstimator]
