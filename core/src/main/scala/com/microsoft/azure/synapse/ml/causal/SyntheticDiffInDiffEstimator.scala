// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.FeatureNames
import org.apache.spark.ml.param.{DoubleParam, ParamValidators, Params}
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.sql.{Dataset, Row}

trait SyntheticDiffInDiffEstimatorParams extends SyntheticEstimatorParams {
  final val zeta = new DoubleParam(this, "zeta",
    "The zeta value for regularization term when fitting unit weights. " +
      "If not specified, a default value will be computed based on formula (2.2) specified in " +
      "https://www.nber.org/system/files/working_papers/w25532/w25532.pdf. " +
      "For large scale data, one may want to tune the zeta value, minimizing the loss of the unit weights regression.",
    ParamValidators.gtEq(0))

  def getZeta: Double = $(zeta)
  def setZeta(value: Double): this.type = set(zeta, value)
}

class SyntheticDiffInDiffEstimator(override val uid: String)
  extends BaseDiffInDiffEstimator(uid)
    with SyntheticEstimator
    with SyntheticDiffInDiffEstimatorParams
    with ComplexParamsWritable
    with Wrappable {

  logClass(FeatureNames.Causal)

  import SyntheticEstimator._

  def this() = this(Identifiable.randomUID("SyntheticDiffInDiffEstimator"))

  // scalastyle:off method.length
  override def fit(dataset: Dataset[_]): DiffInDiffModel = logFit({
    val df = dataset
      .withColumn(getTreatmentCol, treatment.cast(BooleanType))
      .withColumn(getPostTreatmentCol, postTreatment.cast(BooleanType))
      .toDF
    val controlDf = df.filter(not(treatment)).cache
    val preDf = df.filter(not(postTreatment)).cache
    val timeIdxCol = DatasetExtensions.findUnusedColumnName("Time_idx", df)
    val unitIdxCol = DatasetExtensions.findUnusedColumnName("Unit_idx", df)
    val timeIdx = createIndex(preDf, getTimeCol, timeIdxCol).cache
    val unitIdx = createIndex(controlDf, getUnitCol, unitIdxCol).cache
    val size = (unitIdx.count, timeIdx.count)

    // indexing
    val indexedControlDf = controlDf.join(timeIdx, controlDf(getTimeCol) === timeIdx(getTimeCol), "left_outer")
      .join(unitIdx, controlDf(getUnitCol) === unitIdx(getUnitCol), "left_outer")
      .select(unitIdxCol, timeIdxCol, getTreatmentCol, getPostTreatmentCol, getOutcomeCol)
      .localCheckpoint(true)

    val indexedPreDf = preDf.join(timeIdx, preDf(getTimeCol) === timeIdx(getTimeCol), "left_outer")
      .join(unitIdx, preDf(getUnitCol) === unitIdx(getUnitCol), "left_outer")
      .select(unitIdxCol, timeIdxCol, getTreatmentCol, getPostTreatmentCol, getOutcomeCol)
      .localCheckpoint(true)

    // fit time weights
    val (timeWeights, timeIntercept, timeRMSE, lossHistoryTimeWeights) = fitTimeWeights(
      handleMissingOutcomes(indexedControlDf, timeIdx.count.toInt)(unitIdxCol, timeIdxCol),
      size
    )(unitIdxCol, timeIdxCol)

    // fit unit weights
    val zetaValue = this.get(zeta).getOrElse(calculateRegularization(df))
    val (unitWeights, unitIntercept, unitRMSE, lossHistoryUnitWeights) = fitUnitWeights(
      handleMissingOutcomes(indexedPreDf, timeIdx.count.toInt)(unitIdxCol, timeIdxCol),
      zetaValue,
      fitIntercept = true,
      size.swap
    )(unitIdxCol, timeIdxCol)

    // join weights
    val Row(t: Long, u: Long) = df.agg(
      countDistinct(when(postTreatment, col(getTimeCol))),
      countDistinct(when(treatment, col(getUnitCol)))
    ).head

    val indexedDf = df.join(timeIdx, df(getTimeCol) === timeIdx(getTimeCol), "left_outer")
      .join(unitIdx, df(getUnitCol) === unitIdx(getUnitCol), "left_outer")

    val interactionCol = findInteractionCol(indexedDf.columns.toSet)
    val weightsCol = findWeightsCol(indexedDf.columns.toSet)
    val didData = indexedDf.select(
        col(unitIdxCol),
        col(timeIdxCol),
        postTreatment.cast(IntegerType).as(getPostTreatmentCol),
        treatment.cast(IntegerType).as(getTreatmentCol),
        outcome
      ).as("l")
      .join(timeWeights.as("t"), col(s"l.$timeIdxCol") === col("t.i"), "left_outer")
      .join(unitWeights.as("u"), col(s"l.$unitIdxCol") === col("u.i"), "left_outer")
      .select(
        postTreatment,
        treatment,
        outcome,
        (
          coalesce(col("t.value"), lit(1d / t)) * // time weights
            coalesce(col("u.value"), lit(1d / u)) + // unit weights
            lit(getEpsilon) // avoid zero weights
          ).as(weightsCol),
        (treatment * postTreatment).as(interactionCol)
      )

    val linearModel = fitLinearModel(
      didData,
      Array(getPostTreatmentCol, getTreatmentCol, interactionCol),
      fitIntercept = true,
      Some(weightsCol)
    )

    val treatmentEffect = linearModel.coefficients(2)
    val standardError = linearModel.summary.coefficientStandardErrors(2)

    val summary = DiffInDiffSummary(
      treatmentEffect,
      standardError,
      timeWeights = Some(timeWeights),
      timeIntercept = Some(timeIntercept),
      timeRMSE = Some(timeRMSE),
      unitWeights = Some(unitWeights),
      unitIntercept = Some(unitIntercept),
      unitRMSE = Some(unitRMSE),
      zeta = Some(zetaValue),
      lossHistoryTimeWeights = Some(lossHistoryTimeWeights.toList),
      lossHistoryUnitWeights = Some(lossHistoryUnitWeights.toList)
    )

    copyValues(new DiffInDiffModel(this.uid))
      .setSummary(Some(summary))
      .setParent(this)
      .setTimeIndex(timeIdx)
      .setTimeIndexCol(timeIdxCol)
      .setUnitIndex(unitIdx)
      .setUnitIndexCol(unitIdxCol)
  }, dataset.columns.length)
}

object SyntheticDiffInDiffEstimator extends ComplexParamsReadable[SyntheticDiffInDiffEstimator]
