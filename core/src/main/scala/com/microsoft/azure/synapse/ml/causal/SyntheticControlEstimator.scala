package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.sql.{Dataset, Row}

class SyntheticControlEstimator(override val uid: String)
  extends BaseDiffInDiffEstimator(uid)
    with SyntheticEstimator
    with SyntheticEstimatorParams
    with ComplexParamsWritable
    with Wrappable {

  logClass()

  import SyntheticEstimator._

  def this() = this(Identifiable.randomUID("syncon"))

  override def fit(dataset: Dataset[_]): DiffInDiffModel = logFit({
    val df = dataset
      .withColumn(getTreatmentCol, treatment.cast(BooleanType))
      .withColumn(getPostTreatmentCol, postTreatment.cast(BooleanType))
      .toDF
    val controlDf = df.filter(not(treatment)).cache
    val preDf = df.filter(not(postTreatment)).cache
    val controlPreDf = controlDf.filter(not(postTreatment)).cache
    val timeIdx = createIndex(controlPreDf, getTimeCol, TimeIdxCol).cache
    val unitIdx = createIndex(controlPreDf, getUnitCol, UnitIdxCol).cache

    // indexing
    val indexedPreDf = preDf.join(timeIdx, preDf(getTimeCol) === timeIdx(getTimeCol), "left_outer")
      .join(unitIdx, preDf(getUnitCol) === unitIdx(getUnitCol), "left_outer")
      .select(UnitIdxCol, TimeIdxCol, getTreatmentCol, getPostTreatmentCol, getOutcomeCol)
      .localCheckpoint(true)

    val (unitWeights, unitIntercept, lossHistory) = fitUnitWeights(
      handleMissingOutcomes(indexedPreDf, timeIdx.count.toInt),
      zeta = 0d,
      fitIntercept = false
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
        col(UnitIdxCol),
        postTreatment,
        treatment,
        outcome,
        (
          coalesce(col("u.value"), lit(1d / u)) + // unit weights
            lit(epsilon) // avoid zero weights
          ).as(weightsCol),
        (treatment * postTreatment).as(interactionCol)
      )

    val meanDf = didData.groupBy(getTimeCol).agg(
      mean(interactionCol).as(s"mean_$interactionCol"),
      mean(getOutcomeCol).as(s"mean_$getOutcomeCol")
    )

    val demeaned = didData.as("l").join(meanDf.as("r"),
        col(s"l.$getTimeCol") === col(s"r.$getTimeCol"), "inner")
      .select(
        col(s"l.$getTimeCol").as(getTimeCol),
        postTreatment,
        treatment,
        (col(interactionCol) - col(s"mean_$interactionCol")).as(interactionCol),
        (col(getOutcomeCol) - col(s"mean_$getOutcomeCol")).as(getOutcomeCol),
        col(weightsCol)
      )

    val linearModel = fitLinearModel(demeaned, Array(interactionCol), fitIntercept = false, Some(weightsCol))

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
}

object SyntheticControlEstimator extends ComplexParamsReadable[SyntheticControlEstimator]
