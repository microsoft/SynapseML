package com.microsoft.azure.synapse.ml.causal

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class DiffInDiffEstimator(override val uid: String)
  extends BaseDiffInDiffEstimator(uid) {

  def this() = this(Identifiable.randomUID("did"))

  override def fit(dataset: Dataset[_]): DiffInDiffModel = {
    val postTreatment = col(getPostTreatmentCol)
    val treatment = col(getTreatmentCol)
    val outcome = col(getOutcomeCol)

    val didData = dataset.select(
        postTreatment.cast(IntegerType).as(getPostTreatmentCol),
        treatment.cast(IntegerType).as(getTreatmentCol),
        outcome.cast(DoubleType).as(getOutcomeCol)
      )
      .withColumn(interactionCol, treatment * postTreatment)

    val linearModel = fitLinearModel(didData)

    val treatmentEffect = linearModel.coefficients(2)
    val standardError = linearModel.summary.coefficientStandardErrors(2)
    val summary = DiffInDiffSummary(treatmentEffect, standardError)
    val model = new DiffInDiffModel(this.uid, summary).setParent(this)
    copyValues(model)
  }
}
