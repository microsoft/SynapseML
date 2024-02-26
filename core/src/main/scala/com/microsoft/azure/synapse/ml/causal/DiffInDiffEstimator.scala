// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class DiffInDiffEstimator(override val uid: String)
  extends BaseDiffInDiffEstimator(uid)
    with ComplexParamsWritable
    with Wrappable
    with SynapseMLLogging {

  logClass(FeatureNames.Causal)

  def this() = this(Identifiable.randomUID("DiffInDiffEstimator"))

  override def fit(dataset: Dataset[_]): DiffInDiffModel = logFit({
    val interactionCol = findInteractionCol(dataset.columns.toSet)
    val postTreatment = col(getPostTreatmentCol)
    val treatment = col(getTreatmentCol)
    val outcome = col(getOutcomeCol)

    val didData = dataset.select(
        postTreatment.cast(IntegerType).as(getPostTreatmentCol),
        treatment.cast(IntegerType).as(getTreatmentCol),
        outcome.cast(DoubleType).as(getOutcomeCol)
      )
      .withColumn(interactionCol, treatment * postTreatment)

    val linearModel = fitLinearModel(
      didData,
      Array(getPostTreatmentCol, getTreatmentCol, interactionCol),
      fitIntercept = true
    )

    val treatmentEffect = linearModel.coefficients(2)
    val standardError = linearModel.summary.coefficientStandardErrors(2)
    val summary = DiffInDiffSummary(treatmentEffect, standardError)

    copyValues(new DiffInDiffModel(this.uid))
      .setSummary(Some(summary))
      .setParent(this)
  }, dataset.columns.length)
}

object DiffInDiffEstimator extends ComplexParamsReadable[DiffInDiffEstimator]
