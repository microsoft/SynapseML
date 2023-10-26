// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts.HasOutputCol
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/** Transform the outcome residual and treatment residual.
  * \E_n\left[ \left(\tilde{Y} - \theta(X) \cdot \tilde{T}\right)^2 \right] =
  * \E_n\left[ \tilde{T}^2 \left(\frac{\tilde{Y}}{\tilde{T}} - \theta(X)\right)^2 \right]
  * The latter corresponds to a weighted regression problem, where the target label is :math:`\tilde{Y}/\tilde{T}`,
  * the features are `X` and the weight of each sample is :math:`\tilde{T}^2`.
  * Any regressor that accepts sample weights can be used as a final model, e.g.:
  */
class OrthoForestVariableTransformer(override val uid: String) extends Transformer
  with HasOutputCol with DefaultParamsWritable with Wrappable with SynapseMLLogging {

  logClass(FeatureNames.Causal)

  def this() = this(Identifiable.randomUID("OrthoForestVariableTransformer"))

  val treatmentResidualCol = new Param[String](
    this, "treatmentResidualCol", "Treatment Residual Col")

  def setTreatmentResidualCol(value: String): this.type = set(param = treatmentResidualCol, value = value)

  final def getTreatmentResidualCol: String = getOrDefault(treatmentResidualCol)

  val outcomeResidualCol = new Param[String](
    this, "outcomeResidualCol", "Outcome Residual Col")

  def setOutcomeResidualCol(value: String): this.type = set(param = outcomeResidualCol, value = value)

  final def getOutcomeResidualCol: String = getOrDefault(outcomeResidualCol)

  val weightsCol = new Param[String](this, "weightsCol", "Weights Col")

  def setWeightsCol(value: String): this.type = set(param = weightsCol, value = value)

  final def getWeightsCol: String = getOrDefault(weightsCol)

  override def copy(extra: ParamMap): OrthoForestVariableTransformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType =
    StructType(
      schema.fields :+ StructField(
        name = $(outputCol),
        dataType = DoubleType,
        nullable = false
      ) :+ StructField(
        name = $(weightsCol),
        dataType = DoubleType,
        nullable = false
      )
    )

  setDefault(treatmentResidualCol -> "TResid",
    outcomeResidualCol -> "OResid",
    outputCol -> "_tmp_tsOutcome",
    weightsCol -> "_tmp_twOutcome")

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      transformSchema(schema = dataset.schema, logging = true)

      val treatmentResidualColType = dataset.schema(getTreatmentResidualCol).dataType
      require(treatmentResidualColType == DoubleType,
        s"treatmentResidualColType must be of type DoubleType got $treatmentResidualColType")

      val outcomeResidualColType = dataset.schema(getOutcomeResidualCol).dataType
      require(outcomeResidualColType == DoubleType,
        s"outcomeResidualColType must be of type DoubleType got $outcomeResidualColType")

      val finalData = dataset
        .withColumn($(outputCol), col(getOutcomeResidualCol) / col(getTreatmentResidualCol))
        .withColumn($(weightsCol), col(getTreatmentResidualCol) * col(getTreatmentResidualCol))

      finalData
    }, dataset.columns.length)
  }
}

object OrthoForestVariableTransformer extends DefaultParamsReadable[OrthoForestVariableTransformer]
