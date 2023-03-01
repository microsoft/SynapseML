// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.DatasetExtensions
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.TransformerArrayParam
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.param.{DoubleArrayParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.concurrent.Future

/** Model produced by [[DoubleMLEstimator]]. */
class DoubleMLModel(val uid: String)
  extends Model[DoubleMLModel] with DoubleMLParams with ComplexParamsWritable with Wrappable with SynapseMLLogging {

  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("DoubleMLModel"))

  val rawTreatmentEffects = new DoubleArrayParam(
    this,
    "rawTreatmentEffects",
    "raw treatment effect results for all iterations")

  def getRawTreatmentEffects: Array[Double] = $(rawTreatmentEffects)

  def setRawTreatmentEffects(v: Array[Double]): this.type = set(rawTreatmentEffects, v)


  val trainedTreatmentModels = new TransformerArrayParam(this,
    "trainedTreatmentModels",
    "treatment models produced by DML training")

  def getTrainedTreatmentModels(): Array[Transformer] = $(trainedTreatmentModels)

  def setTrainedTreatmentModels(v: Array[Transformer]): this.type = set(trainedTreatmentModels, v)

  val trainedOutcomeModels = new TransformerArrayParam(this,
    "trainedOutcomeModels",
    "outcome models produced by DML training")

  def getTrainedOutcomeModels(): Array[Transformer] = $(trainedOutcomeModels)

  def setTrainedOutcomeModels(v: Array[Transformer]): this.type = set(trainedOutcomeModels, v)

  val trainedResidualModels = new TransformerArrayParam(this,
    "trainedResidualModels",
    "residual models produced by DML training")

  def getTrainedResidualModels(): Array[Transformer] = $(trainedResidualModels)

  def setTrainedResidualModels(v: Array[Transformer]): this.type = set(trainedResidualModels, v)

  def getAvgTreatmentEffect: Double = {
    val finalAte = $(rawTreatmentEffects).sum / $(rawTreatmentEffects).length
    finalAte
  }

  def getConfidenceInterval: Array[Double] = {
    val ciLowerBound = percentile($(rawTreatmentEffects), 100 * (1 - getConfidenceLevel))
    val ciUpperBound = percentile($(rawTreatmentEffects), getConfidenceLevel * 100)
    Array(ciLowerBound, ciUpperBound)
  }

  private def percentile(values: Seq[Double], quantile: Double): Double = {
    val sortedValues = values.sorted
    val percentile = new Percentile()
    percentile.setData(sortedValues.toArray)
    percentile.evaluate(quantile)
  }

  override def copy(extra: ParamMap): DoubleMLModel = defaultCopy(extra)

  /**
    * :: Experimental ::
    * DoubleMLEstimator transform  function is still experimental, and its behavior could change in the future.
    */
  @Experimental
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val df = {
        if (getTreatmentType == TreatmentTypes.Continuous) {
          println("Warning: transform function will not calculate " +
            "individual treatment effect for continuous treatment.")

          dataset.withColumn(getIteOutputCol, lit(Double.NaN))
            .withColumn(getIteStddevOutputCol, lit(Double.NaN))
        } else {
          val treatmentModels = getTrainedTreatmentModels()
          val outcomeModels = getTrainedOutcomeModels()
          val regressorModels = getTrainedResidualModels()
          val idxColName = DatasetExtensions.findUnusedColumnName("idx", dataset)
          val iteColName = DatasetExtensions.findUnusedColumnName("ite", dataset)

          val dfIdx = dataset.withColumn(idxColName, monotonically_increasing_id())

          val executionContext = getExecutionContextProxy
          val transformDFFutures = (0 until treatmentModels.length).toArray.map { index =>
            Future[DataFrame] {
              val processedDF = calculateIteCol(
                dfIdx,
                iteColName,
                NamespaceInjections.pipelineModel(Array(treatmentModels(index), outcomeModels(index), regressorModels(index)))
              )
              processedDF
            }(executionContext)
          }

          val dfs = awaitFutures(transformDFFutures)

          val dfIte =
            dfs.reduce((df1, df2) => df1.union(df2))
              .groupBy(idxColName)
              .agg(avg(iteColName).alias(getIteOutputCol),
                stddev(iteColName).alias(getIteStddevOutputCol))

          val dfRes =
            dfIdx.join(dfIte, Seq(idxColName))
              .select(dfIdx.columns.map(col) ++ Seq(col(getIteOutputCol), col(getIteStddevOutputCol)): _*)
              .drop(idxColName)

          dfRes
        }

      }
      df
    })
  }

  private def calculateIteCol(dataset: Dataset[_],
                              iteColName: String,
                              pipelineModel: PipelineModel): DataFrame = {
    val (treatmentModel, outcomeModel, residualModel) = (pipelineModel.stages(0), pipelineModel.stages(1), pipelineModel.stages(2))
    val getFreshCol: String => String = name => DatasetExtensions.findUnusedColumnName(name, dataset)
    // Using the treatment model, predict treatment probability (T^) using X columns.
    // and return: T^-1 (treatment residual)
    val df = treatmentModel.transform(dataset)
    val (predictionTreatmentCol, rawPredictionTreatmentCol, probabilityTreatmentCol,
    probabilityTreatedCol, probabilityUntreatedCol) = (
      getFreshCol("predictionTreatment"), getFreshCol("rawPredictionTreatment"), getFreshCol("probabilityTreatment"),
      getFreshCol("probabilityTreated"), getFreshCol("probabilityUntreated"))

    val dfTransformedwithTreatmentModel =
      df.withColumn(probabilityTreatedCol, lit(1) - vector_to_array(col("probability"))(1))
        .withColumn(probabilityUntreatedCol, vector_to_array(col("probability"))(1))
        .withColumnRenamed("prediction", predictionTreatmentCol)
        .withColumnRenamed("rawPrediction", rawPredictionTreatmentCol)
        .withColumnRenamed("probability", probabilityTreatmentCol)

    // Using the outcome model, predict outcome (Y^).
    val dfTransformedwithOutcomeModel = outcomeModel.transform(dfTransformedwithTreatmentModel)
    val (predictionOutcomeCol, rawPredictionOutcomeCol, probabilityOutcomeCol) = (
      getFreshCol("predictionOutcome"), getFreshCol("rawPredictionOutcome"), getFreshCol("probabilityOutcome"))

    val dfDML =
      dfTransformedwithOutcomeModel.withColumnRenamed("prediction", predictionOutcomeCol)
        .withColumnRenamed("rawPrediction", rawPredictionOutcomeCol)
        .withColumnRenamed("probability", probabilityOutcomeCol)

    // Using the residuals model, predict outcome residual using output from previous step.
    val treatmentResidualVATreated =
      new VectorAssembler()
        .setInputCols(Array(probabilityTreatedCol))
        .setOutputCol("treatmentResidualVec")
        .setHandleInvalid("skip")
    val dfTransfomedTreated = treatmentResidualVATreated.transform(dfDML)
    val dfTransformedwithTreatedResidualModel =
      residualModel.transform(dfTransfomedTreated).drop("treatmentResidualVec")
    val (predictionTreatedResidualCol, rawPredictionTreatedResidualCol, probabilityTreatedResidualCol) = (
      getFreshCol("predictionTreatedResidual"), getFreshCol("rawPredictionTreatedResidual"),
      getFreshCol("probabilityTreatedResidual"))

    val dfTreated =
      dfTransformedwithTreatedResidualModel.withColumnRenamed("prediction", predictionTreatedResidualCol)
        .withColumnRenamed("rawPrediction", rawPredictionTreatedResidualCol)
        .withColumnRenamed("probability", probabilityTreatedResidualCol)
    val treatmentResidualVAUntreated =
      new VectorAssembler()
        .setInputCols(Array(probabilityUntreatedCol))
        .setOutputCol("treatmentResidualVec")
        .setHandleInvalid("skip")
    val dfTransformedUntreated = treatmentResidualVAUntreated.transform(dfTreated)
    val dfTransformedUntreatedResidualModel =
      residualModel.transform(dfTransformedUntreated).drop("treatmentResidualVec")
    val (predictionUntreatedResidualCol, rawPredictionUntreatedResidualCol, probabilityUntreatedResidualCol) = (
      getFreshCol("predictionUntreatedResidual"), getFreshCol("rawPredictionUntreatedResidual"),
      getFreshCol("probabilityUntreatedResidual"))
    val dfProcessed =
      dfTransformedUntreatedResidualModel.withColumnRenamed("prediction", predictionUntreatedResidualCol)
        .withColumnRenamed("rawPrediction", rawPredictionUntreatedResidualCol)
        .withColumnRenamed("probability", probabilityUntreatedResidualCol)

    // a = (predictionOutcomeCol + predictionTreatedResidualCol)
    // b = predictionTreatedResidualCol
    // a is the unbiased predicted outcome if the individual gets treatment
    // b is the unbiased predicted outcome if the individual does not get treatment
    // (a - b)  is the individual treatment effect
    val dfFinal =
    dfProcessed.withColumn(iteColName,
      col(predictionOutcomeCol) + col(predictionTreatedResidualCol) - col(predictionTreatedResidualCol)
    )

    val dfFinalCleaned = dfFinal.drop(
      predictionTreatmentCol, rawPredictionTreatmentCol, probabilityTreatmentCol,
      predictionOutcomeCol, rawPredictionOutcomeCol, probabilityOutcomeCol,
      probabilityTreatedCol, probabilityUntreatedCol,
      predictionTreatedResidualCol, rawPredictionTreatedResidualCol, probabilityTreatedResidualCol,
      predictionUntreatedResidualCol, rawPredictionUntreatedResidualCol, probabilityUntreatedResidualCol
    )

    dfFinalCleaned
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    schema.add(getIteOutputCol, DoubleType)
      .add(getIteStddevOutputCol, DoubleType)
}

object DoubleMLModel extends ComplexParamsReadable[DoubleMLModel]