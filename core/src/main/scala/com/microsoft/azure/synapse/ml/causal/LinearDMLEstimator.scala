// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.train.{TrainClassifier, TrainRegressor}
import com.microsoft.azure.synapse.ml.core.schema.{DatasetExtensions, SchemaConstants}
import com.microsoft.azure.synapse.ml.core.utils.StopWatch
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import com.microsoft.azure.synapse.ml.stages.DropColumns
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, Pipeline}
import org.apache.spark.ml.classification.ProbabilisticClassifier
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, Regressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.{DoubleArrayParam, ParamMap}
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import scala.concurrent.Future

/** Linear Double ML estimators. The estimator follows the two stage process,
 *  where a set of nuisance functions are estimated in the first stage in a cross-fitting manner
 *  and a final stage estimates the average treatment effect (ATE) model.
 *  Our goal is to estimate the constant marginal ATE Theta(X)
 *
 *  In this estimator, the ATE is estimated by using the following estimating equations:
 *  .. math ::
 *      Y - \\E[Y | X, W] = \\Theta(X) \\cdot (T - \\E[T | X, W]) + \\epsilon
 *
 *  Thus if we estimate the nuisance functions :math:`q(X, W) = \\E[Y | X, W]` and
 *  :math:`f(X, W)=\\E[T | X, W]` in the first stage, we can estimate the final stage ate for each
 *  treatment t, by running a regression, minimizing the residual on residual square loss,
 *  estimating Theta(X) is a final regression problem, regressing tilde{Y} on X and tilde{T})
 *
 *  .. math ::
 *       \\hat{\\theta} = \\arg\\min_{\\Theta}\
 *       \E_n\\left[ (\\tilde{Y} - \\Theta(X) \\cdot \\tilde{T})^2 \\right]
 *
 * Where
 * `\\tilde{Y}=Y - \\E[Y | X, W]` and :math:`\\tilde{T}=T-\\E[T | X, W]` denotes the
 * residual outcome and residual treatment.
 *
 * The nuisance function :math:`q` is a simple machine learning problem and
 * user can use setOutcomeModel to set an arbitrary sparkML model
 * that is internally used to solve this problem
 *
 * The problem of estimating the nuisance function :math:`f` is also a machine learning problem and
 * user can use setTreatmentModel to set an arbitrary sparkML model
 * that is internally used to solve this problem.
 *
 */
//noinspection ScalaDocParserErrorInspection,ScalaDocUnclosedTagWithoutParser
class LinearDMLEstimator(override val uid: String)
  extends Estimator[LinearDMLModel] with ComplexParamsWritable
    with LinearDMLParams with BasicLogging with Wrappable {

  logClass()

  def this() = this(Identifiable.randomUID("LinearDMLEstimator"))

  /** Fits the LinearDML model.
   *
   * @param dataset The input dataset to train.
   * @return The trained LinearDML model, from which you can get Ate and Ci values
   */
  override def fit(dataset: Dataset[_]): LinearDMLModel = {
    logFit({
      require(getMaxIter > 0, "maxIter should be larger than 0!")
      if (get(weightCol).isDefined) {
        getTreatmentModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception("""The selected treatment model does not support sample weight,
            but the weightCol parameter was set for the LinearDMLEstimator.
            Please select a treatment model that supports sample weight.""".stripMargin)
        }
        getOutcomeModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception("""The selected outcome model does not support sample weight,
            but the weightCol parameter was set for the LinearDMLEstimator.
            Please select a outcome model that supports sample weight.""".stripMargin)
        }
      }

      // sampling with replacement to redraw data and get TE value
      // Run it for multiple times in parallel, get a number of TE values,
      // Use average as Ate value, and 2.5% low end, 97.5% high end as Ci value
      // Create execution context based on $(parallelism)
      log.info(s"Parallelism: $getParallelism")
      val executionContext = getExecutionContextProxy

      val ateFutures =(1 to getMaxIter).toArray.map { index =>
        Future[Option[Double]] {
          log.info(s"Executing ATE calculation on iteration: $index")
          // If the algorithm runs over 1 iteration, do not bootstrap from dataset,
          // otherwise, draw sample with replacement
          val redrewDF =  if (getMaxIter == 1) dataset else dataset.sample(withReplacement = true, fraction = 1)
          val ate: Option[Double] =
            try {
              val totalTime = new StopWatch
              val oneAte = totalTime.measure {
                trainInternal(redrewDF)
              }
              log.info(s"Completed ATE calculation on iteration $index and got ATE value: $oneAte, " +
                s"time elapsed: ${totalTime.elapsed() / 60000000000.0} minutes")
              Some(oneAte)
            } catch {
              case ex: Throwable =>
                log.warn(s"ATE calculation got exception on iteration $index with the redrew sample data. " +
                  s"Exception details: $ex")
                None
            }
          ate
        }(executionContext)
      }

      val ates = awaitFutures(ateFutures).flatten
      val dmlModel = this.copyValues(new LinearDMLModel(uid)).setAtes(ates.toArray)
      dmlModel
    })
  }

  //scalastyle:off method.length
  private def trainInternal(dataset: Dataset[_]): Double = {

    def getModel(model: Estimator[_ <: Model[_]], labelColName: String, excludedFeatures: Array[String]) = {
      model match {
        case classifier: ProbabilisticClassifier[_, _, _] => (
          new TrainClassifier()
            .setModel(model)
            .setLabelCol(labelColName)
            .setExcludedFeatures(excludedFeatures),
          classifier.getProbabilityCol,
          Seq(classifier.getPredictionCol, classifier.getProbabilityCol, classifier.getRawPredictionCol)
        )
        case regressor: Regressor[_, _, _] => (
          new TrainRegressor()
            .setModel(model)
            .setLabelCol(labelColName)
            .setExcludedFeatures(excludedFeatures),
          regressor.getPredictionCol,
          Seq(regressor.getPredictionCol)
        )
      }
    }

    val (treatmentEstimator, treatmentResidualPredictionColName, treatmentPredictionColsToDrop) = getModel(
      getTreatmentModel.copy(getTreatmentModel.extractParamMap()), getTreatmentCol, Array(getOutcomeCol)
    )
    val (outcomeEstimator, outcomeResidualPredictionColName, outcomePredictionColsToDrop) = getModel(
      getOutcomeModel.copy(getOutcomeModel.extractParamMap()), getOutcomeCol, Array(getTreatmentCol)
    )

    val treatmentResidualVecCol = DatasetExtensions.findUnusedColumnName("treatmentResidualVec", dataset)

    def setUpDMLPipeline(train: Dataset[_], test: Dataset[_]): DataFrame = {
      val treatmentModel = treatmentEstimator.fit(train)
      val outcomeModel = outcomeEstimator.fit(train)

      val treatmentResidual =
        new ResidualTransformer()
          .setObservedCol(getTreatmentCol)
          .setPredictedCol(treatmentResidualPredictionColName)
          .setOutcomeCol(SchemaConstants.TreatmentResidualColumn)
      val dropTreatmentPredictedColumns = new DropColumns().setCols(treatmentPredictionColsToDrop.toArray)
      val outcomeResidual =
        new ResidualTransformer()
          .setObservedCol(getOutcomeCol)
          .setPredictedCol(outcomeResidualPredictionColName)
          .setOutcomeCol(SchemaConstants.OutcomeResidualColumn)
      val dropOutcomePredictedColumns = new DropColumns().setCols(outcomePredictionColsToDrop.toArray)
      val treatmentResidualVA =
        new VectorAssembler()
          .setInputCols(Array(SchemaConstants.TreatmentResidualColumn))
          .setOutputCol(treatmentResidualVecCol)
          .setHandleInvalid("skip")
      val pipeline = new Pipeline().setStages(Array(
        treatmentModel, treatmentResidual, dropTreatmentPredictedColumns,
        outcomeModel, outcomeResidual, dropOutcomePredictedColumns,
        treatmentResidualVA))

      pipeline.fit(test).transform(test)
    }

    // Note, we perform these steps to get ATE
    /*
      1. Split sample, e.g. 50/50
      2. Use the first split to fit the treatment model and the outcome model.
      3. Use the two models to fit a residual model on the second split.
      4. Cross-fit treatment and outcome models with the second split, residual model with the first split.
      5. Average slopes from the two residual models.
    */
    val splits = dataset.randomSplit(getSampleSplitRatio)
    val (train, test) = (splits(0).cache, splits(1).cache)
    val treatmentEffectDFV1 = setUpDMLPipeline(train, test)
    val treatmentEffectDFV2 = setUpDMLPipeline(test, train)

    // Average slopes from the two residual models.
    val regressor = new GeneralizedLinearRegression()
      .setLabelCol(SchemaConstants.OutcomeResidualColumn)
      .setFeaturesCol(treatmentResidualVecCol)
      .setFamily("gaussian")
      .setLink("identity")
      .setFitIntercept(false)
    val (lrmV1, lrmV2) = (regressor.fit(treatmentEffectDFV1), regressor.fit(treatmentEffectDFV2))
    Seq(train, test).foreach(_.unpersist)

    val ate = Seq(lrmV1, lrmV2).map(_.coefficients(0)).sum / 2
    ate
  }

  override def copy(extra: ParamMap): Estimator[LinearDMLModel] = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    LinearDMLEstimator.validateTransformSchema(schema)
  }
}

object LinearDMLEstimator extends ComplexParamsReadable[LinearDMLEstimator] {

  def validateTransformSchema(schema: StructType): StructType = {
    StructType(schema.fields)
  }
}

/** Model produced by [[LinearDMLEstimator]]. */
class LinearDMLModel(val uid: String)
  extends Model[LinearDMLModel] with LinearDMLParams with ComplexParamsWritable with Wrappable with BasicLogging {
  logClass()

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("LinearDMLModel"))

  val ates = new DoubleArrayParam(this, "ates", "treatment effect results for each iteration")
  def getAtes: Array[Double] = $(ates)
  def setAtes(v: Array[Double]): this.type = set(ates, v)

  def getAte: Double = {
    val finalAte =  $(ates).sum / $(ates).length
    finalAte
  }

  def getCi: Array[Double] = {
    val ciLowerBound = percentile($(ates), 100 * (1 - getConfidenceLevel))
    val ciUpperBound = percentile($(ates), getConfidenceLevel * 100)
    Array(ciLowerBound, ciUpperBound)
  }

  private def percentile(values: Seq[Double], quantile: Double): Double = {
    val sortedValues = values.sorted
    val percentile = new Percentile()
    percentile.setData(sortedValues.toArray)
    percentile.evaluate(quantile)
  }

  override def copy(extra: ParamMap): LinearDMLModel = defaultCopy(extra)

  /**
   * LinearDMLEstimator transform does nothing by design and isn't supposed to be called by end user.
  */
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      dataset.toDF()
    })
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    StructType(schema.fields)
}

object LinearDMLModel extends ComplexParamsReadable[LinearDMLModel]
