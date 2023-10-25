// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.{DatasetExtensions, SchemaConstants}
import com.microsoft.azure.synapse.ml.core.utils.StopWatch
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import com.microsoft.azure.synapse.ml.stages.DropColumns
import com.microsoft.azure.synapse.ml.train.{TrainClassifier, TrainRegressor}
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.ml.classification.ProbabilisticClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.shared.{HasPredictionCol, HasProbabilityCol, HasRawPredictionCol, HasWeightCol}
import org.apache.spark.ml.param.{DoubleArrayParam, ParamMap}
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, Regressor}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.concurrent.Future

// scalastyle:off method.length
// scalastyle:off cyclomatic.complexity

/** Double ML estimators. The estimator follows the two stage process,
  * where a set of nuisance functions are estimated in the first stage in a cross-fitting manner
  * and a final stage estimates the average treatment effect (ATE) model.
  * Our goal is to estimate the constant marginal ATE Theta(X)
  *
  * In this estimator, the ATE is estimated by using the following estimating equations:
  * .. math ::
  * Y - \\E[Y | X, W] = \\Theta(X) \\cdot (T - \\E[T | X, W]) + \\epsilon
  *
  * Thus if we estimate the nuisance functions :math:`q(X, W) = \\E[Y | X, W]` and
  * :math:`f(X, W)=\\E[T | X, W]` in the first stage, we can estimate the final stage ate for each
  * treatment t, by running a regression, minimizing the residual on residual square loss,
  * estimating Theta(X) is a final regression problem, regressing tilde{Y} on X and tilde{T})
  *
  * .. math ::
  * \\hat{\\theta} = \\arg\\min_{\\Theta}\
  * \E_n\\left[ (\\tilde{Y} - \\Theta(X) \\cdot \\tilde{T})^2 \\right]
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
class DoubleMLEstimator(override val uid: String)
  extends Estimator[DoubleMLModel] with ComplexParamsWritable
    with DoubleMLParams with SynapseMLLogging with Wrappable {

  logClass(FeatureNames.Causal)

  def this() = this(Identifiable.randomUID("DoubleMLEstimator"))

  /** Fits the DoubleML model.
    *
    * @param dataset The input dataset to train.
    * @return The trained DoubleML model, from which you can get Ate and Ci values
    */
  override def fit(dataset: Dataset[_]): DoubleMLModel = {
    logFit({
      require(getMaxIter > 0, "maxIter should be larger than 0!")
      validateColTypeWithModel(dataset, getTreatmentCol, getTreatmentModel)
      validateColTypeWithModel(dataset, getOutcomeCol, getOutcomeModel)

      if (get(weightCol).isDefined) {
        getTreatmentModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception(
            """The selected treatment model does not support sample weight,
            but the weightCol parameter was set for the DoubleMLEstimator.
            Please select a treatment model that supports sample weight.""".stripMargin)
        }
        getOutcomeModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception(
            """The selected outcome model does not support sample weight,
            but the weightCol parameter was set for the DoubleMLEstimator.
            Please select a outcome model that supports sample weight.""".stripMargin)
        }
      }

      // sampling with replacement to redraw data and get TE value
      // Run it for multiple times in parallel, get a number of TE values,
      // Use average as Ate value, and 2.5% low end, 97.5% high end as Ci value
      // Create execution context based on $(parallelism)
      log.info(s"Parallelism: $getParallelism")
      val executionContext = getExecutionContextProxy

      val ateFutures = (1 to getMaxIter).toArray.map { index =>
        Future[Option[Double]] {
          log.info(s"Executing ATE calculation on iteration: $index")
          // If the algorithm runs over 1 iteration, do not bootstrap from dataset,
          // otherwise, redraw sample with replacement
          val redrewDF = if (getMaxIter == 1) dataset else dataset.sample(withReplacement = true, fraction = 1)
          val ate: Option[Double] =
            try {
              val totalTime = new StopWatch
              val oneAte = totalTime.measure {
                trainInternal(redrewDF)
              }
              log.info(s"Completed ATE calculation on iteration $index " +
                s"and got ATE value: $oneAte, time elapsed: ${totalTime.elapsed() / 6e10} minutes")
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

      if (ates.isEmpty) {
        throw new Exception("ATE calculation failed on all iterations. Please check the log for details.")
      }
      val dmlModel = this.copyValues(new DoubleMLModel(uid)).setRawTreatmentEffects(ates.toArray)
      dmlModel
    }, dataset.columns.length)
  }

  //scalastyle:off method.length
  private def trainInternal(dataset: Dataset[_]): Double = {

    def getModel(model: Estimator[_ <: Model[_]], labelColName: String) = {
      model match {
        case classifier: ProbabilisticClassifier[_, _, _] => (
          new TrainClassifier()
            .setModel(model)
            .setLabelCol(labelColName),
          classifier.getProbabilityCol
        )
        case regressor: Regressor[_, _, _] => (
          new TrainRegressor()
            .setModel(model)
            .setLabelCol(labelColName),
          regressor.getPredictionCol
        )
      }
    }

    def getPredictedCols(model: Estimator[_ <: Model[_]]): Array[String] = {
      val rawPredictionCol = model match {
        case rp: HasRawPredictionCol => Some(rp.getRawPredictionCol)
        case _ => None
      }

      val predictionCol = model match {
        case p: HasPredictionCol => Some(p.getPredictionCol)
        case _ => None
      }

      val probabilityCol = model match {
        case pr: HasProbabilityCol => Some(pr.getProbabilityCol)
        case _ => None
      }

      (rawPredictionCol :: predictionCol :: probabilityCol :: Nil).flatten.toArray
    }

    val (treatmentEstimator, treatmentResidualPredictionColName) = getModel(
      getTreatmentModel.copy(getTreatmentModel.extractParamMap()),
      getTreatmentCol
    )
    val treatmentPredictionColsToDrop = getPredictedCols(getTreatmentModel)

    val (outcomeEstimator, outcomeResidualPredictionColName) = getModel(
      getOutcomeModel.copy(getOutcomeModel.extractParamMap()),
      getOutcomeCol
    )
    val outcomePredictionColsToDrop = getPredictedCols(getOutcomeModel)

    val treatmentResidualCol = DatasetExtensions.findUnusedColumnName(SchemaConstants.TreatmentResidualColumn, dataset)
    val outcomeResidualCol = DatasetExtensions.findUnusedColumnName(SchemaConstants.OutcomeResidualColumn, dataset)
    val treatmentResidualVecCol = DatasetExtensions.findUnusedColumnName("treatmentResidualVec", dataset)

    def calculateResiduals(train: Dataset[_], test: Dataset[_]): DataFrame = {
      val treatmentModel = treatmentEstimator.setInputCols(
        train.columns.filterNot(Array(getTreatmentCol, getOutcomeCol
        ).contains)
      )

      val outcomeModel = outcomeEstimator.setInputCols(
        train.columns.filterNot(Array(getOutcomeCol, getTreatmentCol
        ).contains)
      )

      val treatmentResidual =
        new ResidualTransformer()
          .setObservedCol(getTreatmentCol)
          .setPredictedCol(treatmentResidualPredictionColName)
          .setOutputCol(treatmentResidualCol)
      val dropTreatmentPredictedColumns = new DropColumns().setCols(treatmentPredictionColsToDrop)
      val outcomeResidual =
        new ResidualTransformer()
          .setObservedCol(getOutcomeCol)
          .setPredictedCol(outcomeResidualPredictionColName)
          .setOutputCol(outcomeResidualCol)
      val dropOutcomePredictedColumns = new DropColumns().setCols(outcomePredictionColsToDrop)

      // TODO: use org.apache.spark.ml.functions.array_to_vector function maybe slightly more efficient.
      val treatmentResidualVA =
        new VectorAssembler()
          .setInputCols(Array(treatmentResidualCol))
          .setOutputCol(treatmentResidualVecCol)
          .setHandleInvalid("skip")

      val treatmentPipeline = new Pipeline()
        .setStages(Array(treatmentModel, treatmentResidual, dropTreatmentPredictedColumns))
        .fit(train)

      val outcomePipeline = new Pipeline()
        .setStages(Array(outcomeModel, outcomeResidual, dropOutcomePredictedColumns))
        .fit(train)

      val df1 = treatmentPipeline.transform(test).cache
      val df2 = outcomePipeline.transform(df1).cache
      val transformed = treatmentResidualVA.transform(df2)
      transformed
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
    val residualsDF1 = calculateResiduals(train, test).select(outcomeResidualCol, treatmentResidualVecCol)
    val residualsDF2 = calculateResiduals(test, train).select(outcomeResidualCol, treatmentResidualVecCol)

    // Average slopes from the two residual models.
    val regressor = new GeneralizedLinearRegression()
      .setLabelCol(outcomeResidualCol)
      .setFeaturesCol(treatmentResidualVecCol)
      .setFamily("gaussian")
      .setLink("identity")
      .setFitIntercept(true)

    val coefficients = Array(residualsDF1, residualsDF2).map(regressor.fit).map(_.coefficients(0))
    val ate = coefficients.sum / coefficients.length
    Seq(train, test).foreach(_.unpersist)
    ate
  }

  override def copy(extra: ParamMap): Estimator[DoubleMLModel] = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    DoubleMLEstimator.validateTransformSchema(schema)
  }

  protected def validateColTypeWithModel(dataset: Dataset[_], colName: String, model: Estimator[_]): Unit = {
    val colType = dataset.schema(colName).dataType
    val modelType = getDoubleMLModelType(model)
    colType match {
      case IntegerType =>
        // If column is integer with value 0 or 1, it can be used with classification model
        // If column has normal integer values, it can be used with regression model
        // If user set to use classification model, verify if all values in (0, 1)
        if (modelType == DoubleMLModelTypes.Binary) {
          val hasInvalidValues = dataset.filter(!col(colName).isin(0, 1)).count() > 0
          if (hasInvalidValues)
            throw new Exception(s"column '$colName' in dataset is integer data type and " +
              "you set to use a classification model for it, " +
              "its all values must be either 0 or 1, but it has other values.")
        }
      case BooleanType =>
        if (modelType == DoubleMLModelTypes.Continuous)
          throw new Exception(s"column '$colName' in dataset is boolean data type, " +
            "but you set to use a regression model for it.")
      case DoubleType | LongType =>
        if (modelType == DoubleMLModelTypes.Binary)
          throw new Exception(s"column '$colName' in dataset is double or long data type, " +
            "but you set to use a classification model for it.")
      case _ =>
        throw new Exception(s"column '$colName' must be of type DoubleType, LongType, " +
          s"IntegerType or BooleanType but got $colType")
    }
  }
}

object DoubleMLEstimator extends ComplexParamsReadable[DoubleMLEstimator] {

  def validateTransformSchema(schema: StructType): StructType = {
    StructType(schema.fields)
  }
}

/** Model produced by [[DoubleMLEstimator]]. */
class DoubleMLModel(val uid: String)
  extends Model[DoubleMLModel] with DoubleMLParams with ComplexParamsWritable with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.Causal)

  override protected lazy val pyInternalWrapper = true

  def this() = this(Identifiable.randomUID("DoubleMLModel"))

  val rawTreatmentEffects = new DoubleArrayParam(
    this,
    "rawTreatmentEffects",
    "raw treatment effect results for all iterations")

  def getRawTreatmentEffects: Array[Double] = $(rawTreatmentEffects)

  def setRawTreatmentEffects(v: Array[Double]): this.type = set(rawTreatmentEffects, v)

  def getAvgTreatmentEffect: Double = {
    val finalAte = $(rawTreatmentEffects).sum / $(rawTreatmentEffects).length
    finalAte
  }

  def getPValue: Double = {
    val pvalue = TestUtils.tTest(0.0, $(rawTreatmentEffects))
    pvalue
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
      dataset.toDF()
    }, dataset.columns.length)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    StructType(schema.fields)
}

object DoubleMLModel extends ComplexParamsReadable[DoubleMLModel]
