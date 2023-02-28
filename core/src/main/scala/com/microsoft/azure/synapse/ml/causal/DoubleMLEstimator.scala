// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import breeze.linalg.sum
import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.train.{AutoTrainedModel, TrainClassifier, TrainRegressor}
import com.microsoft.azure.synapse.ml.core.schema.{DatasetExtensions, SchemaConstants}
import com.microsoft.azure.synapse.ml.core.utils.StopWatch
import com.microsoft.azure.synapse.ml.logging.SynapseMLLogging
import com.microsoft.azure.synapse.ml.param.{TransformerArrayParam, TransformerParam}
import com.microsoft.azure.synapse.ml.stages.DropColumns
import org.apache.spark.annotation.Experimental
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, Estimator, Model, Pipeline, Transformer}
import org.apache.spark.ml.classification.ProbabilisticClassifier
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, Regressor}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.param.{DoubleArrayParam, ParamMap}
import org.apache.spark.ml.param.shared.{HasPredictionCol, HasProbabilityCol, HasRawPredictionCol, HasWeightCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{avg, col, lit, stddev, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StructType}

import scala.concurrent.Future

// scalastyle:off method.length
// scalastyle:off cyclomatic.complexity

/** Double ML estimators. The estimator follows the two stage process,
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
class DoubleMLEstimator(override val uid: String)
  extends Estimator[DoubleMLModel] with ComplexParamsWritable
    with DoubleMLParams with SynapseMLLogging with Wrappable {

  logClass()

  def this() = this(Identifiable.randomUID("DoubleMLEstimator"))

  /** Fits the DoubleML model.
   *
   * @param dataset The input dataset to train.
   * @return The trained DoubleML model, from which you can get Ate and Ci values
   */
  override def fit(dataset: Dataset[_]): DoubleMLModel = {
    logFit({
      require(getMaxIter > 0, "maxIter should be larger than 0!")
      val treatmentColType = dataset.schema(getTreatmentCol).dataType
      require(treatmentColType == DoubleType || treatmentColType == LongType
        || treatmentColType == IntegerType || treatmentColType == BooleanType,
        s"TreatmentCol must be of type DoubleType, LongType, IntegerType or BooleanType but got $treatmentColType")

      if (treatmentColType != IntegerType && treatmentColType != BooleanType
        && getTreatmentType == TreatmentTypes.Binary)
      {throw new Exception("TreatmentModel was set as classifier but treatment columns isn't integer or boolean.")}

      if (treatmentColType != DoubleType && treatmentColType != LongType
        && getTreatmentType == TreatmentTypes.Continuous)
      {throw new Exception("TreatmentModel was set as regression but treatment columns isn't continuous data type.")}

      if (get(weightCol).isDefined) {
        getTreatmentModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception("""The selected treatment model does not support sample weight,
            but the weightCol parameter was set for the DoubleMLEstimator.
            Please select a treatment model that supports sample weight.""".stripMargin)
        }
        getOutcomeModel match {
          case w: HasWeightCol => w.set(w.weightCol, getWeightCol)
          case _ => throw new Exception("""The selected outcome model does not support sample weight,
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

      val ateFutures =(1 to getMaxIter).toArray.map { index =>
        Future[Option[(Double, Array[Transformer], Array[Transformer], Array[Transformer])]] {
          log.info(s"Executing ATE calculation on iteration: $index")
          // If the algorithm runs over 1 iteration, do not bootstrap from dataset,
          // otherwise, redraw sample with replacement
          val redrewDF =  if (getMaxIter == 1) dataset else dataset.sample(withReplacement = true, fraction = 1)
          val ate: Option[(Double, Array[Transformer], Array[Transformer], Array[Transformer])] =
            try {
              val totalTime = new StopWatch
              val oneAte = totalTime.measure {
                trainInternal(redrewDF)
              }
              log.info(s"Completed ATE calculation on iteration $index and got ATE value: $oneAte, " +
                s"time elapsed: ${totalTime.elapsed() / 6e10} minutes")
              Some(oneAte._1, oneAte._2, oneAte._3, oneAte._4)
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

      val dmlModel = this.copyValues(new DoubleMLModel(uid))
        .setRawTreatmentEffects(ates.map(_._1).toArray)
        .setTrainedTreatmentModels(ates.flatMap(_._2).toArray)
        .setTrainedOutcomeModels(ates.flatMap(_._3).toArray)
        .setTrainedRegressionModels(ates.flatMap(_._4).toArray)
      dmlModel
    })
  }

  //scalastyle:off method.length
  private def trainInternal(dataset: Dataset[_]): (
    Double,
    Array[Transformer],
    Array[Transformer],
    Array[Transformer]) = {

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

    def calculateResiduals(train: Dataset[_], test: Dataset[_]): (DataFrame, Transformer, Transformer) = {
      val treatmentModel = treatmentEstimator.setInputCols(
        train.columns.filterNot(Array(getTreatmentCol, getOutcomeCol).contains)
      ).fit(train)

      val outcomeModel = outcomeEstimator.setInputCols(
        train.columns.filterNot(Array(getOutcomeCol, getTreatmentCol).contains)
      ).fit(train)

      val treatmentResidual =
        new ResidualTransformer()
          .setObservedCol(getTreatmentCol)
          .setPredictedCol(treatmentResidualPredictionColName)
          .setOutputCol(treatmentResidualCol)
      val dropTreatmentPredictedColumns = new DropColumns().setCols(treatmentPredictionColsToDrop.toArray)
      val outcomeResidual =
        new ResidualTransformer()
          .setObservedCol(getOutcomeCol)
          .setPredictedCol(outcomeResidualPredictionColName)
          .setOutputCol(outcomeResidualCol)
      val dropOutcomePredictedColumns = new DropColumns().setCols(outcomePredictionColsToDrop.toArray)
      val treatmentResidualVA =
        new VectorAssembler()
          .setInputCols(Array(treatmentResidualCol))
          .setOutputCol(treatmentResidualVecCol)
          .setHandleInvalid("skip")
      val pipeline = new Pipeline().setStages(Array(
        treatmentModel, treatmentResidual, dropTreatmentPredictedColumns,
        outcomeModel, outcomeResidual, dropOutcomePredictedColumns,
        treatmentResidualVA))

      (pipeline.fit(test).transform(test), treatmentModel, outcomeModel)
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
    val (residualsDF1, treatmentModel1, outcomeModel1) = calculateResiduals(train, test)
    val (residualsDF2, treatmentModel2, outcomeModel2) = calculateResiduals(test, train)

    // Average slopes from the two residual models.
    val regressor = new GeneralizedLinearRegression()
      .setLabelCol(outcomeResidualCol)
      .setFeaturesCol(treatmentResidualVecCol)
      .setFamily("gaussian")
      .setLink("identity")
      .setFitIntercept(false)

    val regressorModel1 = regressor.fit(residualsDF1)
    val regressorModel2 = regressor.fit(residualsDF2)
    val ate = (regressorModel1.coefficients(0) + regressorModel2.coefficients(0)) / 2.0

    Seq(train, test).foreach(_.unpersist)
    (ate,
      Array(treatmentModel1, treatmentModel2),
      Array(outcomeModel1, outcomeModel2),
      Array(regressorModel1, regressorModel2)
    )
  }

  override def copy(extra: ParamMap): Estimator[DoubleMLModel] = {
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    DoubleMLEstimator.validateTransformSchema(schema)
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

  val trainedRegressorModels = new TransformerArrayParam(this,
    "trainedRegressorModels",
    "regressor models produced by DML training")
  def getTrainedRegressorModels(): Array[Transformer] = $(trainedRegressorModels)
  def setTrainedRegressionModels(v: Array[Transformer]): this.type = set(trainedRegressorModels, v)

  def getAvgTreatmentEffect: Double = {
    val finalAte =  $(rawTreatmentEffects).sum / $(rawTreatmentEffects).length
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
        }
        else {
          val treatmentModels = getTrainedTreatmentModels()
          val outcomeModels = getTrainedOutcomeModels()
          val regressorModels = getTrainedRegressorModels()
          val idxColName = DatasetExtensions.findUnusedColumnName("idx", dataset)
          val iteColName = DatasetExtensions.findUnusedColumnName("ite", dataset)

          val dfIdx = dataset.withColumn(idxColName, monotonically_increasing_id())

          val executionContext = getExecutionContextProxy
          val transformDFFutures = (0 until  treatmentModels.length).toArray.map { index =>
            Future[DataFrame] {
              val processedDF = calculateIteCol(
                dfIdx,
                iteColName,
                treatmentModels(index),
                outcomeModels(index),
                regressorModels(index))
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
                              treatmentModel: Transformer,
                              outcomeModel: Transformer,
                              regressorModel: Transformer): DataFrame = {
    // Using the treatment model, predict treatment probability (T^) using X columns.
    // and return: T^-1 (treatment residual)
    val df = treatmentModel.transform(dataset)
    val predictionTreatmentCol = DatasetExtensions.findUnusedColumnName("predictionTreatment", dataset)
    val rawPredictionTreatmentCol = DatasetExtensions.findUnusedColumnName("rawPredictionTreatment", dataset)
    val probabilityTreatmentCol = DatasetExtensions.findUnusedColumnName("probabilityTreatment", dataset)
    val probabilityTreatedCol = DatasetExtensions.findUnusedColumnName("probabilityTreated", dataset)
    val probabilityUntreatedCol = DatasetExtensions.findUnusedColumnName("probabilityUntreated", dataset)

    val dfTransformedwithTreatmentModel =
      df.withColumn(probabilityTreatedCol, lit(1) - vector_to_array(col("probability"))(1))
        .withColumn(probabilityUntreatedCol, vector_to_array(col("probability"))(1))
        .withColumnRenamed("prediction", predictionTreatmentCol)
        .withColumnRenamed("rawPrediction", rawPredictionTreatmentCol)
        .withColumnRenamed("probability", probabilityTreatmentCol)

    // Using the outcome model, predict outcome (Y^).
    val dfTransformedwithOutcomeModel =  outcomeModel.transform(dfTransformedwithTreatmentModel)
    val predictionOutcomeCol = DatasetExtensions.findUnusedColumnName("predictionOutcome", dataset)
    val rawPredictionOutcomeCol = DatasetExtensions.findUnusedColumnName("rawPredictionOutcome", dataset)
    val probabilityOutcomeCol = DatasetExtensions.findUnusedColumnName("probabilityOutcome", dataset)
    val dfDML =
      dfTransformedwithOutcomeModel.withColumnRenamed("prediction", predictionOutcomeCol)
        .withColumnRenamed("rawPrediction", rawPredictionOutcomeCol)
        .withColumnRenamed("probability", probabilityOutcomeCol)

    // Using the residuals model, predict outcome residual using output from step 1.
    val treatmentResidualVATreated =
      new VectorAssembler()
        .setInputCols(Array(probabilityTreatedCol))
        .setOutputCol("treatmentResidualVec")
        .setHandleInvalid("skip")
    val dfTransfomedTreated = treatmentResidualVATreated.transform(dfDML)
    val dfTransformedwithTreatedResidualModel =
      regressorModel.transform(dfTransfomedTreated).drop("treatmentResidualVec")
    val predictionTreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("predictionTreatedResidual", dataset)
    val rawPredictionTreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("rawPredictionTreatedResidual", dataset)
    val probabilityTreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("probabilityTreatedResidual", dataset)
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
      regressorModel.transform(dfTransformedUntreated).drop("treatmentResidualVec")
    val predictionUntreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("predictionUntreatedResidual", dataset)
    val rawPredictionUntreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("rawPredictionUntreatedResidual", dataset)
    val probabilityUntreatedResidualCol =
      DatasetExtensions.findUnusedColumnName("probabilityUntreatedResidual", dataset)
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
