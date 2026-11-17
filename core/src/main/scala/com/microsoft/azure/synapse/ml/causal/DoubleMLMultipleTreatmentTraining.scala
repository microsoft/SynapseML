// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.schema.SparkSchema
import com.microsoft.azure.synapse.ml.train.{TrainClassifier, TrainRegressor}
import org.apache.commons.math3.linear.{Array2DRowRealMatrix, EigenDecomposition}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, ProbabilisticClassifier}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasPredictionCol, HasProbabilityCol, HasRawPredictionCol}
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, Regressor}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, count, covar_samp, isnan, lit, sum, variance, when}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType}

import java.util.Locale
import java.util.concurrent.{Callable, ExecutionException, FutureTask}
import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/** Joint residual regression. This does not change the historical singular implementation. */
private[causal] object DoubleMLMultipleTreatmentTraining {
  private val RelativeVarianceFloor = 1e-20
  private val EigenvalueRatioFloor = 1e-10

  private def finite(value: Double): Boolean = java.lang.Double.isFinite(value)

  def validate(estimator: DoubleMLEstimator, dataset: Dataset[_]): Unit = {
    val names = dataset.columns
    val caseSensitive = dataset.sparkSession.conf.get("spark.sql.caseSensitive", "false").toBoolean
    val resolved = if (caseSensitive) names else names.map(_.toLowerCase(Locale.ROOT))
    require(resolved.distinct.length == resolved.length,
      "Plural input contains duplicate or case-insensitively ambiguous column names")
    require(names.contains(estimator.getOutcomeCol), "Outcome column does not exist")
    val treatments = estimator.getTreatmentCols
    require(treatments.length >= 2 && treatments.distinct.length == treatments.length,
      "treatmentCols must contain at least two distinct columns")
    require(!treatments.contains(estimator.getOutcomeCol), "Treatment columns must not include the outcome")
    require(!estimator.isDefined(estimator.weightCol), "weightCol is not supported for multiple treatments")
    require(estimator.getTreatmentModel.isInstanceOf[Regressor[_, _, _]],
      "Multiple treatments require an explicitly supplied Regressor treatmentModel")
    treatments.foreach { name =>
      require(dataset.columns.contains(name), s"Treatment column '$name' does not exist")
      require(Set[DataType](DoubleType, LongType, IntegerType).contains(dataset.schema(name).dataType),
        s"Treatment column '$name' must be DoubleType, LongType or IntegerType")
      val attribute = Attribute.fromStructField(dataset.schema(name))
      val sparkCategorical = attribute.isInstanceOf[NominalAttribute] || attribute.isInstanceOf[BinaryAttribute]
      require(!sparkCategorical && !SparkSchema.isCategorical(dataset.toDF(), name),
        s"Treatment column '$name' is categorical; multiple treatments must be continuous")
    }
    require(estimator.getSampleSplitRatio.forall(v => finite(v) && v > 0),
      "Plural sampleSplitRatio must contain two positive finite values")
  }

  /** FutureTask records even fatal causes; Scala Future's fatal-error promise can remain incomplete.
    * Only NonFatal failures become draw outcomes. No cancellation of sibling draws is promised.
    */
  def runIterations(iterations: Int, executionContext: ExecutionContext)
                   (calculate: Int => Array[Double]): Array[Array[Double]] = {
    val tasks = (1 to iterations).map { iteration =>
      val task = new FutureTask[Either[Throwable, Array[Double]]](
        new Callable[Either[Throwable, Array[Double]]] {
          override def call(): Either[Throwable, Array[Double]] = {
            try Right(calculate(iteration)) catch {
              case NonFatal(error) => Left(new IllegalArgumentException(
                s"Multiple-treatment draw $iteration failed", error))
            }
          }
        })
      executionContext.execute(task)
      task
    }
    val outcomes = tasks.map { task =>
      try task.get() catch {
        case error: ExecutionException => throw error.getCause
      }
    }
    outcomes.collectFirst { case Left(error) => error }.foreach(error => throw error)
    outcomes.collect { case Right(values) => values }.toArray
  }

  /** Split original rows before resampling so copies cannot cross cross-fit folds. */
  def splitFolds(dataset: Dataset[_], ratios: Array[Double], bootstrap: Boolean): Array[DataFrame] = {
    val original = dataset.toDF().randomSplit(ratios)
    original.map { fold =>
      if (bootstrap) fold.sample(withReplacement = true, fraction = 1) else fold
    }
  }

  /** Remap only supported auxiliary inputs, never caller parameters or learner outputs. */
  private def copyNuisance(model: Estimator[_ <: Model[_]], mapping: Map[String, String],
                           excluded: Set[String], caseSensitive: Boolean): Estimator[_ <: Model[_]] = {
    val copied = model.copy(ParamMap.empty)
    Seq("offsetCol", "weightCol", "validationIndicatorCol", "initScoreCol").foreach { name =>
      if (model.hasParam(name) && copied.hasParam(name)) {
        val parameter = model.getParam(name)
        // Leave defaults, unset parameters and explicitly empty input names unchanged.
        model.get(parameter).foreach {
          case value: String if value.nonEmpty =>
            val candidates = mapping.keys.filter { original =>
              if (caseSensitive) original == value
              else original.toLowerCase(Locale.ROOT) == value.toLowerCase(Locale.ROOT)
            }.toSeq
            require(candidates.size == 1, s"Auxiliary column '$value' from $name does not exist")
            val original = candidates.head
            require(!excluded.contains(original),
              s"Auxiliary parameter '$name' must not reference outcome or treatment column '$original'")
            copied.set(copied.getParam(name), mapping(original))
          case _ =>
        }
      }
    }
    copied
  }

  def fit(estimator: DoubleMLEstimator, dataset: Dataset[_]): Array[Array[Double]] = {
    validate(estimator, dataset)
    // Alias all columns positionally before any learner or Spark expression resolves a name.
    // Literal dots/backticks and caller prediction columns remain ordinary input features.
    val safeNames = dataset.columns.indices.map(i => Identifiable.randomUID(s"dml_input_$i")).toArray
    val mapping = dataset.columns.zip(safeNames).toMap
    val safeData = dataset.toDF(safeNames: _*)
    val caseSensitive = dataset.sparkSession.conf.get("spark.sql.caseSensitive", "false").toBoolean
    val excluded = (estimator.getTreatmentCols :+ estimator.getOutcomeCol).toSet
    val internal = estimator.copy(ParamMap.empty).asInstanceOf[DoubleMLEstimator]
      .setTreatmentCols(estimator.getTreatmentCols.map(mapping))
      .setOutcomeCol(mapping(estimator.getOutcomeCol))
      .setTreatmentModel(copyNuisance(estimator.getTreatmentModel, mapping, excluded, caseSensitive))
      .setOutcomeModel(copyNuisance(estimator.getOutcomeModel, mapping, excluded, caseSensitive))
    try internal.validateMultipleOutcome(safeData) catch {
      case NonFatal(error) => throw new IllegalArgumentException(
        error.getMessage.replace(internal.getOutcomeCol, estimator.getOutcomeCol), error)
    }
    runIterations(estimator.getMaxIter, estimator.getExecutionContextProxy) { iteration =>
      val folds = splitFolds(safeData, estimator.getSampleSplitRatio, estimator.getMaxIter > 1)
      try {
        folds.foreach { fold =>
          fold.cache()
          require(fold.count() > 1, s"Draw $iteration has an empty or singleton cross-fit fold")
        }
        val coefficients = folds.indices.map { fold =>
          try fitFold(internal, folds(1 - fold), folds(fold), estimator.getTreatmentCols) catch {
            case NonFatal(error) => throw new IllegalArgumentException(
              s"Draw $iteration fold ${fold + 1} failed", error)
          }
        }
        coefficients(0).zip(coefficients(1)).map { case (left, right) => left / 2 + right / 2 }
      } finally {
        folds.foreach(_.unpersist())
      }
    }
  }

  private def copyForPrediction(model: Estimator[_ <: Model[_]]): Estimator[_ <: Model[_]] = {
    val copied = model.copy(model.extractParamMap())
    copied match {
      case classifier: MultilayerPerceptronClassifier if classifier.isDefined(classifier.layers) =>
        classifier.setLayers(classifier.getLayers.clone())
      case _ =>
    }
    copied
  }

  private def prediction(model: Estimator[_ <: Model[_]], label: String,
                         inputs: Array[String], train: DataFrame, test: DataFrame,
                         residual: String): DataFrame = {
    val copied = copyForPrediction(model)
    val prefix = Identifiable.randomUID("dml_nuisance")
    val predictionCol = prefix + "_prediction"
    val probabilityCol = prefix + "_probability"
    copied match {
      case p: HasPredictionCol => p.set(p.predictionCol, predictionCol)
      case _ =>
    }
    copied match {
      case p: HasProbabilityCol => p.set(p.probabilityCol, probabilityCol)
      case _ =>
    }
    copied match {
      case p: HasRawPredictionCol => p.set(p.rawPredictionCol, prefix + "_raw")
      case _ =>
    }
    val (pipeline, predicted) = copied match {
      case _: Regressor[_, _, _] =>
        val fitted = new TrainRegressor().setModel(copied).setLabelCol(label).setInputCols(inputs).fit(train)
        (fitted.getModel, predictionCol)
      case _: ProbabilisticClassifier[_, _, _] =>
        val numericTrain = train.withColumn(label, col(label).cast(DoubleType))
        val fitted = new TrainClassifier().setReindexLabel(false).setModel(copied)
          .setLabelCol(label).setInputCols(inputs).fit(numericTrain)
        (fitted.getModel, probabilityCol)
      case _ => throw new IllegalArgumentException(s"Unsupported nuisance model for '$label'")
    }
    // Use the fitted inner pipeline: wrapper.transform assumes a literal 'prediction' metadata column.
    val scored = pipeline.transform(test)
    val transformed = new ResidualTransformer().setObservedCol(label).setPredictedCol(predicted)
      .setOutputCol(residual).transform(scored)
    transformed.select((test.columns.map(col) :+ col(residual)): _*)
  }

  private def fitFold(estimator: DoubleMLEstimator, train: DataFrame, test: DataFrame,
                      originalTreatments: Array[String]): Array[Double] = {
    val treatments = estimator.getTreatmentCols
    val outcome = estimator.getOutcomeCol
    val inputs = train.columns.filterNot((treatments :+ outcome).contains)
    val prefix = Identifiable.randomUID("dml_residual")
    val residuals = treatments.indices.map(i => s"${prefix}_$i").toArray
    val outcomeResidual = prefix + "_outcome"
    var current = test
    treatments.zip(residuals).zipWithIndex.foreach { case ((treatment, residual), index) =>
      try {
        current = prediction(estimator.getTreatmentModel, treatment, inputs, train, current, residual)
      } catch {
        case NonFatal(error) => throw new IllegalArgumentException(
          s"Treatment '${originalTreatments(index)}' nuisance failed", error)
      }
    }
    val frame = prediction(estimator.getOutcomeModel, outcome, inputs, train, current, outcomeResidual).cache()
    try {
      finalStageNamed(frame, treatments, residuals, outcomeResidual, originalTreatments)
    } finally {
      frame.unpersist()
    }
  }

  /** One aggregation collects only O(K^2) statistics; no input rows are collected. */
  def finalStage(frame: DataFrame, treatments: Array[String], residuals: Array[String],
                 outcomeResidual: String): Array[Double] = {
    finalStageNamed(frame, treatments, residuals, outcomeResidual, treatments)
  }

  private def invalidValues(names: Array[String]): Column = {
    names.map { name =>
      val value = col(name).cast(DoubleType)
      value.isNull || isnan(value) || value === lit(Double.PositiveInfinity) || value === lit(Double.NegativeInfinity)
    }.reduce(_ || _)
  }

  private def validateVariances(treatments: Array[String], originalTreatments: Array[String],
                                rawVariance: Array[Double], residualVariance: Array[Double]): Unit = {
    treatments.indices.foreach { i =>
      require(finite(rawVariance(i)) && rawVariance(i) > 0,
        s"Treatment '${originalTreatments(i)}' has nonpositive or nonfinite variance")
      require(finite(residualVariance(i)) && residualVariance(i) > 0 &&
        residualVariance(i) / rawVariance(i) > RelativeVarianceFloor,
        s"Treatment '${originalTreatments(i)}' has insufficient residual variance")
    }
  }

  private def finalStageNamed(frame: DataFrame, treatments: Array[String], residuals: Array[String],
                              outcomeResidual: String, originalTreatments: Array[String]): Array[Double] = {
    val size = treatments.length
    val covariancePairs = for (i <- 0 until size; j <- i until size) yield (i, j)
    val checked = treatments ++ residuals :+ outcomeResidual
    val invalid = invalidValues(checked)
    val columns = Seq(count(lit(1)).as("n"), sum(when(invalid, 1L).otherwise(0L)).as("invalid")) ++
      treatments.map(name => variance(col(name).cast(DoubleType))) ++
      residuals.map(name => variance(col(name))) ++
      covariancePairs.map { case (i, j) => covar_samp(col(residuals(i)), col(residuals(j))) }
    val stats = frame.agg(columns.head, columns.tail: _*).head()
    require(stats.getLong(0) > size + 1, "Insufficient residual rows for joint regression")
    require(stats.getLong(1) == 0, "Nonfinite treatment or residual values")
    val rawVariance = (0 until size).map(i => stats.getDouble(2 + i)).toArray
    val residualVariance = (0 until size).map(i => stats.getDouble(2 + size + i)).toArray
    validateVariances(treatments, originalTreatments, rawVariance, residualVariance)
    val correlation = Array.ofDim[Double](size, size)
    covariancePairs.zipWithIndex.foreach { case ((i, j), index) =>
      val covariance = stats.getDouble(2 + 2 * size + index)
      val normalized = (covariance / math.sqrt(residualVariance(i))) / math.sqrt(residualVariance(j))
      correlation(i)(j) = normalized
      correlation(j)(i) = normalized
    }
    require(correlation.flatten.forall(finite), "Residual correlation matrix is nonfinite")
    val eigenvalues = new EigenDecomposition(new Array2DRowRealMatrix(correlation)).getRealEigenvalues
    val largest = eigenvalues.max
    require(finite(largest) && largest > 0 && eigenvalues.min / largest >= EigenvalueRatioFloor,
      "Residual treatments are singular or ill-conditioned")
    val features = Identifiable.randomUID("dml_joint_features")
    val vectorized = new VectorAssembler().setInputCols(residuals).setOutputCol(features).transform(frame)
    val coefficients = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity")
      .setFitIntercept(true).setFeaturesCol(features).setLabelCol(outcomeResidual).fit(vectorized).coefficients.toArray
    require(coefficients.length == size && coefficients.forall(finite), "Joint coefficients are nonfinite")
    coefficients
  }
}
