// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.contracts._
import com.microsoft.azure.synapse.ml.core.metrics.{MetricConstants, MetricUtils}
import com.microsoft.azure.synapse.ml.core.schema.{CategoricalUtilities, SchemaConstants}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.log4j.Logger
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{SQLDataTypes, Vector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ComputeModelStatistics extends DefaultParamsReadable[ComputeModelStatistics] {
  private def getPositiveClassScore(scores: Vector, scoresColumnName: String): Double = {
    if (scores.size < 2) {
      throw new IllegalArgumentException(
        s"ComputeModelStatistics scoresCol '$scoresColumnName' vectors must contain at least two values.")
    }
    scores(1)
  }
}

trait ComputeModelStatisticsParams extends Wrappable with DefaultParamsWritable
  with HasLabelCol with HasScoresCol with HasScoredLabelsCol with HasEvaluationMetric {
  /** Param "evaluationMetric" is the metric to evaluate the models with. Default is "all"
    *
    * The metrics that can be chosen are:
    *
    *   For binary classification:
    *     - areaUnderROC (reported in the AUC column)
    *     - AUC
    *     - areaUnderPR
    *     - accuracy
    *     - precision
    *     - recall
    *
    *   For regression:
    *     - mse
    *     - rmse
    *     - r2
    *     - mae
    *
    *   Or, for either:
    *     - all - This will report all the relevant metrics
    *
    *   If scoring metadata is unavailable, specify the evaluation kind:
    *     - classification
    *     - regression
    *
    * @group param
    */
  setDefault(evaluationMetric -> MetricConstants.AllSparkMetrics)
}

/** Evaluates the given scored dataset. */
class ComputeModelStatistics(override val uid: String) extends Transformer
  with ComputeModelStatisticsParams with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("ComputeModelStatistics"))

  /** The ROC curve evaluated for a binary classifier. */
  var rocCurve: DataFrame = _

  lazy val metricsLogger = new MetricsLogger(uid)
  private val selectedPredictionColumn = "__synapseml_prediction"
  private val selectedLabelColumn = "__synapseml_label"

  /** Calculates the metrics for the given dataset and model.
    * @param dataset the dataset to calculate the metrics for
    * @return DataFrame whose columns contain the calculated metrics
    */
  //scalastyle:off method.length
  //scalastyle:off cyclomatic.complexity
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val caseSensitive = isCaseSensitive(dataset.sparkSession)
      val (modelName, resolvedLabelColumnName, scoreValueKind) =
        resolveSchemaInfo(dataset.schema, caseSensitive)
      val inputColumns =
        validateInputColumns(
          dataset.schema, modelName, resolvedLabelColumnName, scoreValueKind, caseSensitive)
      val labelColumnName = inputColumns.labelColumnName

      // For creating the result dataframe in classification or regression case
      val spark = dataset.sparkSession
      import spark.implicits._

      if (scoreValueKind == SchemaConstants.ClassificationKind) {
        val scoredLabelsColumnName = inputColumns.scoredLabelsColumnName.get
        var resultDF: DataFrame =
          Seq(MetricConstants.ClassificationEvaluationType)
            .toDF(MetricConstants.EvaluationType)

        // Get levels for label column if categorical
        val levels = CategoricalUtilities.getLevels(dataset.schema, labelColumnName)

        val levelsExist = levels.isDefined

        lazy val levelsToIndexMap: Map[Any, Double] = getLevelsToIndexMap(levels.get)

        lazy val predictionAndLabels =
          if (levelsExist)
            getPredictionAndLabels(dataset, labelColumnName, scoredLabelsColumnName, levelsToIndexMap)
          else
            selectAndCastToRDD(dataset, scoredLabelsColumnName, labelColumnName)

        lazy val scoresAndLabels = {
          inputColumns.scoresColumnName match {
            case Some(columnName) if levelsExist =>
              getScoresAndLabels(dataset, labelColumnName, columnName, levelsToIndexMap)
            case Some(columnName) => getScalarScoresAndLabels(dataset, labelColumnName, columnName)
            case None => predictionAndLabels
          }
        }

        lazy val (labels: Array[Double], confusionMatrix: Matrix) = createConfusionMatrix(predictionAndLabels)

        // If levels exist, use the extra information they give to get better performance
        getEvaluationMetric match {
          case allMetrics if allMetrics == MetricConstants.AllSparkMetrics ||
            allMetrics == MetricConstants.ClassificationMetricsName =>
            resultDF = addConfusionMatrixToResult(labels, confusionMatrix, resultDF)
            resultDF = addAllClassificationMetrics(
              modelName, dataset, labelColumnName, predictionAndLabels,
              confusionMatrix, scoresAndLabels, resultDF)
          case simpleMetric if simpleMetric == MetricConstants.AccuracySparkMetric ||
            simpleMetric == MetricConstants.PrecisionSparkMetric ||
            simpleMetric == MetricConstants.RecallSparkMetric =>
            resultDF = addSimpleMetric(simpleMetric, predictionAndLabels, resultDF)
          case MetricConstants.AucSparkMetric | MetricConstants.AreaUnderROCMetric =>
            val numLevels = if (levelsExist) levels.get.length
            else confusionMatrix.numRows
            if (numLevels <= 2) {
              val auc = getAUC(modelName, dataset, labelColumnName, scoresAndLabels)
              resultDF = resultDF.withColumn(MetricConstants.AucColumnName, lit(auc))
            } else {
              throw new Exception("Error: AUC is not available for multiclass case")
            }
          case MetricConstants.AreaUnderPRMetric =>
            val numLevels = if (levelsExist) levels.get.length
            else confusionMatrix.numRows
            if (numLevels <= 2) {
              val areaUnderPR = getAreaUnderPR(scoresAndLabels)
              resultDF = resultDF.withColumn(MetricConstants.AreaUnderPRColumnName, lit(areaUnderPR))
            } else {
              throw new Exception("Error: areaUnderPR is not available for multiclass case")
            }
          case default =>
            throwOnInvalidEvaluationMetric(
              default, SchemaConstants.ClassificationKind, MetricConstants.ClassificationMetrics)
        }
        resultDF
      } else if (scoreValueKind == SchemaConstants.RegressionKind) {
        val scoresColumnName = inputColumns.scoresColumnName.get

        val scoresAndLabels = selectAndCastToRDD(dataset, scoresColumnName, labelColumnName)
        val regressionMetrics = new RegressionMetrics(scoresAndLabels)
        val mse = regressionMetrics.meanSquaredError
        val rmse = regressionMetrics.rootMeanSquaredError
        val r2 = regressionMetrics.r2
        val mae = regressionMetrics.meanAbsoluteError

        metricsLogger.logRegressionMetrics(mse, rmse, r2, mae)

        getEvaluationMetric match {
          case MetricConstants.AllSparkMetrics | MetricConstants.RegressionMetricsName =>
            Seq((mse, rmse, r2, mae)).toDF(MetricConstants.MseColumnName,
              MetricConstants.RmseColumnName,
              MetricConstants.R2ColumnName,
              MetricConstants.MaeColumnName)
          case MetricConstants.MseSparkMetric =>
            Seq(mse).toDF(MetricConstants.MseColumnName)
          case MetricConstants.RmseSparkMetric =>
            Seq(rmse).toDF(MetricConstants.RmseColumnName)
          case MetricConstants.R2SparkMetric =>
            Seq(r2).toDF(MetricConstants.R2ColumnName)
          case MetricConstants.MaeSparkMetric =>
            Seq(mae).toDF(MetricConstants.MaeColumnName)
          case default =>
            throwOnInvalidEvaluationMetric(
              default, SchemaConstants.RegressionKind, MetricConstants.RegressionMetrics)
        }
      } else {
        throwOnInvalidScoringKind(scoreValueKind)
      }
    }, dataset.columns.length)
  }
  //scalastyle:on method.length
  //scalastyle:on cyclomatic.complexity

  private def validateInputColumns(schema: StructType,
                                   modelName: String,
                                   labelColumnName: String,
                                   scoreValueKind: String,
                                   caseSensitive: Boolean):
      ComputeModelStatisticsInputValidator.ValidatedInputColumns = {
    ComputeModelStatisticsInputValidator.validate(
      schema,
      modelName,
      labelColumnName,
      scoreValueKind,
      if (isDefined(scoredLabelsCol)) Some(getScoredLabelsCol) else None,
      if (isDefined(scoresCol)) Some(getScoresCol) else None,
      getEvaluationMetric,
      caseSensitive)
  }

  private def resolveSchemaInfo(schema: StructType,
                                caseSensitive: Boolean): (String, String, String) = {
    validateEvaluationMetric(schema, caseSensitive)
    if (canEvaluateWithoutMetadata) {
      getExplicitSchemaInfo(schema, caseSensitive)
    } else {
      getMetadataSchemaInfo(schema, caseSensitive)
    }
  }

  private def validateEvaluationMetric(schema: StructType, caseSensitive: Boolean): Unit = {
    if (!validEvaluationMetrics.contains(getEvaluationMetric)) {
      inferScoreValueKind(schema, caseSensitive) match {
        case Some(SchemaConstants.ClassificationKind) =>
          throwOnInvalidEvaluationMetric(
            getEvaluationMetric,
            SchemaConstants.ClassificationKind,
            MetricConstants.ClassificationMetrics)
        case Some(SchemaConstants.RegressionKind) =>
          throwOnInvalidEvaluationMetric(
            getEvaluationMetric,
            SchemaConstants.RegressionKind,
            MetricConstants.RegressionMetrics)
        case _ =>
          throwOnInvalidEvaluationMetric(
            getEvaluationMetric,
            "classification or regression",
            MetricConstants.ClassificationMetrics ++ MetricConstants.RegressionMetrics)
      }
    }
  }

  private def inferScoreValueKind(schema: StructType, caseSensitive: Boolean): Option[String] = {
    try {
      Some(MetricUtils.getSchemaInfo(
        schema,
        configuredLabelCol,
        MetricConstants.AllSparkMetrics,
        caseSensitive)._3)
    } catch {
      case _: IllegalArgumentException => None
    }
  }

  private def getExplicitSchemaInfo(schema: StructType,
                                    caseSensitive: Boolean): (String, String, String) = {
    val resolvedLabelCol = MetricUtils.resolveLabelColumn(schema, configuredLabelCol, caseSensitive).get
    ("custom model", resolvedLabelCol, scoreValueKindForMetric)
  }

  private def getMetadataSchemaInfo(schema: StructType,
                                    caseSensitive: Boolean): (String, String, String) = {
    try {
      MetricUtils.getSchemaInfo(
        schema,
        configuredLabelCol,
        getEvaluationMetric,
        caseSensitive)
    } catch {
      case error: IllegalArgumentException =>
        if (canFallBackToExplicitSchema(error)) {
          ("custom model", configuredLabelCol.getOrElse(""), scoreValueKindForMetric)
        } else {
          throw error
        }
    }
  }

  private def canFallBackToExplicitSchema(error: IllegalArgumentException): Boolean = {
    getEvaluationMetric != MetricConstants.AllSparkMetrics &&
      error.getMessage.startsWith("Unable to determine a complete scored model")
  }

  private def scoreValueKindForMetric: String = {
    if (MetricUtils.isClassificationMetric(getEvaluationMetric)) SchemaConstants.ClassificationKind
    else SchemaConstants.RegressionKind
  }

  private def configuredLabelCol: Option[String] =
    if (isDefined(labelCol)) Some(getLabelCol) else None

  private def isCaseSensitive(sparkSession: SparkSession): Boolean =
    sparkSession.conf.get("spark.sql.caseSensitive", "false").toBoolean

  private def canEvaluateWithoutMetadata: Boolean = {
    if (!isDefined(labelCol) || getEvaluationMetric == MetricConstants.AllSparkMetrics) {
      false
    } else if (MetricUtils.isClassificationMetric(getEvaluationMetric)) {
      isDefined(scoredLabelsCol)
    } else {
      isDefined(scoresCol)
    }
  }

  private def validEvaluationMetrics: Set[String] =
    MetricConstants.ClassificationMetrics ++
      MetricConstants.RegressionMetrics +
      MetricConstants.AllSparkMetrics

  private def addSimpleMetric(simpleMetric: String,
                              predictionAndLabels: RDD[(Double, Double)],
                              resultDF: DataFrame): DataFrame = {
    val (_, confusionMatrix: Matrix) = createConfusionMatrix(predictionAndLabels)
    // Compute metrics for binary classification
    if (confusionMatrix.numCols == 2) {
      val (accuracy: Double, precision: Double, recall: Double) =
        getBinaryAccuracyPrecisionRecall(confusionMatrix)
      metricsLogger.logClassificationMetrics(accuracy, precision, recall)
      // Add the metrics to the DF
      simpleMetric match {
        case MetricConstants.AccuracySparkMetric =>
          resultDF.withColumn(MetricConstants.AccuracyColumnName, lit(accuracy))
        case MetricConstants.PrecisionSparkMetric =>
          resultDF.withColumn(MetricConstants.PrecisionColumnName, lit(precision))
        case MetricConstants.RecallSparkMetric =>
          resultDF.withColumn(MetricConstants.RecallColumnName, lit(recall))
        case _ => resultDF
      }
    } else {
      val (microAvgAccuracy: Double, microAvgPrecision: Double, microAvgRecall: Double, _, _, _) =
        getMulticlassMetrics(predictionAndLabels, confusionMatrix)
      metricsLogger.logClassificationMetrics(microAvgAccuracy, microAvgPrecision, microAvgRecall)
      // Add the metrics to the DF
      simpleMetric match {
        case MetricConstants.AccuracySparkMetric =>
          resultDF.withColumn(MetricConstants.AccuracyColumnName, lit(microAvgAccuracy))
        case MetricConstants.PrecisionSparkMetric =>
          resultDF.withColumn(MetricConstants.PrecisionColumnName, lit(microAvgPrecision))
        case MetricConstants.RecallSparkMetric =>
          resultDF.withColumn(MetricConstants.RecallColumnName, lit(microAvgRecall))
        case _ => resultDF
      }
    }
  }

  private def addAllClassificationMetrics(modelName: String,
                                          dataset: Dataset[_],
                                          labelColumnName: String,
                                          predictionAndLabels: RDD[(Double, Double)],
                                          confusionMatrix: Matrix,
                                          scoresAndLabels: RDD[(Double, Double)],
                                          resultDF: DataFrame): DataFrame = {
    // Compute metrics for binary classification
    if (confusionMatrix.numCols == 2) {
      val (accuracy: Double, precision: Double, recall: Double)
          = getBinaryAccuracyPrecisionRecall(confusionMatrix)
      metricsLogger.logClassificationMetrics(accuracy, precision, recall)
      val (auc, areaUnderPR) =
        getBinaryMetrics(modelName, dataset, labelColumnName, scoresAndLabels)
      // Add the metrics to the DF
      resultDF
        .withColumn(MetricConstants.AccuracyColumnName, lit(accuracy))
        .withColumn(MetricConstants.PrecisionColumnName, lit(precision))
        .withColumn(MetricConstants.RecallColumnName, lit(recall))
        .withColumn(MetricConstants.AucColumnName, lit(auc))
        .withColumn(MetricConstants.AreaUnderPRColumnName, lit(areaUnderPR))
    } else {
      val (microAvgAccuracy: Double,
           microAvgPrecision: Double,
           microAvgRecall: Double,
           averageAccuracy: Double,
           macroAveragedPrecision: Double,
           macroAveragedRecall: Double)
          = getMulticlassMetrics(predictionAndLabels, confusionMatrix)
      metricsLogger.logClassificationMetrics(microAvgAccuracy, microAvgPrecision, microAvgRecall)
      resultDF
        .withColumn(MetricConstants.AccuracyColumnName, lit(microAvgAccuracy))
        .withColumn(MetricConstants.PrecisionColumnName, lit(microAvgPrecision))
        .withColumn(MetricConstants.RecallColumnName, lit(microAvgRecall))
        .withColumn(MetricConstants.AverageAccuracy, lit(averageAccuracy))
        .withColumn(MetricConstants.MacroAveragedPrecision, lit(macroAveragedPrecision))
        .withColumn(MetricConstants.MacroAveragedRecall, lit(macroAveragedRecall))
    }
  }

  private def addConfusionMatrixToResult(labels: Array[Double],
                                         confusionMatrix: Matrix,
                                         resultDF: DataFrame): DataFrame = {
    val schema = resultDF.schema.add(MetricConstants.ConfusionMatrix, SQLDataTypes.MatrixType)
    resultDF.map { row => Row.fromSeq(row.toSeq :+ confusionMatrix.asML) }(ExpressionEncoder(schema))
  }

  private def selectAndCastToDF(dataset: Dataset[_],
                                predictionColumnName: String,
                                labelColumnName: String): DataFrame = {
    dataset.select(
      inputColumn(predictionColumnName).cast(DoubleType).as(selectedPredictionColumn),
      inputColumn(labelColumnName).cast(DoubleType).as(selectedLabelColumn))
      .na
      .drop(Array(selectedPredictionColumn, selectedLabelColumn))
  }

  private def selectAndCastToRDD(dataset: Dataset[_],
                                 predictionColumnName: String,
                                 labelColumnName: String): RDD[(Double, Double)] = {
    selectAndCastToDF(dataset, predictionColumnName, labelColumnName)
      .rdd
      .map {
        case Row(prediction: Double, label: Double) => (prediction, label)
        case _ => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
  }

  private def getPredictionAndLabels(dataset: Dataset[_],
                                     labelColumnName: String,
                                     scoredLabelsColumnName: String,
                                     levelsToIndexMap: Map[Any, Double]): RDD[(Double, Double)] = {
    // Calculate confusion matrix and output it as DataFrame
    dataset.select(
      inputColumn(scoredLabelsColumnName).cast(DoubleType).as(selectedPredictionColumn),
      inputColumn(labelColumnName).as(selectedLabelColumn))
      .na
      .drop(Array(selectedPredictionColumn, selectedLabelColumn))
      .rdd
      .map {
        case Row(prediction: Double, label) => (prediction, levelsToIndexMap(label))
        case _ => throw new Exception(s"Error: prediction and label columns invalid or missing")
    }
  }

  private def getScalarScoresAndLabels(dataset: Dataset[_],
                                       labelColumnName: String,
                                       scoresColumnName: String): RDD[(Double, Double)] = {

    selectScoresAndLabels(dataset, labelColumnName, scoresColumnName, castLabel = true)
      .rdd
      .map {
        case Row(prediction: Vector, label: Double) =>
          (ComputeModelStatistics.getPositiveClassScore(prediction, scoresColumnName), label)
        case Row(prediction: Double, label: Double) => (prediction, label)
        case _ => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
  }

  private def getScoresAndLabels(dataset: Dataset[_],
                                labelColumnName: String,
                                scoresColumnName: String,
                                levelsToIndexMap: Map[Any, Double]): RDD[(Double, Double)] = {
    selectScoresAndLabels(dataset, labelColumnName, scoresColumnName, castLabel = false)
      .rdd
      .map {
        case Row(prediction: Vector, label) =>
          (ComputeModelStatistics.getPositiveClassScore(prediction, scoresColumnName), levelsToIndexMap(label))
        case Row(prediction: Double, label) => (prediction, levelsToIndexMap(label))
        case _ => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
  }

  private def selectScoresAndLabels(dataset: Dataset[_],
                                   labelColumnName: String,
                                   scoresColumnName: String,
                                   castLabel: Boolean): DataFrame = {
    val scores = if (dataset.schema(scoresColumnName).dataType == SQLDataTypes.VectorType)
      inputColumn(scoresColumnName).as(selectedPredictionColumn)
    else
      inputColumn(scoresColumnName).cast(DoubleType).as(selectedPredictionColumn)
    val label = if (castLabel) inputColumn(labelColumnName).cast(DoubleType).as(selectedLabelColumn)
    else inputColumn(labelColumnName).as(selectedLabelColumn)
    dataset.select(scores, label)
      .na
      .drop(Array(selectedPredictionColumn, selectedLabelColumn))
  }

  private def inputColumn(columnName: String): Column = {
    col(s"`${columnName.replace("`", "``")}`")
  }

  private def getLevelsToIndexMap(levels: Array[_]): Map[Any, Double] = {
    levels.zipWithIndex.map(t => t._1 -> t._2.toDouble).toMap
  }

  private def getMulticlassMetrics(predictionAndLabels: RDD[(Double, Double)],
                                   confusionMatrix: Matrix): (Double, Double, Double, Double, Double, Double) = {
    // Compute multiclass metrics based on paper "A systematic analysis
    // of performance measure for classification tasks", Sokolova and Lapalme
    var tpSum: Double = 0.0
    for (diag: Int <- 0 until confusionMatrix.numCols) {
      tpSum += confusionMatrix(diag, diag)
    }
    val totalSum = predictionAndLabels.count()

    val microAvgAccuracy = tpSum / totalSum
    val microAvgPrecision = microAvgAccuracy
    val microAvgRecall = microAvgAccuracy

    // Compute class counts - these are the row and column sums of the matrix, used to calculate the
    // average accuracy, macro averaged precision and macro averaged recall
    val actualClassCounts = new Array[Double](confusionMatrix.numCols)
    val predictedClassCounts = new Array[Double](confusionMatrix.numRows)
    val truePositives = new Array[Double](confusionMatrix.numRows)
    for (rowIndex: Int <- 0 until confusionMatrix.numRows) {
      for (colIndex: Int <- 0 until confusionMatrix.numCols) {
        actualClassCounts(rowIndex) += confusionMatrix(rowIndex, colIndex)
        predictedClassCounts(colIndex) += confusionMatrix(rowIndex, colIndex)

        if (rowIndex == colIndex) {
          truePositives(rowIndex) += confusionMatrix(rowIndex, colIndex)
        }
      }
    }

    val (totalAccuracy, totalPrecision, totalRecall)
        = (0 until confusionMatrix.numCols).foldLeft((0.0,0.0,0.0)) {
      case ((acc, prec, rec), classIndex) =>
        (// compute the class accuracy as:
         // (true positive + true negative) / total =>
         // (true positive + (total - (actual + predicted - true positive))) / total =>
         // 2 * true positive + (total - (actual + predicted)) / total
         acc + (2 * truePositives(classIndex) +
                  (totalSum - (actualClassCounts(classIndex) + predictedClassCounts(classIndex)))) / totalSum,
         // compute the class precision as:
         // true positive / predicted as positive (=> tp + fp)
         prec + truePositives(classIndex) / predictedClassCounts(classIndex),
         // compute the class recall as:
         // true positive / actual positive (=> tp + fn)
         rec + truePositives(classIndex) / actualClassCounts(classIndex))
    }
    val averageAccuracy = totalAccuracy / confusionMatrix.numCols
    val macroAveragedPrecision = totalPrecision / confusionMatrix.numCols
    val macroAveragedRecall = totalRecall / confusionMatrix.numCols
    (microAvgAccuracy, microAvgPrecision, microAvgRecall, averageAccuracy, macroAveragedPrecision, macroAveragedRecall)
  }

  private def getBinaryMetrics(modelName: String,
                               dataset: Dataset[_],
                               labelColumnName: String,
                               scoresAndLabels: RDD[(Double, Double)]): (Double, Double) = {
    withBinaryClassificationMetrics(scoresAndLabels) { binaryMetrics =>
      (getAUC(modelName, dataset, labelColumnName, binaryMetrics), getAreaUnderPR(binaryMetrics))
    }
  }

  private def getAUC(modelName: String,
                     dataset: Dataset[_],
                     labelColumnName: String,
                     scoresAndLabels: RDD[(Double, Double)]): Double = {
    withBinaryClassificationMetrics(scoresAndLabels) { binaryMetrics =>
      getAUC(modelName, dataset, labelColumnName, binaryMetrics)
    }
  }

  private def getAUC(modelName: String,
                     dataset: Dataset[_],
                     labelColumnName: String,
                     binaryMetrics: BinaryClassificationMetrics): Double = {
    val spark = dataset.sparkSession
    import spark.implicits._

    rocCurve = binaryMetrics.roc()
      .toDF(MetricConstants.FpRateROCColumnName, MetricConstants.TpRateROCColumnName)
    metricsLogger.logROC(rocCurve)
    val auc = binaryMetrics.areaUnderROC()
    metricsLogger.logAUC(auc)
    auc
  }

  private def getAreaUnderPR(scoresAndLabels: RDD[(Double, Double)]): Double = {
    withBinaryClassificationMetrics(scoresAndLabels) { binaryMetrics =>
      getAreaUnderPR(binaryMetrics)
    }
  }

  private def getAreaUnderPR(binaryMetrics: BinaryClassificationMetrics): Double = {
    val areaUnderPR = binaryMetrics.areaUnderPR()
    metricsLogger.logAreaUnderPR(areaUnderPR)
    areaUnderPR
  }

  private def withBinaryClassificationMetrics[T](scoresAndLabels: RDD[(Double, Double)])
                                                  (f: BinaryClassificationMetrics => T): T = {
    val binaryMetrics = new BinaryClassificationMetrics(scoresAndLabels, MetricConstants.BinningThreshold)
    try {
      f(binaryMetrics)
    } finally {
      binaryMetrics.unpersist()
    }
  }

  private def getBinaryAccuracyPrecisionRecall(confusionMatrix: Matrix): (Double, Double, Double) = {
    val tp: Double = confusionMatrix(1, 1)
    val fp: Double = confusionMatrix(0, 1)
    val tn: Double = confusionMatrix(0, 0)
    val fn: Double = confusionMatrix(1, 0)

    val accuracy: Double = (tp + tn) / (tp + tn + fp + fn)
    val precision: Double = tp / (tp + fp)
    val recall: Double = tp / (tp + fn)
    (accuracy, precision, recall)
  }

  private def createConfusionMatrix(predictionAndLabels: RDD[(Double, Double)]): (Array[Double], Matrix) = {
    val metrics = new MulticlassMetrics(predictionAndLabels)
    var labels = metrics.labels
    var confusionMatrix = metrics.confusionMatrix

    val numCols = confusionMatrix.numCols
    val numRows = confusionMatrix.numRows

    // Reformat the confusion matrix if less than binary size
    if (numCols < 2 && numRows < 2) {
      val values = Array.ofDim[Double](2 * 2)
      for (col: Int <- 0 until confusionMatrix.numCols;
           row: Int <- 0 until confusionMatrix.numRows) {
        // We need to interpret the actual label value
        val colLabel = if (labels(col) > 0) 1 else 0
        val rowLabel = if (labels(row) > 0) 1 else 0
        values(colLabel + rowLabel * 2) =
          confusionMatrix(row, col)
      }
      confusionMatrix = Matrices.dense(2, 2, values)
      labels = Array(0, 1)
    }
    (labels, confusionMatrix)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val caseSensitive = SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .exists(isCaseSensitive)
    val (modelName, resolvedLabelColumnName, scoreValueKind) =
      resolveSchemaInfo(schema, caseSensitive)
    val inputColumns =
      validateInputColumns(
        schema, modelName, resolvedLabelColumnName, scoreValueKind, caseSensitive)
    val labelLevels = getLabelLevels(schema, inputColumns.labelColumnName)
    val (columns, validMetrics) =
      if (scoreValueKind == SchemaConstants.ClassificationKind)
        (getClassificationColumns(labelLevels), MetricConstants.ClassificationMetrics)
      else if (scoreValueKind == SchemaConstants.RegressionKind)
        (MetricConstants.RegressionColumns, MetricConstants.RegressionMetrics)
      else throwOnInvalidScoringKind(scoreValueKind)
    validateBinaryOnlyMetricSchema(labelLevels, scoreValueKind)
    getTransformedSchema(columns, scoreValueKind, validMetrics)
  }

  private def throwOnInvalidScoringKind(scoreValueKind: String) = {
    throw new Exception(s"Error: unknown scoring kind $scoreValueKind")
  }

  private def throwOnInvalidEvaluationMetric(metric: String,
                                             metricType: String,
                                             validMetrics: Set[String]): Nothing = {
    val validValues = (validMetrics + MetricConstants.AllSparkMetrics).toSeq.sorted.mkString(", ")
    val metricTypeName = metricType match {
      case SchemaConstants.ClassificationKind => "classification"
      case SchemaConstants.RegressionKind => "regression"
      case _ => metricType
    }
    throw new IllegalArgumentException(
      s"ComputeModelStatistics evaluationMetric '$metric' is not valid for $metricTypeName data. " +
        s"Valid values: $validValues.")
  }

  private def getLabelLevels(schema: StructType, labelColumnName: String): Option[Array[_]] = {
    if (schema.fieldNames.contains(labelColumnName)) CategoricalUtilities.getLevels(schema, labelColumnName)
    else None
  }

  private def getClassificationColumns(labelLevels: Option[Array[_]]): List[String] = {
    // Keep the legacy common-metrics schema when cardinality is unknown or multiclass.
    if (labelLevels.exists(_.length <= 2)) MetricConstants.BinaryClassificationColumns
    else MetricConstants.ClassificationColumns
  }

  private def validateBinaryOnlyMetricSchema(labelLevels: Option[Array[_]],
                                             scoreValueKind: String): Unit = {
    val isBinaryOnlyClassificationMetric =
      getEvaluationMetric == MetricConstants.AucSparkMetric ||
        getEvaluationMetric == MetricConstants.AreaUnderROCMetric ||
        getEvaluationMetric == MetricConstants.AreaUnderPRMetric
    // Match runtime semantics: only SynapseML categorical metadata establishes label cardinality.
    if (scoreValueKind == SchemaConstants.ClassificationKind &&
        isBinaryOnlyClassificationMetric &&
        labelLevels.exists(_.length > 2)) {
      throw new IllegalArgumentException(getBinaryOnlyMetricMulticlassError(getEvaluationMetric))
    }
  }

  private def getBinaryOnlyMetricMulticlassError(metric: String): String = metric match {
    case MetricConstants.AucSparkMetric | MetricConstants.AreaUnderROCMetric =>
      "Error: AUC is not available for multiclass case"
    case MetricConstants.AreaUnderPRMetric =>
      "Error: areaUnderPR is not available for multiclass case"
    case _ =>
      throw new IllegalArgumentException(s"Error: $metric is not a classification metric")
  }

  private def getTransformedSchema(columns: List[String],
                                   metricType: String,
                                   validMetrics: Set[String]): StructType = {
    getEvaluationMetric match {
      case allMetrics if allMetrics == MetricConstants.AllSparkMetrics ||
                         allMetrics == MetricConstants.ClassificationMetricsName ||
                         allMetrics == MetricConstants.RegressionMetricsName =>
        StructType(columns.map(StructField(_, DoubleType)))
      case metric: String if validMetrics.contains(metric) &&
                             MetricConstants.MetricToColumnName.contains(metric) =>
        StructType(Array(StructField(MetricConstants.MetricToColumnName(metric), DoubleType)))
      case default =>
        throwOnInvalidEvaluationMetric(default, metricType, validMetrics)
    }
  }
}

/** Helper class for logging metrics to log4j.
  * @param uid The unique id of the parent module caller.
  */
class MetricsLogger(uid: String) {

  lazy val logger: Logger = Logger.getLogger(this.getClass.getName)

  def logClassificationMetrics(accuracy: Double, precision: Double, recall: Double): Unit = {
    val metrics = MetricData.create(
        Map(MetricConstants.AccuracyColumnName -> accuracy,
            MetricConstants.PrecisionColumnName -> precision,
            MetricConstants.RecallColumnName -> recall),
        "Classification Metrics", uid)
    logger.info(metrics)
  }

  def logRegressionMetrics(mse: Double, rmse: Double, r2: Double, mae: Double): Unit = {
    val metrics = MetricData.create(
        Map(MetricConstants.MseColumnName -> mse,
            MetricConstants.RmseColumnName -> rmse,
            MetricConstants.R2ColumnName -> r2,
            MetricConstants.MaeColumnName -> mae),
        "Regression Metrics", uid)
    logger.info(metrics)
  }

  def logAUC(auc: Double): Unit = {
    val metrics = MetricData.create(Map(MetricConstants.AucColumnName -> auc), "AUC Metric", uid)
    logger.info(metrics)
  }

  def logAreaUnderPR(areaUnderPR: Double): Unit = {
    val metrics = MetricData.create(
      Map(MetricConstants.AreaUnderPRColumnName -> areaUnderPR), "AreaUnderPR Metric", uid)
    logger.info(metrics)
  }

  def logROC(roc: DataFrame): Unit = {
    val metrics = MetricData.createTable(
      Map(MetricConstants.TpRateROCLog ->
            roc.select(MetricConstants.TpRateROCColumnName)
               .collect()
               .map(row => row(0).asInstanceOf[Double])
               .toSeq,
          MetricConstants.FpRateROCLog ->
            roc.select(MetricConstants.FpRateROCColumnName)
               .collect()
               .map(row => row(0).asInstanceOf[Double])
               .toSeq),
      "ROC Metric", uid)
    logger.info(metrics)
  }

}
