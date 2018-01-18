// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.ml.spark.contracts.MetricData
import com.microsoft.ml.spark.schema.SchemaConstants._
import com.microsoft.ml.spark.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

/** Contains constants used by Compute Model Statistics. */
object ComputeModelStatistics extends DefaultParamsReadable[ComputeModelStatistics] {
  // Regression metrics
  val MseSparkMetric  = "mse"
  val RmseSparkMetric = "rmse"
  val R2SparkMetric   = "r2"
  val MaeSparkMetric  = "mae"
  val RegressionMetrics = "regression"

  val regressionMetrics = Set(MseSparkMetric, RmseSparkMetric, R2SparkMetric,
                              MaeSparkMetric, RegressionMetrics)

  // Binary Classification metrics
  val AreaUnderROCMetric   = "areaUnderROC"
  val AucSparkMetric       = "AUC"
  val AccuracySparkMetric  = "accuracy"
  val PrecisionSparkMetric = "precision"
  val RecallSparkMetric    = "recall"
  val ClassificationMetrics = "classification"

  val classificationMetrics = Set(AreaUnderROCMetric, AucSparkMetric, AccuracySparkMetric,
                                  PrecisionSparkMetric, RecallSparkMetric, ClassificationMetrics)

  val AllSparkMetrics      = "all"

  // Regression column names
  val MseColumnName  = "mean_squared_error"
  val RmseColumnName = "root_mean_squared_error"
  val R2ColumnName   = "R^2"
  val MaeColumnName  = "mean_absolute_error"

  // Binary Classification column names
  val AucColumnName = "AUC"

  // Binary and Multiclass (micro-averaged) column names
  val PrecisionColumnName = "precision"
  val RecallColumnName    = "recall"
  val AccuracyColumnName  = "accuracy"

  // Multiclass Classification column names
  val AverageAccuracy        = "average_accuracy"
  val MacroAveragedRecall    = "macro_averaged_recall"
  val MacroAveragedPrecision = "macro_averaged_precision"

  // Metric to column name
  val metricToColumnName = Map(AccuracySparkMetric -> AccuracyColumnName,
    PrecisionSparkMetric -> PrecisionColumnName,
    RecallSparkMetric    -> RecallColumnName,
    MseSparkMetric       -> MseColumnName,
    RmseSparkMetric      -> RmseColumnName,
    R2SparkMetric        -> R2ColumnName,
    MaeSparkMetric       -> MaeColumnName)

  val classificationColumns = List(AccuracyColumnName, PrecisionColumnName, RecallColumnName)

  val regressionColumns = List(MseColumnName, RmseColumnName, R2ColumnName, MaeColumnName)

  val ClassificationEvaluationType = "Classification"
  val EvaluationType = "evaluation_type"

  val FpRateROCColumnName = "false_positive_rate"
  val TpRateROCColumnName = "true_positive_rate"

  val FpRateROCLog = "fpr"
  val TpRateROCLog = "tpr"

  val BinningThreshold = 1000

  def isClassificationMetric(metric: String): Boolean = {
    if (regressionMetrics.contains(metric)) false
    else if (classificationMetrics.contains(metric)) true
    else throw new Exception("Invalid metric specified")
  }
}

trait ComputeModelStatisticsParams extends MMLParams {
  /** Metric to evaluate the models with. Default is "all"
    *
    * The metrics that can be chosen are:
    *
    *   For binary classification:
    *     - AreaUnderROC
    *     - AUC
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
    *   If using a native Spark ML model, you will need to specify either "classifier" or "regressor"
    *     - classifier
    *     - regressor
    *
    * @group param
    */
  val evaluationMetric: Param[String] =
    StringParam(this, "evaluationMetric", "Metric to evaluate models with",
                ComputeModelStatistics.AllSparkMetrics)

  /** @group getParam */
  def getEvaluationMetric: String = $(evaluationMetric)

  /** @group setParam */
  def setEvaluationMetric(value: String): this.type = set(evaluationMetric, value)

  val scoredLabelsCol =
    StringParam(this, "scoredLabelsCol", "Scored labels column name, only required if using SparkML estimators")

  def getScoredLabelsCol: String = $(scoredLabelsCol)

  def setScoredLabelsCol(value: String): this.type = set(scoredLabelsCol, value)

  val scoresCol =
    StringParam(this, "scoresCol", "Scores or raw prediction column name, only required if using SparkML estimators")

  def getScoresCol: String = $(scoresCol)

  def setScoresCol(value: String): this.type = set(scoresCol, value)
}

/** Evaluates the given scored dataset. */
class ComputeModelStatistics(override val uid: String) extends Transformer with ComputeModelStatisticsParams
  with HasLabelCol {

  def this() = this(Identifiable.randomUID("ComputeModelStatistics"))

  /** The ROC curve evaluated for a binary classifier. */
  var rocCurve: DataFrame = null

  lazy val metricsLogger = new MetricsLogger(uid)

  /** Calculates the metrics for the given dataset and model.
    * @param dataset
    * @return DataFrame whose columns contain the calculated metrics
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val (modelName, labelColumnName, scoreValueKind) = getSchemaInfo(dataset.schema)

    // For creating the result dataframe in classification or regression case
    val spark = dataset.sparkSession
    import spark.implicits._

    if (scoreValueKind == SchemaConstants.ClassificationKind) {

      var resultDF: DataFrame =
        Seq(ComputeModelStatistics.ClassificationEvaluationType)
          .toDF(ComputeModelStatistics.EvaluationType)
      val scoredLabelsColumnName =
        if (isDefined(scoredLabelsCol)) getScoredLabelsCol
        else SparkSchema.getScoredLabelsColumnName(dataset.schema, modelName)

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
        val scoresColumnName =
          if (isDefined(scoresCol)) getScoresCol
          else SparkSchema.getScoresColumnName(dataset.schema, modelName)
        if (scoresColumnName == null) predictionAndLabels
        else if (levelsExist) getScoresAndLabels(dataset, labelColumnName, scoresColumnName, levelsToIndexMap)
        else getScalarScoresAndLabels(dataset, labelColumnName, scoresColumnName)
      }

      lazy val (labels: Array[Double], confusionMatrix: Matrix) = createConfusionMatrix(predictionAndLabels)

      // If levels exist, use the extra information they give to get better performance
      getEvaluationMetric match {
        case allMetrics if allMetrics == ComputeModelStatistics.AllSparkMetrics ||
                           allMetrics == ComputeModelStatistics.ClassificationMetrics => {
          resultDF = addConfusionMatrixToResult(labels, confusionMatrix, resultDF)
          resultDF = addAllClassificationMetrics(
              modelName, dataset, labelColumnName, predictionAndLabels,
              confusionMatrix, scoresAndLabels, resultDF)
        }
        case simpleMetric if simpleMetric == ComputeModelStatistics.AccuracySparkMetric ||
                             simpleMetric == ComputeModelStatistics.PrecisionSparkMetric ||
                             simpleMetric == ComputeModelStatistics.RecallSparkMetric => {
          resultDF = addSimpleMetric(simpleMetric, predictionAndLabels, resultDF)
        }
        case ComputeModelStatistics.AucSparkMetric => {
          val numLevels = if (levelsExist) levels.get.length
          else confusionMatrix.numRows
          if (numLevels <= 2) {
            // Add the AUC
            val auc: Double = getAUC(modelName, dataset, labelColumnName, scoresAndLabels)
            resultDF = resultDF.withColumn(ComputeModelStatistics.AucColumnName, lit(auc))
          } else {
            throw new Exception("Error: AUC is not available for multiclass case")
          }
        }
        case default => {
          throw new Exception(s"Error: $default is not a classification metric")
        }
      }
      resultDF
    } else if (scoreValueKind == SchemaConstants.RegressionKind) {
      val scoresColumnName =
        if (isDefined(scoresCol)) getScoresCol
        else SparkSchema.getScoresColumnName(dataset.schema, modelName)

      val scoresAndLabels = selectAndCastToRDD(dataset, scoresColumnName, labelColumnName)

      val regressionMetrics = new RegressionMetrics(scoresAndLabels)

      // get all spark metrics possible: "mse", "rmse", "r2", "mae"
      val mse  = regressionMetrics.meanSquaredError
      val rmse = regressionMetrics.rootMeanSquaredError
      val r2   = regressionMetrics.r2
      val mae  = regressionMetrics.meanAbsoluteError

      metricsLogger.logRegressionMetrics(mse, rmse, r2, mae)

      Seq((mse, rmse, r2, mae)).toDF(ComputeModelStatistics.MseColumnName,
                                     ComputeModelStatistics.RmseColumnName,
                                     ComputeModelStatistics.R2ColumnName,
                                     ComputeModelStatistics.MaeColumnName)
    } else {
      throwOnInvalidScoringKind(scoreValueKind)
    }
  }

  private def getSchemaInfo(schema: StructType): (String, String, String) = {
    val schemaInfo = tryGetSchemaInfo(schema)
    if (schemaInfo.isDefined) {
      schemaInfo.get
    } else {
      if (!isDefined(labelCol) || getEvaluationMetric == ComputeModelStatistics.AllSparkMetrics) {
        throw new Exception("Please score the model prior to evaluating")
      }
      ("custom model", getLabelCol,
       if (ComputeModelStatistics.isClassificationMetric(getEvaluationMetric))
         SchemaConstants.ClassificationKind
       else SchemaConstants.RegressionKind)
    }
  }

  private def tryGetSchemaInfo(schema: StructType): Option[(String, String, String)] = {
    // TODO: evaluate all models; for now, get first model name found
    val firstModelName = schema.collectFirst {
      case StructField(c, t, _, m)
          if (getFirstModelName(m) != null && !getFirstModelName(m).isEmpty) => {
        getFirstModelName(m).get
      }
    }
    if (!firstModelName.isEmpty) {
      val modelName = firstModelName.get
      val labelColumnName = SparkSchema.getLabelColumnName(schema, modelName)
      val scoreValueKind = SparkSchema.getScoreValueKind(schema, modelName, labelColumnName)
      Option((modelName, labelColumnName, scoreValueKind))
    } else {
      None
    }
  }

  private def addSimpleMetric(simpleMetric: String,
                              predictionAndLabels: RDD[(Double, Double)],
                              resultDF: DataFrame): DataFrame = {
    val (labels: Array[Double], confusionMatrix: Matrix) = createConfusionMatrix(predictionAndLabels)
    // Compute metrics for binary classification
    if (confusionMatrix.numCols == 2) {
      val (accuracy: Double, precision: Double, recall: Double) =
        getBinaryAccuracyPrecisionRecall(confusionMatrix)
      metricsLogger.logClassificationMetrics(accuracy, precision, recall)
      // Add the metrics to the DF
      simpleMetric match {
        case ComputeModelStatistics.AccuracySparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.AccuracyColumnName, lit(accuracy))
        case ComputeModelStatistics.PrecisionSparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.PrecisionColumnName, lit(precision))
        case ComputeModelStatistics.RecallSparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.RecallColumnName, lit(recall))
        case default => resultDF
      }
    } else {
      val (microAvgAccuracy: Double, microAvgPrecision: Double, microAvgRecall: Double, _, _, _) =
        getMulticlassMetrics(predictionAndLabels, confusionMatrix)
      metricsLogger.logClassificationMetrics(microAvgAccuracy, microAvgPrecision, microAvgRecall)
      // Add the metrics to the DF
      simpleMetric match {
        case ComputeModelStatistics.AccuracySparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.AccuracyColumnName, lit(microAvgAccuracy))
        case ComputeModelStatistics.PrecisionSparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.PrecisionColumnName, lit(microAvgPrecision))
        case ComputeModelStatistics.RecallSparkMetric =>
          resultDF.withColumn(ComputeModelStatistics.RecallColumnName, lit(microAvgRecall))
        case default => resultDF
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
      // Add the AUC
      val auc: Double = getAUC(modelName, dataset, labelColumnName, scoresAndLabels)
      metricsLogger.logAUC(auc)
      // Add the metrics to the DF
      resultDF
        .withColumn(ComputeModelStatistics.AccuracyColumnName, lit(accuracy))
        .withColumn(ComputeModelStatistics.PrecisionColumnName, lit(precision))
        .withColumn(ComputeModelStatistics.RecallColumnName, lit(recall))
        .withColumn(ComputeModelStatistics.AucColumnName, lit(auc))
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
        .withColumn(ComputeModelStatistics.AccuracyColumnName, lit(microAvgAccuracy))
        .withColumn(ComputeModelStatistics.PrecisionColumnName, lit(microAvgPrecision))
        .withColumn(ComputeModelStatistics.RecallColumnName, lit(microAvgRecall))
        .withColumn(ComputeModelStatistics.AverageAccuracy, lit(averageAccuracy))
        .withColumn(ComputeModelStatistics.MacroAveragedPrecision, lit(macroAveragedPrecision))
        .withColumn(ComputeModelStatistics.MacroAveragedRecall, lit(macroAveragedRecall))
    }
  }

  private def addConfusionMatrixToResult(labels: Array[Double],
                                         confusionMatrix: Matrix,
                                         resultDF: DataFrame): DataFrame = {
    var resultDFModified = resultDF
    for (col: Int <- 0 until confusionMatrix.numCols;
         row: Int <- 0 until confusionMatrix.numRows) {
      resultDFModified = resultDFModified
        .withColumn(s"predicted_class_as_${labels(col).toString}_actual_is_${labels(row).toString}",
          lit(confusionMatrix(row, col)))
    }
    resultDFModified
  }

  private def selectAndCastToDF(dataset: Dataset[_],
                                predictionColumnName: String,
                                labelColumnName: String): DataFrame = {
    dataset.select(col(predictionColumnName), col(labelColumnName).cast(DoubleType))
      .na
      .drop(Array(predictionColumnName, labelColumnName))
  }

  private def selectAndCastToRDD(dataset: Dataset[_],
                                 predictionColumnName: String,
                                 labelColumnName: String): RDD[(Double, Double)] = {
    selectAndCastToDF(dataset, predictionColumnName, labelColumnName)
      .rdd
      .map {
        case Row(prediction: Double, label: Double) => (prediction, label)
        case default => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
  }

  private def getPredictionAndLabels(dataset: Dataset[_],
                                     labelColumnName: String,
                                     scoredLabelsColumnName: String,
                                     levelsToIndexMap: Map[Any, Double]): RDD[(Double, Double)] = {
    // Calculate confusion matrix and output it as DataFrame
    dataset.select(col(scoredLabelsColumnName), col(labelColumnName))
      .na
      .drop(Array(scoredLabelsColumnName, labelColumnName))
      .rdd
      .map {
        case Row(prediction: Double, label) => (prediction, levelsToIndexMap(label))
        case default => throw new Exception(s"Error: prediction and label columns invalid or missing")
    }
  }

  private def getScalarScoresAndLabels(dataset: Dataset[_],
                                       labelColumnName: String,
                                       scoresColumnName: String): RDD[(Double, Double)] = {
    selectAndCastToDF(dataset, scoresColumnName, labelColumnName)
      .rdd
      .map {
        case Row(prediction: Vector, label: Double) => (prediction(1), label)
        case default => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
  }

  private def getScoresAndLabels(dataset: Dataset[_],
                         labelColumnName: String,
                         scoresColumnName: String,
                         levelsToIndexMap: Map[Any, Double]): RDD[(Double, Double)] = {
    dataset.select(col(scoresColumnName), col(labelColumnName))
      .na
      .drop(Array(scoresColumnName, labelColumnName))
      .rdd
      .map {
        case Row(prediction: Vector, label) => (prediction(1), levelsToIndexMap(label))
        case default => throw new Exception(s"Error: prediction and label columns invalid or missing")
      }
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

  private def getAUC(modelName: String,
             dataset: Dataset[_],
             labelColumnName: String,
             scoresAndLabels: RDD[(Double, Double)]): Double = {
    val binaryMetrics = new BinaryClassificationMetrics(scoresAndLabels,
      ComputeModelStatistics.BinningThreshold)

    val spark = dataset.sparkSession
    import spark.implicits._

    rocCurve = binaryMetrics.roc()
      .toDF(ComputeModelStatistics.FpRateROCColumnName, ComputeModelStatistics.TpRateROCColumnName)
    metricsLogger.logROC(rocCurve)
    val auc = binaryMetrics.areaUnderROC()
    metricsLogger.logAUC(auc)
    auc
  }

  private def getBinaryAccuracyPrecisionRecall(confusionMatrix: Matrix): (Double, Double, Double) = {
    val TP: Double = confusionMatrix(1, 1)
    val FP: Double = confusionMatrix(0, 1)
    val TN: Double = confusionMatrix(0, 0)
    val FN: Double = confusionMatrix(1, 0)

    val accuracy: Double = (TP + TN) / (TP + TN + FP + FN)
    val precision: Double = TP / (TP + FP)
    val recall: Double = TP / (TP + FN)
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

  private def getFirstModelName(colMetadata: Metadata): Option[String] = {
    if (!colMetadata.contains(MMLTag)) null
    else {
      val mlTagMetadata = colMetadata.getMetadata(MMLTag)
      val metadataKeys = MetadataUtilities.getMetadataKeys(mlTagMetadata)
      metadataKeys.find(key => key.startsWith(SchemaConstants.ScoreModelPrefix))
    }
  }

  override def copy(extra: ParamMap): Transformer = new ComputeModelStatistics()

  override def transformSchema(schema: StructType): StructType = {
    val (_, _, scoreValueKind) = getSchemaInfo(schema)
    val columns =
      if (scoreValueKind == SchemaConstants.ClassificationKind) ComputeModelStatistics.classificationColumns
      else if (scoreValueKind == SchemaConstants.RegressionKind) ComputeModelStatistics.regressionColumns
      else throwOnInvalidScoringKind(scoreValueKind)
    getTransformedSchema(columns, scoreValueKind)

  }

  private def throwOnInvalidScoringKind(scoreValueKind: String) = {
    throw new Exception(s"Error: unknown scoring kind $scoreValueKind")
  }

  private def getTransformedSchema(columns: List[String], metricType: String) = {
    getEvaluationMetric match {
      case allMetrics if allMetrics == ComputeModelStatistics.AllSparkMetrics ||
                         allMetrics == ComputeModelStatistics.ClassificationMetrics ||
                         allMetrics == ComputeModelStatistics.RegressionMetrics =>
        StructType(columns.map(StructField(_, DoubleType)))
      case metric: String if (ComputeModelStatistics.metricToColumnName.contains(metric)) &&
                             columns.contains(ComputeModelStatistics.metricToColumnName(metric)) =>
        StructType(Array(StructField(ComputeModelStatistics.metricToColumnName(metric), DoubleType)))
      case default =>
        throw new Exception(s"Error: $default is not a $metricType metric")
    }
  }
}

/** Helper class for logging metrics to log4j.
  * @param uid The unique id of the parent module caller.
  */
class MetricsLogger(uid: String) {

  lazy val logger = Logger.getLogger(this.getClass.getName)

  def logClassificationMetrics(accuracy: Double, precision: Double, recall: Double): Unit = {
    val metrics = MetricData.create(Map(ComputeModelStatistics.AccuracyColumnName -> accuracy,
      ComputeModelStatistics.PrecisionColumnName -> precision,
      ComputeModelStatistics.RecallColumnName -> recall), "Classification Metrics", uid)
    logger.info(metrics)
  }

  def logRegressionMetrics(mse: Double, rmse: Double, r2: Double, mae: Double): Unit = {
    val metrics = MetricData.create(Map(ComputeModelStatistics.MseColumnName -> mse,
      ComputeModelStatistics.RmseColumnName -> rmse,
      ComputeModelStatistics.R2ColumnName -> r2,
      ComputeModelStatistics.MaeColumnName -> mae), "Regression Metrics", uid)
    logger.info(metrics)
  }

  def logAUC(auc: Double): Unit = {
    val metrics = MetricData.create(Map(ComputeModelStatistics.AucColumnName -> auc), "AUC Metric", uid)
    logger.info(metrics)
  }

  def logROC(roc: DataFrame): Unit = {
    val metrics = MetricData.createTable(
      Map(
        ComputeModelStatistics.TpRateROCLog ->
          roc.select(ComputeModelStatistics.TpRateROCColumnName)
            .collect()
            .map(row => row(0).asInstanceOf[Double]).toSeq,
        ComputeModelStatistics.FpRateROCLog ->
          roc.select(ComputeModelStatistics.FpRateROCColumnName)
            .collect()
            .map(row => row(0).asInstanceOf[Double]).toSeq
      ),
      "ROC Metric",
      uid)
    logger.info(metrics)
  }

}
