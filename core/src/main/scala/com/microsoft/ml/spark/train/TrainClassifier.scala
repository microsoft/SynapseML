// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.train

import java.util.UUID
import com.microsoft.ml.spark.codegen.Wrappable
import com.microsoft.ml.spark.core.schema.{CategoricalUtilities, SchemaConstants, SparkSchema}
import com.microsoft.ml.spark.core.utils.CastUtilities._
import com.microsoft.ml.spark.featurize.{Featurize, FeaturizeUtilities, ValueIndexer, ValueIndexerModel}
import com.microsoft.ml.spark.logging.BasicLogging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/** Trains a classification model.  Featurizes the given data into a vector of doubles.
  *
  * Note the behavior of the reindex and labels parameters, the parameters interact as:
  *
  *   reindex -> false
  *   labels -> false (Empty)
  * Assume all double values, don't use metadata, assume natural ordering
  *
  *   reindex -> true
  *   labels -> false (Empty)
  * Index, use natural ordering of string indexer
  *
  *   reindex -> false
  *   labels -> true (Specified)
  * Assume user knows indexing, apply label values. Currently only string type supported.
  *
  *   reindex -> true
  *   labels -> true (Specified)
  * Validate labels matches column type, try to recast to label type, reindex label column
  *
  * The currently supported classifiers are:
  * Logistic Regression Classifier
  * Decision Tree Classifier
  * Random Forest Classifier
  * Gradient Boosted Trees Classifier
  * Naive Bayes Classifier
  * Multilayer Perceptron Classifier
  * In addition to any generic learner that inherits from Predictor.
  */
class TrainClassifier(override val uid: String) extends AutoTrainer[TrainedClassifierModel] with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("TrainClassifier"))

  /** Doc for model to run.
    */
  override def modelDoc: String = "Classifier to run"

  /** Specifies whether to reindex the given label column.
    * See class documentation for how this parameter interacts with specified labels.
    * @group param
    */
  val reindexLabel = new BooleanParam(this, "reindexLabel", "Re-index the label column")
  setDefault(reindexLabel -> true)
  /** @group getParam */
  def getReindexLabel: Boolean = $(reindexLabel)
  /** @group setParam */
  def setReindexLabel(value: Boolean): this.type = set(reindexLabel, value)

  /** Specifies the labels metadata on the column.
    * See class documentation for how this parameter interacts with reindex labels parameter.
    * @group param
    */
  val labels = new StringArrayParam(this, "labels", "Sorted label values on the labels column")
  /** @group getParam */
  def getLabels: Array[String] = $(labels)
  /** @group setParam */
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /** Optional parameter, specifies the name of the features column passed to the learner.
    * Must have a unique name different from the input columns.
    * By default, set to <uid>_features.
    * @group param
    */
  setDefault(featuresCol, this.uid + "_features")

  /** Fits the classification model.
    *
    * @param dataset The input dataset to train.
    * @return The trained classification model.
    */
  override def fit(dataset: Dataset[_]): TrainedClassifierModel = {
    logFit({
      val labelValues =
        if (isDefined(labels)) {
          Some(getLabels)
        } else {
          None
        }
      // Convert label column to categorical on train, remove rows with missing labels
      val (convertedLabelDataset, levels) = convertLabel(dataset, getLabelCol, labelValues)

      val (oneHotEncodeCategoricals, modifyInputLayer, numFeatures) = getFeaturizeParams

      var classifier: Estimator[_ <: PipelineStage] = getModel match {
        case logisticRegressionClassifier: LogisticRegression =>
          if (levels.isDefined && levels.get.length > 2) {
            new OneVsRest()
              .setClassifier(
                logisticRegressionClassifier
                  .setLabelCol(getLabelCol)
                  .setFeaturesCol(getFeaturesCol))
              .setLabelCol(getLabelCol)
              .setFeaturesCol(getFeaturesCol)
          } else {
            logisticRegressionClassifier
          }
        case gradientBoostedTreesClassifier: GBTClassifier =>
          if (levels.isDefined && levels.get.length > 2) {
            throw new Exception("Multiclass Gradient Boosted Tree Classifier not supported yet")
          } else {
            gradientBoostedTreesClassifier
          }
        case default@defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] =>
          default
        case _ => throw new Exception("Unsupported learner type " + getModel.getClass.toString)
      }

      classifier = classifier match {
        case predictor: Predictor[_, _, _] =>
          predictor
            .setLabelCol(getLabelCol)
            .setFeaturesCol(getFeaturesCol).asInstanceOf[Estimator[_ <: PipelineStage]]
        case default@defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] =>
          // assume label col and features col already set
          default
      }

      val featuresToHashTo =
        if (getNumFeatures != 0) {
          getNumFeatures
        } else {
          numFeatures
        }

      val featureColumns = convertedLabelDataset.columns.filter(col => col != getLabelCol).toSeq

      val featurizer = new Featurize()
        .setOutputCol(getFeaturesCol)
        .setInputCols(featureColumns.toArray)
        .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
        .setNumFeatures(featuresToHashTo)
      val featurizedModel = featurizer.fit(convertedLabelDataset)
      val processedData = featurizedModel.transform(convertedLabelDataset)

      processedData.cache()

      // For neural network, need to modify input layer so it will automatically work during train
      if (modifyInputLayer) {
        val multilayerPerceptronClassifier = classifier.asInstanceOf[MultilayerPerceptronClassifier]
        val row = processedData.take(1)(0)
        val featuresVector = row.get(row.fieldIndex(getFeaturesCol))
        val vectorSize = featuresVector.asInstanceOf[linalg.Vector].size
        multilayerPerceptronClassifier.getLayers(0) = vectorSize
        multilayerPerceptronClassifier.setLayers(multilayerPerceptronClassifier.getLayers)
      }

      // Train the learner
      val fitModel = classifier.fit(processedData)

      processedData.unpersist()

      // Note: The fit shouldn't do anything here
      val pipelineModel = new Pipeline().setStages(Array(featurizedModel, fitModel)).fit(convertedLabelDataset)
      val model = new TrainedClassifierModel()
        .setLabelCol(getLabelCol)
        .setModel(pipelineModel)
        .setFeaturesCol(getFeaturesCol)

      levels.map(l => model.setLevels(l.toArray)).getOrElse(model)
    })
  }

  def getFeaturizeParams: (Boolean, Boolean, Int) = {
    var oneHotEncodeCategoricals = true
    var modifyInputLayer = false
    // Create trainer based on the pipeline stage and set the parameters
    val numFeatures: Int = getModel match {
      case _: DecisionTreeClassifier | _: GBTClassifier | _: RandomForestClassifier =>
        oneHotEncodeCategoricals = false
        FeaturizeUtilities.NumFeaturesTreeOrNNBased
      case _: MultilayerPerceptronClassifier =>
        modifyInputLayer = true
        FeaturizeUtilities.NumFeaturesTreeOrNNBased
      case _ =>
        FeaturizeUtilities.NumFeaturesDefault
    }
    (oneHotEncodeCategoricals, modifyInputLayer, numFeatures)
  }

  def convertLabel(dataset: Dataset[_],
                   labelColumn: String,
                   labelValues: Option[Array[_]]): (DataFrame, Option[Array[_]]) = {
    var levels: Option[Array[_]] = None
    if (getReindexLabel) {

      val dataframe = dataset.toDF().na.drop(Seq(labelColumn))

      if (labelValues.isDefined) {
        if (SparkSchema.isCategorical(dataframe, labelColumn)) {
          throw new Exception("Column is already categorical, cannot set label values")
        }
        // Reindex is true, and labels are given, set levels, make column categorical given the levels
        val labelDataType = dataset.schema(labelColumn).dataType
        // Cast the labels to the given data type, validate labels match column type
        labelValues.get.map(value => value.toDataType(labelDataType))
        levels = labelValues
        // Reindex the column to be categorical with given metadata
        val reindexedDF = new ValueIndexerModel()
          .setLevels(levels.get)
          .setDataType(labelDataType)
          .setInputCol(labelColumn)
          .setOutputCol(labelColumn)
          .transform(dataframe)
        (reindexedDF, levels)
      } else {
        if (!SparkSchema.isCategorical(dataframe, labelColumn)) {
          val model = new ValueIndexer().setInputCol(labelColumn).setOutputCol(labelColumn).fit(dataframe)
          val categoricalLabelDataset = model.transform(dataframe)
          levels = CategoricalUtilities.getLevels(categoricalLabelDataset.schema, labelColumn)
          (categoricalLabelDataset.withColumn(labelColumn,
            categoricalLabelDataset(labelColumn).cast(DoubleType).as(labelColumn,
              categoricalLabelDataset.schema(labelColumn).metadata)), levels)
        } else {
          levels = CategoricalUtilities.getLevels(dataframe.schema, labelColumn)
          (dataframe, levels)
        }
      }
    } else {
      if (labelValues.isDefined) {
        // Reindex is false, set label metadata (only strings supported, since we cannot infer types) on
        // column directly
        levels = labelValues
      }
      (dataset.na.drop(Seq(labelColumn)), levels)
    }
  }

  override def copy(extra: ParamMap): Estimator[TrainedClassifierModel] = {
    setModel(getModel.copy(extra))
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val hasScoreCols =
      $(model) match {
        case _: GBTClassifier => false
        case _: MultilayerPerceptronClassifier => false
        case _ => true
      }
    TrainClassifier.validateTransformSchema(hasScoreCols, schema)
  }
}

object TrainClassifier extends ComplexParamsReadable[TrainClassifier] {
  def validateTransformSchema(hasScoreCols: Boolean, schema: StructType): StructType = {
    val scoresSchema =
      if (hasScoreCols) {
        StructType(schema.fields :+ StructField(SchemaConstants.ScoresColumn, DoubleType))
      } else schema
    val probSchema =
      if (hasScoreCols) {
        StructType(scoresSchema.fields :+ StructField(SchemaConstants.ScoredProbabilitiesColumn, DoubleType))
      } else scoresSchema
    StructType(probSchema.fields :+ StructField(SchemaConstants.ScoredLabelsColumn, DoubleType))
  }
}

/** Model produced by [[TrainClassifier]]. */
class TrainedClassifierModel(val uid: String)
    extends AutoTrainedModel[TrainedClassifierModel] with Wrappable with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("TrainClassifierModel"))

  val levels = new UntypedArrayParam(this, "levels", "the levels")

  def getLevels: Array[Any] = $(levels)

  def setLevels(v: Array[Any]): this.type = set(levels, v)

  override def copy(extra: ParamMap): TrainedClassifierModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      val hasScoreCols = hasScoreColumns(getLastStage)

      // re-featurize and score the data
      val scoredData = getModel.transform(dataset)

      // Drop the vectorized features column
      val cleanedScoredData = scoredData.drop(getFeaturesCol)

      // Update the schema - TODO: create method that would generate GUID and add it to the scored model
      val moduleName = SchemaConstants.ScoreModelPrefix + UUID.randomUUID().toString
      val labelColumnExists = cleanedScoredData.columns.contains(getLabelCol)
      val schematizedScoredDataWithLabel =
        if (labelColumnExists) {
          SparkSchema.setLabelColumnName(cleanedScoredData, moduleName, getLabelCol, SchemaConstants.ClassificationKind)
        } else {
          cleanedScoredData
        }

      // Note: The GBT model does not have scores, only scored labels.  Same for OneVsRest with any binary model.
      val schematizedScoredDataWithScores =
        if (hasScoreCols) {
          setMetadataForColumnName(SparkSchema.setScoredProbabilitiesColumnName,
            SchemaConstants.SparkProbabilityColumn,
            SchemaConstants.ScoredProbabilitiesColumn,
            moduleName,
            setMetadataForColumnName(SparkSchema.setScoresColumnName,
              SchemaConstants.SparkRawPredictionColumn,
              SchemaConstants.ScoresColumn,
              moduleName,
              schematizedScoredDataWithLabel))
        } else schematizedScoredDataWithLabel

      val scoredDataWithUpdatedScoredLabels =
        setMetadataForColumnName(SparkSchema.setScoredLabelsColumnName,
          SchemaConstants.SparkPredictionColumn,
          SchemaConstants.ScoredLabelsColumn,
          moduleName,
          schematizedScoredDataWithScores)

      val scoredDataWithUpdatedScoredLevels =
        if (get(levels).isEmpty) scoredDataWithUpdatedScoredLabels
        else CategoricalUtilities.setLevels(scoredDataWithUpdatedScoredLabels,
          SchemaConstants.ScoredLabelsColumn,
          getLevels)

      // add metadata to the scored labels and true labels for the levels in label column
      if (get(levels).isEmpty || !labelColumnExists) scoredDataWithUpdatedScoredLevels
      else CategoricalUtilities.setLevels(scoredDataWithUpdatedScoredLevels,
        getLabelCol,
        getLevels)
    })
  }

  private def setMetadataForColumnName(setter: (DataFrame, String, String, String) => DataFrame,
                                       sparkColumnName: String,
                                       mmlColumnName: String,
                                       moduleName: String,
                                       dataset: DataFrame): DataFrame = {
    if (dataset.columns.contains(sparkColumnName)) {
      setter(dataset.withColumnRenamed(sparkColumnName, mmlColumnName),
        moduleName,
        mmlColumnName,
        SchemaConstants.ClassificationKind)
    } else {
      dataset
    }
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    TrainClassifier.validateTransformSchema(hasScoreColumns(getLastStage), schema)

  def hasScoreColumns(model: Transformer): Boolean = {
    model match {
      case _: GBTClassificationModel => false
      case _: MultilayerPerceptronClassificationModel => false
      case _ => true
    }
  }
}

object TrainedClassifierModel extends ComplexParamsReadable[TrainedClassifierModel]
