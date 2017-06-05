// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.util.UUID

import com.microsoft.ml.spark.schema._
import com.microsoft.ml.spark.CastUtilities._
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.classification._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.EstimatorParam
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Trains a classification model.  Featurizes the given data into a vector of doubles.
  *
  * Note the behavior of the reindex and labels parameters, the parameters interact as:
  *   reindex -> false
  *   labels -> false (Empty)
  * Assume all double values, don't use metadata, assume natural ordering
  *
  *   reindex -> true
  *   labels -> false (Empty)
  * Index, use natural ordering of string indexer
  *   reindex -> false
  *   labels -> true (Specified)
  *
  * Assume user knows indexing, apply label values. Currently only string type supported.
  *   reindex -> true
  *   labels -> true (Specified)
  *
  * Validate labels matches column type, try to recast to label type, reindex label column
  */
class TrainClassifier(override val uid: String) extends Estimator[TrainedClassifierModel]
  with HasLabelCol with MMLParams {

  def this() = this(Identifiable.randomUID("TrainClassifier"))

  val model = new EstimatorParam(this, "model", "Classifier to run")

  def getModel: Estimator[_ <: Model[_]] = $(model)
  /** @group setParam **/
  def setModel(value: Estimator[_ <: Model[_]]): this.type = set(model, value)

  val featuresColumn = this.uid + "_features"

  val numFeatures = IntParam(this, "numFeatures", "number of features to hash to", 0)
  def getNumFeatures: Int = $(numFeatures)
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /**
    * Specifies whether to reindex the given label column.
    * See class documentation for how this parameter interacts with specified labels.
    * @group param
    */
  val reindexLabel = BooleanParam(this, "reindexLabel", "reindex the label column", true)
  def getReindexLabel: Boolean = $(reindexLabel)
  def setReindexLabel(value: Boolean): this.type = set(reindexLabel, value)

  /**
    * Specifies the labels metadata on the column.
    * See class documentation for how this parameter interacts with reindex labels parameter.
    * @group param
    */
  val labels = new StringArrayParam(this, "labels", "sorted label values on the labels column")
  def getLabels: Array[String] = $(labels)
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
    * Fits the classification model.
    *
    * @param dataset The input dataset to train.
    * @return The trained classification model.
    */
  override def fit(dataset: Dataset[_]): TrainedClassifierModel = {
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
      case logisticRegressionClassifier: LogisticRegression => {
        if (levels.isDefined && levels.get.length > 2) {
          new OneVsRest()
            .setClassifier(
              logisticRegressionClassifier
                .setLabelCol(getLabelCol)
                .setFeaturesCol(featuresColumn))
            .setLabelCol(getLabelCol)
            .setFeaturesCol(featuresColumn)
        } else {
          logisticRegressionClassifier
        }
      }
      case gradientBoostedTreesClassifier: GBTClassifier => {
        if (levels.isDefined && levels.get.length > 2) {
          throw new Exception("Multiclass Gradient Boosted Tree Classifier not supported yet")
        } else {
          gradientBoostedTreesClassifier
        }
      }
      case default @ defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] => {
        default
      }
      case _ => throw new Exception("Unsupported learner type " + getModel.getClass.toString)
    }

    classifier = classifier match {
      case predictor: Predictor[_, _, _] => {
        predictor
          .setLabelCol(getLabelCol)
          .setFeaturesCol(featuresColumn).asInstanceOf[Estimator[_ <: PipelineStage]]
      }
      case default @ defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] => {
        // assume label col and features col already set
        default
      }
    }

    val featuresToHashTo =
      if (getNumFeatures != 0) {
        getNumFeatures
      } else {
        numFeatures
      }

    val featureColumns = convertedLabelDataset.columns.filter(col => col != getLabelCol).toSeq

    val featurizer = new Featurize()
      .setFeatureColumns(Map(featuresColumn -> featureColumns))
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumberOfFeatures(featuresToHashTo)
    val featurizedModel = featurizer.fit(convertedLabelDataset)
    val processedData = featurizedModel.transform(convertedLabelDataset)

    processedData.cache()

    // For neural network, need to modify input layer so it will automatically work during train
    if (modifyInputLayer) {
      val multilayerPerceptronClassifier = classifier.asInstanceOf[MultilayerPerceptronClassifier]
      val row = processedData.take(1)(0)
      val featuresVector = row.get(row.fieldIndex(featuresColumn))
      val vectorSize = featuresVector.asInstanceOf[linalg.Vector].size
      multilayerPerceptronClassifier.getLayers(0) = vectorSize
      multilayerPerceptronClassifier.setLayers(multilayerPerceptronClassifier.getLayers)
    }

    // Train the learner
    val fitModel = classifier.fit(processedData)

    processedData.unpersist()

    // Note: The fit shouldn't do anything here
    val pipelineModel = new Pipeline().setStages(Array(featurizedModel, fitModel)).fit(convertedLabelDataset)
    new TrainedClassifierModel(uid, getLabelCol, pipelineModel, levels, featuresColumn)
  }

  def getFeaturizeParams: (Boolean, Boolean, Int) = {
    var oneHotEncodeCategoricals = true
    var modifyInputLayer = false
    // Create trainer based on the pipeline stage and set the parameters
    val numFeatures: Int = getModel match {
      case _: DecisionTreeClassifier | _: GBTClassifier | _: RandomForestClassifier =>
        oneHotEncodeCategoricals = false
        FeaturizeUtilities.numFeaturesTreeOrNNBased
      case _: MultilayerPerceptronClassifier =>
        modifyInputLayer = true
        FeaturizeUtilities.numFeaturesTreeOrNNBased
      case _ =>
        FeaturizeUtilities.numFeaturesDefault
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

  override def copy(extra: ParamMap): Estimator[TrainedClassifierModel] = defaultCopy(extra)

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

object TrainClassifier extends DefaultParamsReadable[TrainClassifier] {
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

/**
  * Model produced by [[TrainClassifier]].
  */
class TrainedClassifierModel(val uid: String,
                             val labelColumn: String,
                             val model: PipelineModel,
                             val levels: Option[Array[_]],
                             val featuresColumn: String)
    extends Model[TrainedClassifierModel] with MLWritable {

  override def write: MLWriter = new TrainedClassifierModel.TrainClassifierModelWriter(uid,
    labelColumn,
    model,
    levels,
    featuresColumn)

  override def copy(extra: ParamMap): TrainedClassifierModel =
    new TrainedClassifierModel(uid,
      labelColumn,
      model.copy(extra),
      levels,
      featuresColumn)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val hasScoreCols = hasScoreColumns(model.stages.last)

    // re-featurize and score the data
    val scoredData = model.transform(dataset)

    // Drop the vectorized features column
    val cleanedScoredData = scoredData.drop(featuresColumn)

    // Update the schema - TODO: create method that would generate GUID and add it to the scored model
    val moduleName = SchemaConstants.ScoreModelPrefix + UUID.randomUUID().toString
    val labelColumnExists = cleanedScoredData.columns.contains(labelColumn)
    val schematizedScoredDataWithLabel =
      if (labelColumnExists) {
        SparkSchema.setLabelColumnName(cleanedScoredData, moduleName, labelColumn, SchemaConstants.ClassificationKind)
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
      if (levels.isEmpty) scoredDataWithUpdatedScoredLabels
      else CategoricalUtilities.setLevels(scoredDataWithUpdatedScoredLabels,
        SchemaConstants.ScoredLabelsColumn,
        levels.get)

    // add metadata to the scored labels and true labels for the levels in label column
    if (levels.isEmpty || !labelColumnExists) scoredDataWithUpdatedScoredLevels
    else CategoricalUtilities.setLevels(scoredDataWithUpdatedScoredLevels,
      labelColumn,
      levels.get)
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
    TrainClassifier.validateTransformSchema(hasScoreColumns(model.stages.last), schema)

  def hasScoreColumns(model: Transformer): Boolean = {
    model match {
      case _: GBTClassificationModel => false
      case _: MultilayerPerceptronClassificationModel => false
      case _ => true
    }
  }

  def getParamMap: ParamMap = model.stages.last.extractParamMap()
}

object TrainedClassifierModel extends MLReadable[TrainedClassifierModel] {

  private val featurizeModelPart = "featurizeModel"
  private val modelPart = "model"
  private val levelsPart = "levels"
  private val dataPart = "data"

  override def read: MLReader[TrainedClassifierModel] = new TrainedClassifierModelReader

  override def load(path: String): TrainedClassifierModel = super.load(path)

  /** [[MLWriter]] instance for [[TrainedClassifierModel]] */
  private[TrainedClassifierModel]
  class TrainClassifierModelWriter(val uid: String,
                                   val labelColumn: String,
                                   val model: PipelineModel,
                                   val levels: Option[Array[_]],
                                   val featuresColumn: String)
    extends MLWriter {
    private case class Data(uid: String, labelColumn: String, featuresColumn: String)

    override protected def saveImpl(path: String): Unit = {
      val overwrite = this.shouldOverwrite
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // Required in order to allow this to be part of an ML pipeline
      PipelineUtilities.saveMetadata(uid,
        TrainedClassifierModel.getClass.getName.replace("$", ""),
        new Path(path, "metadata").toString,
        sc,
        overwrite)

      // save the model
      val modelWriter =
        if (overwrite) model.write.overwrite()
        else model.write
      modelWriter.save(new Path(qualPath, modelPart).toString)

      // save the levels
      ObjectUtilities.writeObject(levels, qualPath, levelsPart, sc, overwrite)

      // save model data
      val data = Data(uid, labelColumn, featuresColumn)
      val dataPath = new Path(qualPath, dataPart).toString
      val saveMode =
        if (overwrite) SaveMode.Overwrite
        else SaveMode.ErrorIfExists
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.mode(saveMode).parquet(dataPath)
    }
  }

  private class TrainedClassifierModelReader
    extends MLReader[TrainedClassifierModel] {

    override def load(path: String): TrainedClassifierModel = {
      val qualPath = PipelineUtilities.makeQualifiedPath(sc, path)
      // load the uid, label column and model name
      val dataPath = new Path(qualPath, dataPart).toString
      val data = sparkSession.read.format("parquet").load(dataPath)
      val Row(uid: String, labelColumn: String, featuresColumn: String) =
        data.select("uid", "labelColumn", "featuresColumn").head()

      // retrieve the underlying model
      val model = PipelineModel.load(new Path(qualPath, modelPart).toString)

      // get the levels
      val levels = ObjectUtilities.loadObject[Option[Array[_]]](qualPath, levelsPart, sc)

      new TrainedClassifierModel(uid, labelColumn, model, levels, featuresColumn)
    }
  }

}
