// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.train

import java.util.UUID

import com.microsoft.ml.spark.core.env.InternalWrapper
import com.microsoft.ml.spark.core.schema.{SchemaConstants, SparkSchema}
import com.microsoft.ml.spark.core.serialize.ConstructorReadable
import com.microsoft.ml.spark.featurize.{Featurize, FeaturizeUtilities}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.util._
import org.apache.spark.ml._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/** Trains a regression model. */
@InternalWrapper
class TrainRegressor(override val uid: String) extends AutoTrainer[TrainedRegressorModel] {

  def this() = this(Identifiable.randomUID("TrainRegressor"))

  /** Doc for model to run.
    */
  override def modelDoc: String = "Regressor to run"

  /** Optional parameter, specifies the name of the features column passed to the learner.
    * Must have a unique name different from the input columns.
    * By default, set to <uid>_features.
    * @group param
    */
  setDefault(featuresCol, this.uid + "_features")

  /** Fits the regression model.
    *
    * @param dataset The input dataset to train.
    * @return The trained regression model.
    */
  override def fit(dataset: Dataset[_]): TrainedRegressorModel = {
    val labelColumn = getLabelCol
    var oneHotEncodeCategoricals = true

    val numFeatures: Int = getModel match {
      case _: DecisionTreeRegressor | _: GBTRegressor | _: RandomForestRegressor =>
        oneHotEncodeCategoricals = false
        FeaturizeUtilities.NumFeaturesTreeOrNNBased
      case _ =>
        FeaturizeUtilities.NumFeaturesDefault
    }

    val regressor = getModel match {
      case predictor: Predictor[_, _, _] => {
        predictor
          .setLabelCol(getLabelCol)
          .setFeaturesCol(getFeaturesCol).asInstanceOf[Estimator[_ <: PipelineStage]]
      }
      case default@defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] => {
        // assume label col and features col already set
        default
      }
      case _ => throw new Exception("Unsupported learner type " + getModel.getClass.toString)
    }

    val featuresToHashTo =
      if (getNumFeatures != 0) {
        getNumFeatures
      } else {
        numFeatures
      }

    // TODO: Handle DateType, TimestampType and DecimalType for label
    // Convert the label column during train to the correct type and drop missings
    val convertedLabelDataset = dataset.withColumn(labelColumn,
      dataset.schema(labelColumn).dataType match {
        case _: IntegerType |
             _: BooleanType |
             _: FloatType |
             _: ByteType |
             _: LongType |
             _: ShortType => {
          dataset(labelColumn).cast(DoubleType)
        }
        case _: StringType => {
          throw new Exception("Invalid type: Regressors are not able to train on a string label column: " + labelColumn)
        }
        case _: DoubleType => {
          dataset(labelColumn)
        }
        case default => throw new Exception("Unknown type: " + default.typeName + ", for label column: " + labelColumn)
      }
    ).na.drop(Seq(labelColumn))

    val featureColumns = convertedLabelDataset.columns.filter(col => col != labelColumn).toSeq

    val featurizer = new Featurize()
      .setFeatureColumns(Map(getFeaturesCol -> featureColumns))
      .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
      .setNumberOfFeatures(featuresToHashTo)

    val featurizedModel = featurizer.fit(convertedLabelDataset)
    val processedData   = featurizedModel.transform(convertedLabelDataset)

    processedData.cache()

    // Train the learner
    val fitModel = regressor.fit(processedData)

    processedData.unpersist()

    // Note: The fit shouldn't do anything here
    val pipelineModel = new Pipeline().setStages(Array(featurizedModel, fitModel)).fit(convertedLabelDataset)
    new TrainedRegressorModel(uid, labelColumn, pipelineModel, getFeaturesCol)
  }

  override def copy(extra: ParamMap): Estimator[TrainedRegressorModel] = {
    setModel(getModel.copy(extra))
    defaultCopy(extra)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = TrainRegressor.validateTransformSchema(schema)
}

object TrainRegressor extends ComplexParamsReadable[TrainRegressor] {
  def validateTransformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(SchemaConstants.ScoresColumn, DoubleType))
  }
}

/** Model produced by [[TrainRegressor]].
  * @param uid The id of the module
  * @param labelColumn The label column
  * @param model The trained model
  * @param featuresColumn The features column
  */
@InternalWrapper
class TrainedRegressorModel(val uid: String,
                            val labelColumn: String,
                            override val model: PipelineModel,
                            val featuresColumn: String)
    extends AutoTrainedModel[TrainedRegressorModel](model) {

  val ttag: TypeTag[TrainedRegressorModel] = typeTag[TrainedRegressorModel]
  val objectsToSave: List[Any] = List(uid, labelColumn, model, featuresColumn)

  override def copy(extra: ParamMap): TrainedRegressorModel =
    new TrainedRegressorModel(uid, labelColumn, model.copy(extra), featuresColumn)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // re-featurize and score the data
    val scoredData = model.transform(dataset)

    // Drop the vectorized features column
    val cleanedScoredData = scoredData.drop(featuresColumn)

    // Update the schema - TODO: create method that would generate GUID and add it to the scored model
    val moduleName = SchemaConstants.ScoreModelPrefix + UUID.randomUUID().toString
    val labelColumnExists = cleanedScoredData.columns.contains(labelColumn)
    val schematizedScoredDataWithLabel =
      if (!labelColumnExists) cleanedScoredData
      else SparkSchema.setLabelColumnName(
        cleanedScoredData, moduleName, labelColumn, SchemaConstants.RegressionKind)

    SparkSchema.setScoresColumnName(
      schematizedScoredDataWithLabel.withColumnRenamed(
        SchemaConstants.SparkPredictionColumn,
        SchemaConstants.ScoresColumn),
      moduleName,
      SchemaConstants.ScoresColumn,
      SchemaConstants.RegressionKind)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    TrainRegressor.validateTransformSchema(schema)
}

object TrainedRegressorModel extends ConstructorReadable[TrainedRegressorModel]
