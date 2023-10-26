// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.train

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.core.schema.{SchemaConstants, SparkSchema}
import com.microsoft.azure.synapse.ml.featurize.{Featurize, FeaturizeUtilities}
import com.microsoft.azure.synapse.ml.logging.{FeatureNames, SynapseMLLogging}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import java.util.UUID

/** Trains a regression model. */
class TrainRegressor(override val uid: String) extends AutoTrainer[TrainedRegressorModel] with SynapseMLLogging {
  logClass(FeatureNames.Core)

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
  //scalastyle:off method.length
  //scalastyle:off cyclomatic.complexity
  override def fit(dataset: Dataset[_]): TrainedRegressorModel = {
    logFit({
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
        case predictor: Predictor[_, _, _] =>
          predictor
            .setLabelCol(getLabelCol)
            .setFeaturesCol(getFeaturesCol).asInstanceOf[Estimator[_ <: PipelineStage]]
        case default@defaultType if defaultType.isInstanceOf[Estimator[_ <: PipelineStage]] =>
          // assume label col and features col already set
          default
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
               _: ShortType |
               _: StringType =>
            dataset(labelColumn).cast(DoubleType)
          case _: DoubleType =>
            dataset(labelColumn)
          case default => throw new Exception("Unknown type: " + default.typeName +
            ", for label column: " + labelColumn)
        }
      ).na.drop(Seq(labelColumn))

      val nonFeatureColumns = getLabelCol
      val featureColumns = if (isDefined(inputCols)) {
        getInputCols
      } else {
        convertedLabelDataset.columns.filterNot(nonFeatureColumns.contains)
      }

      val featurizer = new Featurize()
        .setOutputCol(getFeaturesCol)
        .setInputCols(featureColumns)
        .setOneHotEncodeCategoricals(oneHotEncodeCategoricals)
        .setNumFeatures(featuresToHashTo)

      val featurizedModel = featurizer.fit(convertedLabelDataset)

      val processedData = featurizedModel.transform(convertedLabelDataset)

      processedData.cache()

      // Train the learner
      val fitModel = regressor.fit(processedData)

      processedData.unpersist()

      // Note: The fit shouldn't do anything here
      val pipelineModel = new Pipeline().setStages(Array(featurizedModel, fitModel)).fit(convertedLabelDataset)
      new TrainedRegressorModel()
        .setLabelCol(labelColumn)
        .setModel(pipelineModel)
        .setFeaturesCol(getFeaturesCol)
    }, dataset.columns.length)
  }
  //scalastyle:on method.length
  //scalastyle:on cyclomatic.complexity

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
 */
class TrainedRegressorModel(val uid: String)
    extends AutoTrainedModel[TrainedRegressorModel]
      with Wrappable with SynapseMLLogging {
  logClass(FeatureNames.Core)

  def this() = this(Identifiable.randomUID("TrainedRegressorModel"))

  override def copy(extra: ParamMap): TrainedRegressorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      // re-featurize and score the data
      val scoredData = getModel.transform(dataset)

      // Drop the vectorized features column
      val cleanedScoredData = scoredData.drop(getFeaturesCol)

      // Update the schema - TODO: create method that would generate GUID and add it to the scored model
      val moduleName = SchemaConstants.ScoreModelPrefix + UUID.randomUUID().toString
      val labelColumnExists = cleanedScoredData.columns.contains(getLabelCol)
      val schematizedScoredDataWithLabel =
        if (!labelColumnExists) cleanedScoredData
        else SparkSchema.setLabelColumnName(
          cleanedScoredData, moduleName, getLabelCol, SchemaConstants.RegressionKind)
      SparkSchema.updateColumnMetadata(schematizedScoredDataWithLabel,
        moduleName, SchemaConstants.SparkPredictionColumn, SchemaConstants.RegressionKind)
    }, dataset.columns.length)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    TrainRegressor.validateTransformSchema(schema)
}

object TrainedRegressorModel extends ComplexParamsReadable[TrainedRegressorModel]
