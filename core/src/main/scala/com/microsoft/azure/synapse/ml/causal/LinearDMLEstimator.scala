// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.train._
import com.microsoft.azure.synapse.ml.core.schema.SchemaConstants
import com.microsoft.azure.synapse.ml.featurize.{Featurize, FeaturizeUtilities}
import com.microsoft.azure.synapse.ml.logging.BasicLogging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{Estimator, _}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GBTRegressor, GeneralizedLinearRegression, GeneralizedLinearRegressionModel, RandomForestRegressor, Regressor}


/** Double ML estimators. The estimator follows the two stage process,
 *  where a set of nuisance functions are estimated in the first stage in a crossfitting manner
 *  and a final stage estimates the conditional average treatment effect (CATE) model.
 *  Our goal is to estimate the constant marginal CATE Theta(X)
 *
 *  In this estimator, the CATE is estimated by using the following estimating equations:
 *  .. math ::
 *      Y - \\E[Y | X, W] = \\Theta(X) \\cdot (T - \\E[T | X, W]) + \\epsilon
 *
 *
 *  Thus if we estimate the nuisance functions :math:`q(X, W) = \\E[Y | X, W]` and
 *  :math:`f(X, W)=\\E[T | X, W]` in the first stage, we can estimate the final stage cate for each
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
 * The nuisance function :math:`q` is a simple regression problem and user
 * can use setOutcomeModel to set an arbitrary sparkml regressor that is internally used to solve this regression problem
 *
 * The problem of estimating the nuisance function :math:`f` is also a regression problem and user
 * can use setTreatmentModel to set an arbitrary sparkml regressor that is internally used to solve this regression problem.
 *
 * If the init flag `discrete_treatment` is set to `True`, then the treatment model is treated as a classifier.
 * The input categorical treatment is one-hot encoded (excluding the lexicographically smallest treatment which is used as the baseline)
 * and the `predict_proba` method of the treatment model classifier is used to residualize the one-hot encoded treatment.

      The final stage is (potentially multi-task) linear regression problem with outcomes the labels
      :math:`\\tilde{Y}` and regressors the composite features
      :math:`\\tilde{T}\\otimes \\phi(X) = \\mathtt{vec}(\\tilde{T}\\cdot \\phi(X)^T)`.
      The :class:`.DML` takes as input parameter
      ``model_final``, which is any linear sparkml regressor that is internally used to solve this
      (multi-task) linear regresion problem.
 */
//noinspection ScalaStyle
class LinearDMLEstimator(override val uid: String)
  extends AutoTrainer[LinearDMLModel]
  with LinearDMLParams
  with BasicLogging {

  logClass()

  def this() = this(Identifiable.randomUID("LinearDMLEstimator"))

  override def modelDoc: String = "LinearDML to run"

  setDefault(featuresCol, this.uid + "_features")

  /** Fits the LinearDML model.
   *
   * @param dataset The input dataset to train.
   * @return The trained LinearDML model.
   */
  override def fit(dataset: Dataset[_]): LinearDMLModel = {
    logFit({

      // Step 1 - Train treatment model and compute treatment residual
      val treatmentClassifier =
        if (getDiscreteTreatment) {
          new TrainClassifier().setModel(getTreatmentModel).setLabelCol(getTreatmentCol).setExcludedFeatureCols(Array(getOutcomeCol))
        } else {
          new TrainRegressor().setModel(getTreatmentModel).setLabelCol(getTreatmentCol)
        }

      // get treatment's prediction column, for classifier we use probability column and for regression we use prediction column
      val treatmentPredictionColName = getTreatmentModel match {
        case classifier: ProbabilisticClassifier[_, _, _] =>
          classifier.getProbabilityCol
        case regressor: Regressor[_, _, _] =>
          regressor.getPredictionCol
      }

      // Compute treatment residual
      val computeTreatmentResiduals =
        new ComputeResidualTransformer()
          .setObservedCol(getTreatmentCol)
          .setPredictedCol(treatmentPredictionColName)
          .setOutputCol(SchemaConstants.TreatmentResidualColumn)

      // Step 2 - Train outcome model and compute outcome residual
      val outcomeRegressor = new TrainRegressor().setModel(getOutcomeModel).setLabelCol(getOutcomeCol).setExcludedFeatureCols(Array(getTreatmentCol))

      // get outcome's prediction column, for classifier we use probability column and for regression we use prediction column
      val outcomePredictionColName = getOutcomeModel match {
        case classifier: ProbabilisticClassifier[_, _, _] =>
          classifier.getProbabilityCol
        case regressor: Regressor[_, _, _] =>
          regressor.getPredictionCol
      }

      // Compute outcome residual
      val computeOutcomeResiduals = new ComputeResidualTransformer()
        .setObservedCol(getOutcomeCol)
        .setPredictedCol(outcomePredictionColName)
        .setOutputCol(SchemaConstants.OutcomeResidualColumn)

      // Execute step 1 and 2 in pipeline
      val pipelineModel = new Pipeline().setStages(
        Array(treatmentClassifier, computeTreatmentResiduals, outcomeRegressor, computeOutcomeResiduals,
        )).fit(dataset)

      // Step 3 - final regression
      val scoredData = pipelineModel.transform(dataset).select("treatment_residual", "outcome_residual")

      val convertedLabelDataset_finalModel = LinearDMLEstimator.convertRegressorLabel(scoredData, "outcome_residual")
      val regressor = new GeneralizedLinearRegression().setFamily("gaussian").setLink("identity").setFitIntercept(false)

      val regressor_final = LinearDMLEstimator.getEstimatorWithLabelFeaturesConfigured(regressor, "outcome_residual", getFeaturesCol)
      val (oneHotEncodeCategoricals_finalModel, numFeatures_finalModel, _) = LinearDMLEstimator.getFeaturizeParams(regressor, getNumFeatures)
      val featurizer_finalModel = new Featurize()
        .setOutputCol(getFeaturesCol)
        .setInputCols(Array("treatment_residual"))
        .setOneHotEncodeCategoricals(oneHotEncodeCategoricals_finalModel)
        .setNumFeatures(numFeatures_finalModel)

      val featurized_finalModel = featurizer_finalModel.fit(convertedLabelDataset_finalModel)
      val processedData_finalModel = featurized_finalModel.transform(convertedLabelDataset_finalModel)
      processedData_finalModel.cache()
      val fitFinalModel = regressor_final.fit(processedData_finalModel)
      processedData_finalModel.unpersist()

      val pipeline_finalModel = new Pipeline().setStages(Array(featurized_finalModel, fitFinalModel)).fit(convertedLabelDataset_finalModel)

      val lrm = pipeline_finalModel.stages.last.asInstanceOf[GeneralizedLinearRegressionModel]

      // compute confidence intervals = slope +/- t * SE Coef
      val ci_lower = lrm.coefficients(0) - lrm.summary.tValues(0) * lrm.summary.coefficientStandardErrors(0)
      val ci_higher = lrm.coefficients(0) + lrm.summary.tValues(0) * lrm.summary.coefficientStandardErrors(0)

      new LinearDMLModel()
        .setATE(lrm.coefficients.asInstanceOf[DenseVector].values)
        .setCI(Array(ci_lower, ci_higher))
    })
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
    val treatmentSchema = StructType(schema.fields :+ StructField(SchemaConstants.TreatmentResidualColumn, DoubleType))
    StructType(treatmentSchema.fields :+ StructField(SchemaConstants.OutcomeResidualColumn, DoubleType))
  }

  def getFeaturizeParams(model: Estimator[_ <: Model[_]], getNumFeatures: Int): (Boolean, Int, Boolean) = {
    var oneHotEncodeCategoricals = true
    var modifyInputLayer = false
    // Create trainer based on the pipeline stage and set the parameters

    val numFeatures: Int = model match {
      case _: DecisionTreeClassifier | _: GBTClassifier | _: RandomForestClassifier | _: DecisionTreeRegressor | _: GBTRegressor | _: RandomForestRegressor =>
        oneHotEncodeCategoricals = false
        FeaturizeUtilities.NumFeaturesTreeOrNNBased
      case _: MultilayerPerceptronClassifier =>
        modifyInputLayer = true
        FeaturizeUtilities.NumFeaturesTreeOrNNBased
      case _ =>
        FeaturizeUtilities.NumFeaturesDefault
    }

    val featuresToHashTo = if (getNumFeatures != 0) {
      getNumFeatures
    } else {
      numFeatures
    }

    (oneHotEncodeCategoricals, featuresToHashTo, modifyInputLayer)
  }

  def getEstimatorWithLabelFeaturesConfigured(estimator: Estimator[_ <: Model[_]], labelCol: String, featuresCol: String): Estimator[_ <: Model[_]] = {
    estimator match {
      case predictor: Predictor[_, _, _] =>
        predictor
          .setLabelCol(labelCol)
          .setFeaturesCol(featuresCol).asInstanceOf[Estimator[_ <: Model[_]]]
      case default@defaultType if defaultType.isInstanceOf[Estimator[_ <: Model[_]]] =>
        // assume label col and features col already set
        default
      case _ => throw new Exception("Unsupported learner type " + estimator.getClass.toString)
    }

  }

  def convertRegressorLabel(dataset: Dataset[_],
                            labelColumn: String): DataFrame = {

    // TODO: Handle DateType, TimestampType and DecimalType for label
    // Convert the label column during train to the correct type and drop missings
    val df_cast = dataset.withColumn(labelColumn,
      col = dataset.schema(labelColumn).dataType match {
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


    // For illegal cast (e.g. an invalid string to double), Spark won't fail and just return null for them. (It's default behavior `spark.sql.ansi.enabled=false`)
    // So we do a simple count check to ensure casting has no failure
    if (dataset.count() != df_cast.count()) {
      throw new Exception("Invalid type: " +
        "Regressors are not able to train on a string label column when it has invalid value and not able to be converted to numeric: " + labelColumn
      )
    }

    df_cast
  }
}

/** Model produced by [[LinearDMLEstimator]]. */
class LinearDMLModel(val uid: String)
  extends AutoTrainedModel[LinearDMLModel] with Wrappable with BasicLogging {
  logClass()

  def this() = this(Identifiable.randomUID("LinearDMLModel"))

  val ate = new Param[Array[Double]](this, "cate", "average treatment effect")
  def getATE: Array[Double] = $(ate)
  def setATE(v:Array[Double] ): this.type = set(ate, v)

  var ci = new Param[Array[Double]](this, "ci", "treatment effect's confidence interval")
  def getCI: Array[Double] = $(ci)
  def setCI(v:Array[Double] ): this.type = set(ci, v)

  override def copy(extra: ParamMap): LinearDMLModel = defaultCopy(extra)

  //scalastyle:off
  override def transform(dataset: Dataset[_]): DataFrame = {
    logTransform[DataFrame]({
      throw new Exception("transform is invalid for LinearDMLEstimator.")
    })
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    throw new Exception("transform is invalid for LinearDMLEstimator.")
}

object LinearDMLModel extends ComplexParamsReadable[LinearDMLModel]
