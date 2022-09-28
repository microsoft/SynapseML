package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.contracts.HasWeightCol
import org.apache.spark.ml.classification.{LogisticRegression, ProbabilisticClassifier}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{EstimatorParam, Param, Params}
import org.apache.spark.ml.regression.{RandomForestRegressor, Regressor}


trait HasTreatmentCol extends Params {
  val treatment = new Param[String](this, "treatment", "treatment column")
  def getTreatmentCol: String = $(treatment)
  def setTreatmentCol(value: String): this.type = set(treatment, value)
}

trait HasOutcomeCol extends Params {
  val outcome: Param[String] = new Param[String](this, "outcome", "outcome column")
  def getOutcomeCol: String = $(outcome)
  def setOutcomeCol(value: String): this.type = set(outcome, value)
}

trait LinearDMLParams extends Params with HasTreatmentCol with HasOutcomeCol with HasWeightCol {
  val discrete_treatment: Param[Boolean] = new Param[Boolean](this, name="discrete_treatment", doc="discrete or continuous treatment")
  def getDiscreteTreatment: Boolean = $(discrete_treatment)
  def setDiscreteTreatment(value: Boolean): this.type = set(discrete_treatment, value)

  val treatmentModel = new EstimatorParam(this, "treatmentModel", "treatment model to run")
  def getTreatmentModel: Estimator[_ <: Model[_]] = $(treatmentModel)

  /** The currently supported classifiers are:
   * Logistic Regression Classifier
   * Decision Tree Classifier
   * Random Forest Classifier
   * Gradient Boosted Trees Classifier
   * Naive Bayes Classifier
   * Multilayer Perceptron Classifier
   * In addition to any generic learner that inherits from Predictor.
   */
  def setTreatmentModel(value: Estimator[_ <: Model[_]]) : this.type = {
    val isSupportedModel = value match {
      case regressor: Regressor[_,_,_] =>   true // for continuous treatment
      case classifier: ProbabilisticClassifier[_, _, _] => true
      case _ => false
    }
    if (!isSupportedModel)  {
      throw new Exception("LinearDML only support regressor and ProbabilisticClassifier as treatment model")
    }
    set(treatmentModel, value)
  }

  val outcomeModel = new EstimatorParam(this, "outcomeModel", "outcome model to run")
  def getOutcomeModel: Estimator[_ <: Model[_]] = $(outcomeModel)
  def setOutcomeModel(value: Estimator[_ <: Model[_]] ) : this.type = {
    val isSupportedModel = value match {
      case regressor: Regressor[_,_,_] =>   true // for continuous treatment
      case _ => false
    }
    if (!isSupportedModel)  {
      throw new Exception("LinearDML only support regressor as outcome model")
    }
    set(outcomeModel, value)
  }

  setDefault(
    discrete_treatment -> false,
    treatmentModel -> new LogisticRegression(),
    outcomeModel -> new RandomForestRegressor()
  )
}
