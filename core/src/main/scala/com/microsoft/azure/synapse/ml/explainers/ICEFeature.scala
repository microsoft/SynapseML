package com.microsoft.azure.synapse.ml.explainers

import spray.json._
import DefaultJsonProtocol._

private[explainers] abstract class ICEFeature(val name: String) {
  def validate: Boolean
}

case class ICECategoricalFeature(override val name: String, numTopValues: Option[Int] = None)
  extends ICEFeature(name) {
  override def validate: Boolean = {
    numTopValues.forall(_ > 0)
  }

  private val defaultNumTopValue = 100
  def getNumTopValue: Int = {
    this.numTopValues.getOrElse(defaultNumTopValue)
  }
}

object ICECategoricalFeature {
  implicit val JsonFormat: JsonFormat[ICECategoricalFeature] = jsonFormat2(ICECategoricalFeature.apply)
}

case class ICENumericFeature(override val name: String, numSplits: Option[Int] = None,
                             rangeMin: Option[Double] = None, rangeMax: Option[Double] = None)
  extends ICEFeature(name) {
  override def validate: Boolean = {
    numSplits.forall(_ > 0) && (rangeMax.isEmpty || rangeMin.isEmpty || rangeMin.get <= rangeMax.get)
  }

  private val defaultNumSplits = 10
  def getNumSplits: Int = {
    this.numSplits.getOrElse(defaultNumSplits)
  }
}

object ICENumericFeature {
  implicit val JsonFormat: JsonFormat[ICENumericFeature] = jsonFormat4(ICENumericFeature.apply)
}