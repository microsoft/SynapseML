// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import spray.json._

private[explainers] abstract class ICEFeature(val name: String, outputColName: Option[String] = None) {
  def validate: Boolean
  private val defaultOutputColName = name + "_dependence"
  def getOutputColName: String = this.outputColName.getOrElse(defaultOutputColName)
}

/**
  * Represents a single categorical feature to be explained by ICE explainer.
  * @param name The name of the categorical feature.
  * @param numTopValues The max number of top-occurring values to be included in the categorical feature.
  *                     Default: 100.
  * @param outputColName The name for output column with explanations for the feature.
  *                      Default: input name of the feature + _dependence.
  */
case class ICECategoricalFeature(override val name: String, numTopValues: Option[Int] = None,
                                 outputColName: Option[String] = None)
 extends ICEFeature(name, outputColName) {
  override def validate: Boolean = {
    numTopValues.forall(_ > 0)
  }

  private val defaultNumTopValue = 100
  def getNumTopValue: Int = {
    this.numTopValues.getOrElse(defaultNumTopValue)
  }
}

/**
  * Companion object to provide JSON serializer and deserializer for ICECategoricalFeature.
  */
object ICECategoricalFeature {
  implicit val JsonFormat: JsonFormat[ICECategoricalFeature] = new JsonFormat[ICECategoricalFeature] {
    override def read(json: JsValue): ICECategoricalFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }
      val numTopValues = fields.get("numTopValues") match {
        case Some(JsNumber(value)) => Some(value.toInt)
        case _ => None
      }
      val outputColName = fields.get("outputColName") match {
        case Some(JsString(value)) => Some(value)
        case _ => None
      }
      ICECategoricalFeature(name, numTopValues, outputColName)

    }
    override def write(obj: ICECategoricalFeature): JsValue = {
      val map = Map("name" -> JsString(obj.name))++
        obj.numTopValues.map("numTopValues" -> JsNumber(_))++
        obj.outputColName.map("outputColName" -> JsString(_))
      JsObject(map)
    }
  }
}

/**
  * Represents a single numeric feature to be explained by ICE explainer.
  * @param name The name of the numeric feature.
  * @param numSplits The number of splits for the value range for the numeric feature.
  *                  Default: 10.0
  * @param rangeMin Specifies the min value of the range for the numeric feature. If not specified,
  *                 it will be computed from the background dataset.
  * @param rangeMax Specifies the max value of the range for the numeric feature. If not specified,
  *                 it will be computed from the background dataset.
  * @param outputColName The name for output column with explanations for the feature.
  *                      Default: input name of the feature + "_dependence"
  */
case class ICENumericFeature(override val name: String, numSplits: Option[Int] = None,
                             rangeMin: Option[Double] = None, rangeMax: Option[Double] = None,
                             outputColName: Option[String] = None)
  extends ICEFeature(name, outputColName) {
  override def validate: Boolean = {
    // rangeMax and rangeMin may not be specified, but if specified: rangeMin <= rangeMax.
    numSplits.forall(_ > 0) && (rangeMax.isEmpty || rangeMin.isEmpty || rangeMin.get <= rangeMax.get)
  }

  private val defaultNumSplits = 10
  def getNumSplits: Int = {
    this.numSplits.getOrElse(defaultNumSplits)
  }
}

/**
  * Companion object to provide JSON serializer and deserializer for ICENumericFeature.
  */
object ICENumericFeature {
  implicit val JsonFormat: JsonFormat[ICENumericFeature] = new JsonFormat[ICENumericFeature] {
    override def read(json: JsValue): ICENumericFeature = {
      val fields = json.asJsObject.fields
      val name = fields("name") match {
        case JsString(value) => value
        case _ => throw new Exception("The name field must be a JsString.")
      }

      val numSplits = fields.get("numSplits") match {
        case Some(JsNumber(value)) => Some(value.toInt)
        case _ => None
      }

      val rangeMin = fields.get("rangeMin").map {
        case JsNumber(value) => value.toDouble
      }

      val rangeMax = fields.get("rangeMax").map {
        case JsNumber(value) => value.toDouble
      }

      val outputColName = fields.get("outputColName") match {
        case Some(JsString(value)) => Some(value)
        case _ => None
      }

      ICENumericFeature(name, numSplits, rangeMin, rangeMax, outputColName)

    }

    override def write(obj: ICENumericFeature): JsValue = {
      val map = Map("name" -> JsString(obj.name))++
        obj.numSplits.map("numSplits" -> JsNumber(_))++
        obj.rangeMin.map("rangeMin" -> JsNumber(_))++
        obj.rangeMax.map("rangeMax" -> JsNumber(_))++
        obj.outputColName.map("outputColName" -> JsString(_))
      JsObject(map)
    }
  }
}