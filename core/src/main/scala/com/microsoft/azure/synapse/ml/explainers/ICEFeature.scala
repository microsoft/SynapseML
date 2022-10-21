// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import spray.json.DefaultJsonProtocol._
import spray.json._

private[explainers] abstract class ICEFeature(name: String, outputColName: Option[String] = None) {
  def validate: Boolean
  private val defaultOutputColName = name + "_dependence"
  def getOutputColName: String = this.outputColName.getOrElse(defaultOutputColName)
  def getName: String = name
}

/**
  * Represents a single categorical feature to be explained by ICE explainer.
  * @param name The name of the categorical feature.
  * @param numTopValues The max number of top-occurring values to be included in the categorical feature.
  *                     Default: 100.
  * @param outputColName The name for output column with explanations for the feature.
  *                      Default: input name of the feature + _dependence.
  */
case class ICECategoricalFeature(name: String, numTopValues: Option[Int] = None,
                                 outputColName: Option[String] = None)
 extends ICEFeature(name, outputColName) {
  override def validate: Boolean = {
    numTopValues.forall(_ > 0)
  }

  def getNumTopValue: Int = {
    this.numTopValues.getOrElse(ICECategoricalFeature.DefaultNumTopValue)
  }
}

/**
  * Companion object to provide JSON serializer and deserializer for ICECategoricalFeature.
  */
object ICECategoricalFeature {
  val DefaultNumTopValue: Int = 100
  implicit val JsonFormat: JsonFormat[ICECategoricalFeature] = jsonFormat3(ICECategoricalFeature.apply)

  def fromMap(inputMap: java.util.HashMap[String, Any]): ICECategoricalFeature = {
    val name: String = inputMap.get("name").toString
    val numTopValues: Option[Int] = inputMap.get("numTopValues") match {
      case value: Integer => Some(Integer2int(value))
      case _ => None
    }

    val outputColName: Option[String] = inputMap.get("outputColName") match {
      case value: String => Some(value)
      case _ => None
    }

    ICECategoricalFeature(name, numTopValues, outputColName)
  }

  def fromMap(inputMap: Map[String, Any]): ICECategoricalFeature = {
    val name: String = inputMap.get("name").get.toString
    val numTopValues: Option[Int] = inputMap.get("numTopValues").asInstanceOf[Option[Int]]
    val outputColName: Option[String] = inputMap.get("outputColName").asInstanceOf[Option[String]]

    ICECategoricalFeature(name, numTopValues, outputColName)
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
case class ICENumericFeature(name: String, numSplits: Option[Int] = None,
                             rangeMin: Option[Double] = None, rangeMax: Option[Double] = None,
                             outputColName: Option[String] = None)
  extends ICEFeature(name, outputColName) {
  override def validate: Boolean = {
    // rangeMax and rangeMin may not be specified, but if specified: rangeMin <= rangeMax.
    numSplits.forall(_ > 0) && (rangeMax.isEmpty || rangeMin.isEmpty || rangeMin.get <= rangeMax.get)
  }

  def getNumSplits: Int = {
    this.numSplits.getOrElse(ICENumericFeature.DefaultNumSplits)
  }
}

/**
  * Companion object to provide JSON serializer and deserializer for ICENumericFeature.
  */
object ICENumericFeature {
  val DefaultNumSplits: Int = 10
  implicit val JsonFormat: JsonFormat[ICENumericFeature] = jsonFormat5(ICENumericFeature.apply)
  def fromMap(inputMap: java.util.HashMap[String, Any]): ICENumericFeature = {
    val name: String = inputMap.get("name").toString
    val numSplits: Option[Int] = inputMap.get("numSplits") match {
      case value: Integer => Some(Integer2int(value))
      case _ => None
    }

    val rangeMin: Option[Double] = inputMap.get("rangeMin") match {
      case value: java.lang.Double => Some(value.doubleValue())
      case _ => None
    }

    val rangeMax: Option[Double] = inputMap.get("rangeMax") match {
      case value: java.lang.Double => Some(value.doubleValue())
      case _ => None
    }

    val outputColName = inputMap.get("outputColName") match {
      case value: String => Some(value)
      case _ => None
    }

    ICENumericFeature(name, numSplits, rangeMin, rangeMax, outputColName)
  }

  def fromMap(inputMap: Map[String, Any]): ICENumericFeature = {
    val name: String = inputMap.get("name").get.toString
    val numSplits: Option[Int] = inputMap.get("numSplits").asInstanceOf[Option[Int]]
    val rangeMin: Option[Double] = inputMap.get("rangeMin").asInstanceOf[Option[Double]]
    val rangeMax: Option[Double] = inputMap.get("rangeMax").asInstanceOf[Option[Double]]
    val outputColName = inputMap.get("outputColName").asInstanceOf[Option[String]]

    ICENumericFeature(name, numSplits, rangeMin, rangeMax, outputColName)
  }
}
