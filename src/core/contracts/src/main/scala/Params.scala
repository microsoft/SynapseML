// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import scala.collection.mutable.Map
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}

trait MMLParams extends Wrappable with DefaultParamsWritable

trait Wrappable extends Params {

  // Use this function when instantiating sparkML Identifiable for your
  // own use - it allows us to locate the origin of any stacks
  def chainedUid(origin: String): String = Identifiable.randomUID(this.uid)

  private var orderCounter = 0
  // TODO: Support non-string "enums"?
  val paramDomains = Map[String, Seq[String]]()

  def BooleanParam(i: Identifiable, name: String, description: String): BooleanParam =
    BooleanParam(i, name, description, false)

  def BooleanParam(i: Identifiable, name: String, description: String,
                   default: Boolean): BooleanParam = {
    val baseParam = new BooleanParam(i, name, description)
    MMLParam(baseParam, Some(default), None)
    baseParam
  }

  def IntParam(i: Identifiable, name: String, description: String): IntParam = {
    val baseParam = new IntParam(i, name, description)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def IntParam(i: Identifiable, name: String, description: String,
               default: Int): IntParam = {
    val baseParam = new IntParam(i, name, description)
    MMLParam(baseParam, Some(default), None)
    baseParam
  }

  def IntParam(i: Identifiable, name: String, description: String, validation: Int => Boolean): IntParam = {
    val baseParam = new IntParam(i, name, description, validation)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def LongParam(i: Identifiable, name: String, description: String): LongParam = {
    val baseParam = new LongParam(i, name, description)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def LongParam(i: Identifiable, name: String, description: String,
                default: Long): LongParam = {
    val baseParam = new LongParam(i, name, description)
    MMLParam(baseParam, Some(default), None)
    baseParam
  }

  def DoubleParam(i: Identifiable, name: String, description: String): DoubleParam = {
    val baseParam = new DoubleParam(i, name, description)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def DoubleParam(i: Identifiable, name: String, description: String,
                  default: Double): DoubleParam = {
    val baseParam = new DoubleParam(i, name, description)
    MMLParam(baseParam, Some(default), None)
    baseParam
  }

  def StringParam(i: Identifiable, name: String, description: String): Param[String] = {
    val baseParam = new Param[String](i, name, description)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def StringParam(i: Identifiable, name: String, description: String, validation: String => Boolean): Param[String] = {
    val baseParam = new Param[String](i, name, description, validation)
    MMLParam(baseParam, None, None)
    baseParam
  }

  def StringParam(i: Identifiable, name: String, description: String,
                  default: String): Param[String] = {
    val baseParam = new Param[String](i, name, description)
    MMLParam(baseParam, Some(default), None)
    baseParam
  }

  def StringParam(i: Identifiable, name: String, description: String,
                  default: String, domain: Seq[String]): Param[String] = {
    val baseParam = new Param[String](i, name, description)
    MMLParam(baseParam, Some(default), Some(domain))
    baseParam
  }

  private def MMLParam[T](param: Param[T],
                          default: Option[T], domain: Option[Seq[String]]): Unit = {
    if (default.isDefined) setDefault(param, default.get)
    if (domain.isDefined) paramDomains.put(param.name, domain.get)
    orderCounter += 1
  }

}

trait HasInputCol extends Wrappable {
  val inputCol = StringParam(this, "inputCol", "The name of the input column")
  def setInputCol(value: String): this.type = set(inputCol, value)
  def getInputCol: String = $(inputCol)
}

trait HasOutputCol extends Wrappable {
  val outputCol = StringParam(this, "outputCol", "The name of the output column")
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def getOutputCol: String = $(outputCol)
}

trait HasLabelCol extends Wrappable {
  val labelCol = StringParam(this, "labelCol", "The name of the label column")
  def setLabelCol(value: String): this.type = set(labelCol, value)
  def getLabelCol: String = $(labelCol)
}

trait HasFeaturesCol extends Wrappable {
  val featuresCol = StringParam(this, "featuresCol", "The name of the features column")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
  def getFeaturesCol: String = $(featuresCol)
}
