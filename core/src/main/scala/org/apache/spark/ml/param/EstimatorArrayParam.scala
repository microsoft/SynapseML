// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.ml.spark.core.serialize.ComplexParam
import org.apache.spark.ml.Estimator

import scala.collection.JavaConverters._

/** Param for Array of Models. */
class EstimatorArrayParam(parent: Params, name: String, doc: String, isValid: Array[Estimator[_]] => Boolean)
  extends ComplexParam[Array[Estimator[_]]](parent, name, doc, isValid)
    with WrappableParam[Array[Estimator[_]]] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  def w(value: java.util.List[Estimator[_]]): ParamPair[Array[Estimator[_]]] = w(value.asScala.toArray)

  override def dotnetValue(v: Array[Estimator[_]]): String = s"""${name}Param"""

  override def dotnetParamInfo: String = "EstimatorArray"

}

class EstimatorArray {

  private var estimators: Array[Estimator[_]] = Array()

  def addEstimator(value: Estimator[_]): Unit =
    this.estimators = this.estimators :+ value

  def getEstimators: Array[Estimator[_]] = this.estimators

}
