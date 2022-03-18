// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import org.apache.spark.ml.param._

import java.security.InvalidParameterException

/** Helper class converting individual typed parameters to a single long string for passing to native libraries.
  *
  * Each parameter appended is tracked and can be compared against the current string, to both avoid duplicates
  * and provide an override mechanism.  The first param added is considered the primary one to not be replaced.
  *
  * Use ParamsSet to create encapsulated subsets of params that can be incorporated into the parent ParamsStringBuilder.
  *
  * @param parent Optional parent Params instance to validate each Param against.
  * @param prefix Optional prefix to put before parameter names (e.g. "--").  Defaults to none.
  * @param delimiter Optional delimiter to put between names and values (e.g. "="). Defaults to "=".
  */
class ParamsStringBuilder(parent: Option[Params], prefix: String, delimiter: String) {

  private val sb: StringBuilder = new StringBuilder()

  def this(prefix: String = "", delimiter: String = "=") = {
    this(None, prefix, delimiter)
  }

  def this(parent: Params, prefix: String, delimiter: String) = {
    this(Option(parent), prefix, delimiter)
  }

  def appendParamValueIfNotThere[T](optionShort: String, optionLong: String, param: Param[T]): ParamsStringBuilder = {
    if (getParamValue(param).isDefined &&
      // boost allows " " or "=" as separators
      s"-$optionShort[ =]".r.findAllIn(sb.result).isEmpty &&
      s"$prefix$optionLong[ =]".r.findAllIn(sb.result).isEmpty)
    {
      param match {
        case _: IntArrayParam =>
          appendParamListIfNotThere(optionLong, getParamValue(param).get.asInstanceOf[Array[Int]])
        case _: DoubleArrayParam =>
          appendParamListIfNotThere(optionLong, getParamValue(param).get.asInstanceOf[Array[Double]])
        case _: StringArrayParam =>
          appendParamListIfNotThere(optionLong, getParamValue(param).get.asInstanceOf[Array[String]])
          //for (q <- getParamValue(param).get)  TODO this is the old code for Array[String], but seems wrong
          //  append(s"$prefix$optionLong$delimiter$q")
        case _ => append(s"$prefix$optionLong$delimiter${getParamValue(param).get}")
      }
    }
    this
  }

  def appendParamValueIfNotThere[T](optionLong: String, param: Option[T]): ParamsStringBuilder = {
    if (param.isDefined &&
      // boost allow " " or "="
      s"$prefix$optionLong[ =]".r.findAllIn(sb.result).isEmpty)
    {
      append(s"$prefix$optionLong$delimiter${param.get}")
    }
    sb.to
    this
  }

  def appendParamFlagIfNotThere(name: String): ParamsStringBuilder = {
    if (s"$prefix$name".r.findAllIn(sb.result).isEmpty) {
      append(s"$prefix$name")
    }
    this
  }

  def appendParamListIfNotThere[T](name: String, values: Array[T]): ParamsStringBuilder = {
    if (!values.isEmpty && s"$prefix$name".r.findAllIn(sb.result).isEmpty) {
      appendParamList(name, values)
    }
    this
  }

  def appendParamList[T](name: String, values: Array[T]): ParamsStringBuilder = {
    append(s"$prefix$name$delimiter${values.mkString(",")}")
  }

  def appendParamGroup(paramSet: ParamGroup): ParamsStringBuilder = {
    paramSet.appendParams(this)
  }

  def appendParamGroup(paramSet: ParamGroup, condition: Boolean): ParamsStringBuilder = {
    if (condition) paramSet.appendParams(this) else this
  }

  def append(str: String): ParamsStringBuilder =
  {
    if (!str.isEmpty) {
      if (!sb.isEmpty) sb.append(" ")
      sb.append(str)
    }
    this
  }

  def result(): String =
  {
    sb.result
  }

  private def getParent(): Params = {
    if (parent.isEmpty)
    {
      throw new IllegalArgumentException("ParamsStringBuilder requires a parent for this operation")
    }
    parent.get
  }

  private def getParamValue[T](param: Param[T]): Option[T] = {
    getParent.get(param)
  }
}

/** Derive from this to create an encapsulated subset of params that can be integrated
  *  into a parent ParamsStringBuilder with appendParamsSet
  */
trait ParamGroup extends Serializable {
  override def toString: String = {
    new ParamsStringBuilder().appendParamGroup(this).result
  }

  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder
}




