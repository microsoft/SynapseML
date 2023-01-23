// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import org.apache.spark.ml.param._

/** Helper class for converting individual typed parameters to a single long string for passing to native libraries.
  *
  * Example:
  * new ParamsStringBuilder(prefix = "--", delimiter = "=")
  *   .append("--first_param=a")
  *   .appendParamValueIfNotThere("first_param", Option("a2"))
  *   .appendParamValueIfNotThere("second_param", Option("b"))
  *   .appendParamValueIfNotThere("third_param", None)
  *   .result
  *
  * result == "--first_param=a --second_param=b"
  *
  * This utility mimics a traditional StringBuilder, where you append parts and ask for the final result() at the end.
  * Each parameter appended is tracked and can be compared against the current string, to both avoid duplicates
  * and provide an override mechanism.  The first param added is considered the primary one to not be replaced.
  *
  * Use 'append' to add an unchecked string directly to end of current string.
  *
  * Use ParamsSet to create encapsulated subsets of params that can be incorporated into the parent ParamsStringBuilder.
  *
  * There is also integration with the SparkML Params system. Construct with a parent Params object and use the methods
  * with Param arguments.
  *
  * @param parent Optional parent Params instance to validate each Param against.
  * @param prefix Optional prefix to put before parameter names (e.g. "--").  Defaults to none.
  * @param delimiter Optional delimiter to put between names and values (e.g. "="). Defaults to "=".
  */
class ParamsStringBuilder(parent: Option[Params], prefix: String, delimiter: String) extends Serializable {

  // A StringBuilder is a relatively inefficient way to implement this (e.g. HashTable would be better),
  // but it is simple to interpret/maintain and not on a critical perf path.
  private val sb: StringBuilder = new StringBuilder()

  def this(prefix: String = "", delimiter: String = "=") = {
    this(None, prefix, delimiter)
  }

  def this(parent: Params, prefix: String, delimiter: String) = {
    this(Option(parent), prefix, delimiter)
  }

  /** Add a parameter name-value pair to the end of the current string.
    * @param optionShort Short name of the parameter (only used to check against existing params).
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    * @param param The Param object with the value.  Note that if this is not set, nothing will be appended to string.
    */
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
        case _ => append(s"$prefix$optionLong$delimiter${getParamValue(param).get}")
      }
    }
    this
  }

  /** Add a parameter name-value pair for each array element to the end of the current string (e.g. "-q aa -q bb").
    * @param optionShort Short name of the parameter (only used to check against existing params).
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    * @param param The Param object with the value.  Note that if this is not set, nothing will be appended to string.
    */
  def appendRepeatableParamIfNotThere[T](optionShort: String,
                                          optionLong: String,
                                          param: Param[T]): ParamsStringBuilder = {
    if (getParamValue(param).isDefined)
    {
      param match {
        case _: IntArrayParam =>
          appendRepeatableParamIfNotThere(optionShort, optionLong, getParamValue(param).get.asInstanceOf[Array[Int]])
        case _: DoubleArrayParam =>
          appendRepeatableParamIfNotThere(optionShort, optionLong, getParamValue(param).get.asInstanceOf[Array[Double]])
        case _: StringArrayParam =>
          appendRepeatableParamIfNotThere(optionShort, optionLong, getParamValue(param).get.asInstanceOf[Array[String]])
        case _ => throw new IllegalArgumentException("Repeatable param must be an array")
      }
    }
    this
  }

  /** Add a parameter name-value pair to the end of the current string.
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    * @param param The Option object with the value.  Note that if this is None, nothing will be appended to string.
    */
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

  /** Add a parameter name to the end of the current string. (i.e. a param that does not have a value)
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    */
  def appendParamFlagIfNotThere(name: String): ParamsStringBuilder = {
    if (s"$prefix$name".r.findAllIn(sb.result).isEmpty) {
      append(s"$prefix$name")
    }
    this
  }

  /** Add a parameter name-list pair to the end of the current string. Values will be comma-delimited.
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    * @param values The Array of values.  Note that if the array is empty, nothing will be appended to string.
    */
  def appendParamListIfNotThere[T](name: String, values: Array[T]): ParamsStringBuilder = {
    if (!values.isEmpty && s"$prefix$name".r.findAllIn(sb.result).isEmpty) {
      appendParamList(name, values)
    }
    this
  }

  /** Add a parameter that can be repeated, once for each element in the array (e.g. "-q aa -q bb")
    * @param optionLong Long name of the parameter.  Will be used if it is not already set.
    * @param values The Array of values.  Note that if the array is empty, nothing will be appended to string.
    */
  def appendRepeatableParamIfNotThere[T](optionShort: String,
                                         optionLong: String,
                                         values: Array[T]): ParamsStringBuilder = {
    for (str <- values) {
      val shortPair = s"$prefix$optionShort$delimiter$str"
      val longPair = s"$prefix$optionLong$delimiter$str"
      if (shortPair.r.findAllIn(sb.result).isEmpty && longPair.r.findAllIn(sb.result).isEmpty) append(longPair)
    }
    this
  }

  def appendParamList[T](name: String, values: Array[T]): ParamsStringBuilder = {
    append(s"$prefix$name$delimiter${values.mkString(",")}")
  }

  /** Add a parameter group to the end of the current string.
    * @param paramGroup The ParamGroup to add.
    */
  def appendParamGroup(paramGroup: ParamGroup): ParamsStringBuilder = {
    paramGroup.appendParams(this)
  }

  /** Add a parameter group to the end of the current string conditionally.
    * @param paramGroup The ParamGroup to add.
    * @param condition Whether to add the group or not.
    */
  def appendParamGroup(paramGroup: ParamGroup, condition: Boolean): ParamsStringBuilder = {
    if (condition) paramGroup.appendParams(this) else this
  }

  /** Direct append a string with no checking of existing params.
    * @param str The string to add.
    */
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
  *  into a parent ParamsStringBuilder with appendParamsSet,  Useful for encapsulating Params into
  *  smaller subsets.
  */
trait ParamGroup extends Serializable {
  override def toString: String = {
    new ParamsStringBuilder().appendParamGroup(this).result
  }

  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder
}
