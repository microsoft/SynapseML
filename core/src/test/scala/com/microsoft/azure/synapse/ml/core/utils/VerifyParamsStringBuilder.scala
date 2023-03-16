// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param._

class VerifyParamsStringBuilder extends TestBase {
  // Test LightGBM style arguments, e.g. "objective=classifier"
  test("Verify ParamsStringBuilder handles basic string parameters") {
    val psb = new ParamsStringBuilder(prefix = "", delimiter = "=")
      .append("pass_through_param=custom")
      .append("pass_through_param_with_space custom2")
      .append("pass_through_list=1,2,3")
      .append("pass_through_flag")
      .append("-short_param=ok")
      .append("-short_param_with_space ok2")
      .append("repeated_param=p1")
      .append("rp=p2")
      .appendParamFlagIfNotThere("some_param_flag")
      .appendParamFlagIfNotThere("pass_through_flag") // should not add
      .appendParamListIfNotThere("some_param_list", Array[Int](3, 4, 5))
      .appendParamListIfNotThere("some_empty_list", new Array(0)) // should not add
      .appendParamListIfNotThere("pass_through_list", Array[Int](3, 4, 5)) // should not add
      .appendParamValueIfNotThere("null_param", None) // should not add
      .appendParamValueIfNotThere("some_param", Option(10))
      .appendParamValueIfNotThere("pass_through_param", Option("bad_val")) // should not add
      .appendRepeatableParamIfNotThere("rp",
        "repeated_param",
        Array("p1", "p2", "p3")) // should not add p1 or p2

    val expectedStr: String = "pass_through_param=custom" +
      " pass_through_param_with_space custom2" +
      " pass_through_list=1,2,3" +
      " pass_through_flag" +
      " -short_param=ok" +
      " -short_param_with_space ok2" +
      " repeated_param=p1" +
      " rp=p2" +
      " some_param_flag" +
      " some_param_list=3,4,5" +
      " some_param=10" +
      " repeated_param=p3"

    assert(psb.result == expectedStr)
  }

  // Test Vowpal Wabbit style arguments, e.g. "--hash_seed 4 -l .1 --holdout_off"
  test("Verify ParamsStringBuilder handles different prefix and delimiter") {
    val psb = new ParamsStringBuilder(prefix = "--", delimiter = " ")
      .append("--pass_through_param custom")
      .append("--pass_through_list 1,2,3")
      .append("--pass_through_flag")
      .append("-short_param ok")
      .appendParamFlagIfNotThere("some_param_flag")
      .appendParamFlagIfNotThere("pass_through_flag") // should not add
      .appendParamListIfNotThere("some_param_list", Array[Int](3, 4, 5))
      .appendParamListIfNotThere("some_empty_list", new Array(0)) // should not add
      .appendParamListIfNotThere("pass_through_list", Array[Int](3, 4, 5)) // should not add
      .appendParamValueIfNotThere("null_param", None) // should not add
      .appendParamValueIfNotThere("some_param", Option(10))
      .appendParamValueIfNotThere("pass_through_param", Option("bad_val")) // should not add

    val expectedStr: String = "--pass_through_param custom" +
      " --pass_through_list 1,2,3" +
      " --pass_through_flag" +
      " -short_param ok" +
      " --some_param_flag" +
      " --some_param_list 3,4,5" +
      " --some_param 10"

    assert(psb.result == expectedStr)
  }

  test("Verify ParamsStringBuilder handles sparkml Params") {
    val testParamsContainer = new TestParams()

    // Populate container with parameters
    testParamsContainer.setString("some_string")
    testParamsContainer.setInt(3)
    testParamsContainer.setFloat(3.0f)
    testParamsContainer.setDouble(3.0)
    testParamsContainer.setStringArray(Array("one", "two", "three"))
    testParamsContainer.setIntArray(Array(1,2,3))
    testParamsContainer.setDoubleArray(Array(1.0, 2.0, 3.0))

    val psb = new ParamsStringBuilder(testParamsContainer, prefix = "--", delimiter = " ")
      .appendParamValueIfNotThere("string_value", "string_value", testParamsContainer.testString)
      .appendParamValueIfNotThere("int_value", "int_value", testParamsContainer.testInt)
      .appendParamValueIfNotThere("float_value", "float_value", testParamsContainer.testFloat)
      .appendParamValueIfNotThere("double_value", "double_value", testParamsContainer.testDouble)
      .appendParamValueIfNotThere("string_array_value", "string_array_value", testParamsContainer.testStringArray)
      .appendParamValueIfNotThere("int_array_value", "int_array_value", testParamsContainer.testIntArray)
      .appendParamValueIfNotThere("double_array_value", "double_array_value", testParamsContainer.testDoubleArray)
      .appendRepeatableParamIfNotThere("r", "repeated", testParamsContainer.testStringArray)

    val expectedStr: String = "--string_value some_string" +
      " --int_value 3" +
      " --float_value 3.0" +
      " --double_value 3.0" +
      " --string_array_value one,two,three" +
      " --int_array_value 1,2,3" +
      " --double_array_value 1.0,2.0,3.0" +
      " --repeated one" +
      " --repeated two" +
      " --repeated three"

    assert(psb.result == expectedStr)
  }

  test("Verify ParamsStringBuilder ignores sparkml Params unset parameters") {
    val testParamsContainer = new TestParams()

    val psb = new ParamsStringBuilder(testParamsContainer, prefix = "--", delimiter = " ")
      .appendParamValueIfNotThere("string_value", "string_value", testParamsContainer.testString)
      .appendParamValueIfNotThere("int_value", "int_value", testParamsContainer.testInt)
      .appendParamValueIfNotThere("float_value", "float_value", testParamsContainer.testFloat)
      .appendParamValueIfNotThere("double_value", "double_value", testParamsContainer.testDouble)
      .appendParamValueIfNotThere("string_array_value", "string_array_value", testParamsContainer.testStringArray)
      .appendParamValueIfNotThere("int_array_value", "int_array_value", testParamsContainer.testIntArray)
      .appendParamValueIfNotThere("double_array_value", "double_array_value", testParamsContainer.testDoubleArray)
      .appendRepeatableParamIfNotThere("r", "repeated", testParamsContainer.testDoubleArray)

    val expectedStr: String = "" // Since no parameters were set, they should not have been appended to string

    assert(psb.result == expectedStr)
  }

  test("Verify ParamsStringBuilder throws when sparkml gets invalid Param") {
    val testParamsContainer = new TestParams()

    val unknownParam = new Param[String]("some parent", "unknown_param", "doc_string")
    assertThrows[IllegalArgumentException] {
      val psb = new ParamsStringBuilder(testParamsContainer, prefix = "--", delimiter = " ")
        .appendParamValueIfNotThere("string_value", "string_value", unknownParam)
    }

    assertThrows[IllegalArgumentException] {
      val psb = new ParamsStringBuilder(prefix = "--", delimiter = " ")
        .appendParamValueIfNotThere("string_value", "string_value", testParamsContainer.testString)
    }

    // Can only pass Array types to appendRepeatableParamIfNotThere
    testParamsContainer.setString("val")
    assertThrows[IllegalArgumentException] {
      val psb = new ParamsStringBuilder(prefix = "--", delimiter = " ")
        .appendRepeatableParamIfNotThere("s", "string_value", testParamsContainer.testString)
    }
  }

  test("Verify ParamsStringBuilder handles sparkml Params custom overrrides") {
    val testParamsContainer = new TestParams()

    // Populate container with parameters that will be overridden
    testParamsContainer.setString("some_string")
    testParamsContainer.setInt(3)
    testParamsContainer.setFloat(3.0f)
    testParamsContainer.setDouble(3.0)
    testParamsContainer.setStringArray(Array("one", "two", "three"))
    testParamsContainer.setIntArray(Array(1,2,3))
    testParamsContainer.setDoubleArray(Array(1.0, 2.0, 3.0))

    val psb = new ParamsStringBuilder(testParamsContainer, prefix = "--", delimiter = " ")
      .append("--string_value some_override")
      .append("--int_value 99")
      .append("--float_value 99.0")
      .append("--double_value 99.0")
      .append("--string_array_value three,four,five")
      .append("--int_array_value 97,98,99")
      .append("--double_array_value 97.0,98.0,99.0")
      .appendParamValueIfNotThere("string_value", "string_value", testParamsContainer.testString)
      .appendParamValueIfNotThere("int_value", "int_value", testParamsContainer.testInt)
      .appendParamValueIfNotThere("float_value", "float_value", testParamsContainer.testFloat)
      .appendParamValueIfNotThere("double_value", "double_value", testParamsContainer.testDouble)
      .appendParamValueIfNotThere("int_array_value", "int_array_value", testParamsContainer.testIntArray)
      .appendParamValueIfNotThere("double_array_value", "double_array_value", testParamsContainer.testDoubleArray)
      .appendParamValueIfNotThere("string_array_value", "string_array_value", testParamsContainer.testStringArray)

    // We expect the set Params to be ignored since an override exists, so we should only see the original overrides
    val expectedStr: String = "--string_value some_override" +
      " --int_value 99" +
      " --float_value 99.0" +
      " --double_value 99.0" +
      " --string_array_value three,four,five" +
      " --int_array_value 97,98,99" +
      " --double_array_value 97.0,98.0,99.0"

    assert(psb.result == expectedStr)
  }

  test("Verify ParamsStringBuilder handles ParamGroup") {
    val testParamGroup = new TestParamGroup(1, None)

    val psb = new ParamsStringBuilder(prefix = "", delimiter = "=")
      .appendParamGroup(testParamGroup)

    // We expect the optional Int to be ignored since it wasn't set
    val expectedStr: String = "some_int=1"

    assert(psb.result == expectedStr)
  }
}

/** Test class for testing sparkml Param handling
  * */
class TestParams extends Params {
  override def copy(extra: ParamMap): Params = this // not needed
  override val uid: String = "some id"

  val testString = new Param[String](this, "testString", "Test String param")
  def setString(value: String): this.type = set(testString, value)

  val testInt = new IntParam(this, "testInt", "Test Int param")
  def setInt(value: Int): this.type = set(testInt, value)

  val testFloat = new FloatParam(this, "testFloat", "Test Float param")
  def setFloat(value: Float): this.type = set(testFloat, value)

  val testDouble = new DoubleParam(this, "testDouble", "Test Double param")
  def setDouble(value: Double): this.type = set(testDouble, value)

  val testStringArray = new StringArrayParam(this, "testStringArray", "Test StringArray param")
  def setStringArray(value: Array[String]): this.type = set(testStringArray, value)

  val testIntArray = new IntArrayParam(this, "testIntArray", "Test IntArray param")
  def setIntArray(value: Array[Int]): this.type = set(testIntArray, value)

  val testDoubleArray = new DoubleArrayParam(this, "testDoubleArray", "Test DoubleArray param")
  def setDoubleArray(value: Array[Double]): this.type = set(testDoubleArray, value)
}

/** Test class for testing ParamGroup handling
  * */
case class TestParamGroup (someInt: Int,
                           someOptionInt: Option[Int]) extends ParamGroup {
  def appendParams(sb: ParamsStringBuilder): ParamsStringBuilder = {
    sb.appendParamValueIfNotThere("some_int", Option(someInt))
      .appendParamValueIfNotThere("some_option_int", someOptionInt)
  }
}
