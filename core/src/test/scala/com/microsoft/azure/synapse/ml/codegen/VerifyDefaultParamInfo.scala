// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable

class VerifyDefaultParamInfo extends TestBase {

  private object TestParams extends Params {
    override val uid: String = Identifiable.randomUID("TestParams") // scalastyle:ignore field.name
    override def copy(extra: ParamMap): Params = this
  }

  test("getGeneralParamInfo returns BooleanInfo for BooleanParam") {
    val p = new BooleanParam(TestParams, "b", "desc")
    assert(DefaultParamInfo.getGeneralParamInfo(p) === DefaultParamInfo.BooleanInfo)
  }

  test("getGeneralParamInfo returns IntInfo for IntParam") {
    val p = new IntParam(TestParams, "i", "desc")
    assert(DefaultParamInfo.getGeneralParamInfo(p) === DefaultParamInfo.IntInfo)
  }

  test("getGeneralParamInfo returns DoubleInfo for DoubleParam") {
    val p = new DoubleParam(TestParams, "d", "desc")
    assert(DefaultParamInfo.getGeneralParamInfo(p) === DefaultParamInfo.DoubleInfo)
  }

  test("getGeneralParamInfo returns StringArrayInfo for StringArrayParam") {
    val p = new StringArrayParam(TestParams, "sa", "desc")
    assert(DefaultParamInfo.getGeneralParamInfo(p) === DefaultParamInfo.StringArrayInfo)
  }

  test("getGeneralParamInfo returns UnknownInfo for unrecognized param") {
    val p = new Param[Any](TestParams, "unknown", "desc")
    assert(DefaultParamInfo.getGeneralParamInfo(p) === DefaultParamInfo.UnknownInfo)
  }

  test("ParamInfo instances have correct pyType values") {
    assert(DefaultParamInfo.BooleanInfo.pyType === "bool")
    assert(DefaultParamInfo.IntInfo.pyType === "int")
    assert(DefaultParamInfo.DoubleInfo.pyType === "float")
    assert(DefaultParamInfo.StringArrayInfo.pyType === "list")
    assert(DefaultParamInfo.StringStringMapInfo.pyType === "dict")
    assert(DefaultParamInfo.StringInfo.pyType === "str")
    assert(DefaultParamInfo.UnknownInfo.pyType === "object")
  }
}
