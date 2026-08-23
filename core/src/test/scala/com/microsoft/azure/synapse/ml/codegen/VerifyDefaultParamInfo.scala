// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.param.ServiceParam
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import spray.json.DefaultJsonProtocol._

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

  test("ParamInfo instances publish precise stub types") {
    assert(DefaultParamInfo.pythonTypeInfo(DefaultParamInfo.LongInfo).pyiType === "int")
    assert(DefaultParamInfo.pythonTypeInfo(DefaultParamInfo.StringArrayInfo).pyiType === "List[str]")
    assert(DefaultParamInfo.pythonTypeInfo(DefaultParamInfo.DoubleArrayInfo).pyiType === "List[float]")
    assert(DefaultParamInfo.pythonTypeInfo(DefaultParamInfo.StringStringMapInfo).pyiType === "Dict[str, str]")
    assert(DefaultParamInfo.pythonTypeInfo(DefaultParamInfo.UnknownInfo).pyiType === "Any")
  }

  test("ServiceParam types are inferred from their scalar getters without adding converters") {
    class ServiceParams extends Params {
      override val uid: String = Identifiable.randomUID("ServiceParams")
      val temperature = new ServiceParam[Double](this, "temperature", "temperature")
      val model = new ServiceParam[String](this, "model", "model")

      def getTemperature: Double = 0.0
      def getModel: String = ""

      override def copy(extra: ParamMap): Params = this
    }

    val params = new ServiceParams
    val temperatureType = DefaultParamInfo.defaultPythonTypeInfo(params, params.temperature)
    val modelType = DefaultParamInfo.defaultPythonTypeInfo(params, params.model)
    val temperatureInfo = DefaultParamInfo.defaultGetParamInfo(params, params.temperature)
    val modelInfo = DefaultParamInfo.defaultGetParamInfo(params, params.model)

    assert(temperatureType.pyiType === "float")
    assert(modelType.pyiType === "str")
    assert(temperatureInfo.pyTypeConverter.isEmpty)
    assert(modelInfo.pyTypeConverter.isEmpty)
  }
}
