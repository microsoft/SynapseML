// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.types.{DataType, StructType}

/** Param for DataType */
class DataTypeParam(parent: Params, name: String, doc: String, isValid: DataType => Boolean)
  extends ComplexParam[DataType](parent, name, doc, isValid) with WrappableParam[DataType] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: DataType) => true)

  override private[ml] def dotnetType: String = "DataType"

  override private[ml] def dotnetSetter(dotnetClassName: String,
                                        capName: String,
                                        dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\",
        |    DataType.FromJson(Reference.Jvm, value.Json)));
        |""".stripMargin
  }

  override private[ml] def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    var json = (string)jvmObject.Invoke(\"json\");
        |    return DataType.ParseDataType(json);
        |}
        |""".stripMargin
  }

  override private[ml] def dotnetTestValue(v: DataType): String = {
    v match {
      case st: StructType =>
        st.fields.map(
          x => s"""new StructField("${x.name}", new ${x.dataType}())""".stripMargin).mkString(",")
      case _ =>
        s"""new $v()""".stripMargin
    }
  }

  override private[ml] def dotnetTestSetterLine(v: DataType): String = {
    v match {
      case _: StructType =>
        s"""Set${dotnetName(v).capitalize}(
           |    new StructType(new List<StructField>
           |    {${dotnetTestValue(v)}}))""".stripMargin
      case _ =>
        s"""Set${dotnetName(v).capitalize}(
           |    ${dotnetTestValue(v)})""".stripMargin
    }
  }

}
