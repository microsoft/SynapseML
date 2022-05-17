// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.core.serialize.ComplexParam
import org.apache.spark.sql.types.{DataType, StructType}

/** Param for DataType */
class DataTypeParam(parent: Params, name: String, doc: String, isValid: DataType => Boolean)
  extends ComplexParam[DataType](parent, name, doc, isValid) with DotnetWrappableParam[DataType] {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  override def dotnetType: String = "DataType"

  override def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\",
        |    DataType.FromJson(Reference.Jvm, value.Json)));
        |""".stripMargin
  }

  override def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    var json = (string)jvmObject.Invoke(\"json\");
        |    return DataType.ParseDataType(json);
        |}
        |""".stripMargin
  }

  override def dotnetTestValue(v: DataType): String =
    v.asInstanceOf[StructType].fields.map(
      x => s"""new StructField("${x.name}", new ${x.dataType}())""".stripMargin).mkString(",")

  override def dotnetTestSetterLine(v: DataType): String =
    s"""Set${dotnetName(v).capitalize}(
       |    new StructType(new List<StructField>
       |    {${dotnetTestValue(v)}}))""".stripMargin

}
