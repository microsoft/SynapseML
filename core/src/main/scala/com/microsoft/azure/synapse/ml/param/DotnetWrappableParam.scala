// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.Param
import spray.json._

import java.lang.{StringBuilder => JStringBuilder}

trait DotnetPrinter extends CompactPrinter {

  override protected def printArray(elements: Seq[JsValue], sb: JStringBuilder): Unit = {
    sb.append("{")
    printSeq(elements, sb.append(',')) {
      case JsArray(e) => {
        sb.append("new []")
        printArray(e, sb)
      }
      case JsObject(e) => {
        sb.append("new Dictionary<string, object>")
        printObject(e, sb)
      }
      case e => print(e, sb)
    }
    sb.append("}")
  }

  override protected def printObject(members: Map[String, JsValue], sb: JStringBuilder): Unit = {
    sb.append('{')
    printSeq(members, sb.append(',')) { m =>
      sb.append('{')
      printString(m._1, sb)
      sb.append(',')
      print(m._2, sb)
      sb.append('}')
    }
    sb.append('}')
  }

}

object DotnetPrinter extends DotnetPrinter

object DotnetWrappableParam {

  def dotnetDefaultRender[T](value: T, jsonFunc: T => String): String = {
    DotnetPrinter(jsonFunc(value).parseJson)
  }

  def dotnetDefaultRender[T](value: T)(implicit dataFormat: JsonFormat[T]): String = {
    dotnetDefaultRender(value, { v: T => v.toJson.compactPrint })
  }

  def dotnetDefaultRender[T](value: T, param: Param[T]): String = {
    dotnetDefaultRender(value, { v: T => param.jsonEncode(v) })
  }

}

// Wrapper for testgen system
trait DotnetWrappableParam[T] extends Param[T] {

  private[ml] val name: String

  // Used for generating set values for dotnet tests
  private[ml] def dotnetTestValue(v: T): String

  private[ml] def dotnetName(v: T): String = {
    name
  }

  // Used for generating dotnet tests setters
  private[ml] def dotnetTestSetterLine(v: T): String =
    s"""Set${dotnetName(v).capitalize}(${dotnetTestValue(v)})"""

  // Corresponding dotnet type used for codegen setters
  private[ml] def dotnetType: String

  // Corresponding dotnet type used for codegen getters
  // Override this if dotnet return type is different from the set type
  private[ml] def dotnetReturnType: String = dotnetType

  // Implement this for dotnet codegen setter body
  private[ml] def dotnetSetter(dotnetClassName: String,
                               capName: String,
                               dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
        |""".stripMargin
  }

  // Implement this for dotnet codegen getter body
  private[ml] def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName() =>
        |    ($dotnetReturnType)Reference.Invoke(\"get$capName\");
        |""".stripMargin
  }

}

trait ExternalDotnetWrappableParam[T] extends DotnetWrappableParam[T] {

  // Use this in tests if the param is loaded instead of constructed directly
  private[ml] def dotnetLoadLine(modelNum: Int): String

  // Used for PipelineStage Params & EvaluatorParam
  private[ml] def dotnetGetterHelper(dotnetReturnType: String,
                                     parentClassType: String,
                                     capName: String): String = {
    s"""|public $dotnetReturnType Get$capName()
        |{
        |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
        |    Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
        |                typeof($parentClassType),
        |                "s_className");
        |    JvmObjectUtils.TryConstructInstanceFromJvmObject(
        |                jvmObject,
        |                classMapping,
        |                out $dotnetReturnType instance);
        |    return instance;
        |}
        |""".stripMargin
  }

}
