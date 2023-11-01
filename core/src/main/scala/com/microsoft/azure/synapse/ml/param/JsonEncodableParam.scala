// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import org.apache.spark.ml.param.{Param, Params}
import spray.json.{JsonFormat, _}

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object ServiceParamJsonProtocol extends DefaultJsonProtocol {
  override implicit def eitherFormat[A: JsonFormat, B: JsonFormat]: JsonFormat[Either[A, B]] =
    new JsonFormat[Either[A, B]] {
      def write(either: Either[A, B]): JsValue = either match {
        case Left(a) => JsObject.apply(("left", a.toJson))
        case Right(b) => JsObject.apply(("right", b.toJson))
      }

      def read(value: JsValue): Either[A, B] = value.asJsObject().fields.head match {
        case ("left", jv) => Left(jv.convertTo[A])
        case ("right", jv) => Right(jv.convertTo[B])
        case _ => throw new IllegalArgumentException("Could not parse either type")
      }
    }

}

class JsonEncodableParam[T](parent: Params, name: String, doc: String, isValid: T => Boolean)
                           (@transient implicit val format: JsonFormat[T])
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String)(implicit format: JsonFormat[T]) =
    this(parent, name, doc, (_: T) => true)

  override def jsonEncode(value: T): String = {
    value.toJson.compactPrint
  }

  override def jsonDecode(json: String): T = {
    json.parseJson.convertTo[T]
  }

}


import com.microsoft.azure.synapse.ml.param.ServiceParamJsonProtocol._

object ServiceParam {
  def toSeq[T](arr: java.util.ArrayList[T]): Seq[T] = arr.asScala.toSeq
}

class ServiceParam[T: TypeTag](parent: Params,
                               name: String,
                               doc: String,
                               isValid: Either[T, String] => Boolean = (_: Either[T, String]) => true,
                               val isRequired: Boolean = false,
                               val isURLParam: Boolean = false,
                               val toValueString: T => String = { x: T => x.toString }
                              )
                              (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[Either[T, String]](parent, name, doc, isValid)
    with WrappableParam[Either[T, String]] {

  type ValueType = T

  val payloadName: String = name

  override def pyValue(v: Either[T, String]): String = {
    v match {
      case Left(t) => PythonWrappableParam.pyDefaultRender(t)
      case Right(n) => s""""$n""""
    }
  }

  override def pyName(v: Either[T, String]): String = {
    v match {
      case Left(_) => name
      case Right(_) => name + "Col"
    }
  }

  override def rValue(v: Either[T, String]): String = {
    v match {
      case Left(t) => RWrappableParam.rDefaultRender(t)
      case Right(n) => s""""$n""""
    }
  }

  override def rName(v: Either[T, String]): String = {
    v match {
      case Left(_) => name
      case Right(_) => name + "Col"
    }
  }

  override private[ml] def dotnetTestValue(v: Either[T, String]): String = {
    v match {
      case Left(t) => DotnetWrappableParam.dotnetDefaultRender(t)
      case Right(n) => s""""$n""""
    }
  }

  override private[ml] def dotnetName(v: Either[T, String]): String = {
    v match {
      case Left(_) => name
      case Right(_) => name + "Col"
    }
  }

  //scalastyle:off cyclomatic.complexity
  override private[ml] def dotnetTestSetterLine(v: Either[T, String]): String = {
    v match {
      case Left(_) => typeOf[T] match {
        case t if t =:= typeOf[Array[String]] | t =:= typeOf[Seq[String]] =>
          s"""Set${dotnetName(v).capitalize}(new string[] ${dotnetTestValue(v)})"""
        case t if t =:= typeOf[Array[Double]] =>
          s"""Set${dotnetName(v).capitalize}(new double[] ${dotnetTestValue(v)})"""
        case t if t =:= typeOf[Array[Int]] =>
          s"""Set${dotnetName(v).capitalize}(new int[] ${dotnetTestValue(v)})"""
        case t if t =:= typeOf[Array[Byte]] =>
          s"""Set${dotnetName(v).capitalize}(new byte[] ${dotnetTestValue(v)})"""
        case _ => s"""Set${dotnetName(v).capitalize}(${dotnetTestValue(v)})"""
      }
      case Right(_) => s"""Set${dotnetName(v).capitalize}(${dotnetTestValue(v)})"""
    }
  }
  //scalastyle:on cyclomatic.complexity

  //scalastyle:off cyclomatic.complexity
  private[ml] def dotnetType: String = typeOf[T].toString match {
    case "String" => "string"
    case "Boolean" => "bool"
    case "Double" => "double"
    case "Int" => "int"
    case "Seq[String]" => "string[]"
    case "Seq[Double]" => "double[]"
    case "Seq[Int]" => "int[]"
    case "Seq[Seq[Int]]" => "int[][]"
    case "Array[Byte]" => "byte[]"
    case "Seq[com.microsoft.azure.synapse.ml.services.anomaly.TimeSeriesPoint]" => "TimeSeriesPoint[]"
    case "Seq[com.microsoft.azure.synapse.ml.services.translate.TargetInput]" => "TargetInput[]"
    case "Seq[com.microsoft.azure.synapse.ml.services.translate.TextAndTranslation]" => "TextAndTranslation[]"
    case _ => throw new Exception(s"unsupported type ${typeOf[T].toString}, please add implementation")
  }
  //scalastyle:on cyclomatic.complexity

  override private[ml] def dotnetSetter(dotnetClassName: String,
                                        capName: String,
                                        dotnetClassWrapperName: String): String =
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
        |""".stripMargin

  private[ml] def dotnetSetterForSrvParamCol(dotnetClassName: String,
                                             capName: String,
                                             dotnetClassWrapperName: String): String =
    s"""|public $dotnetClassName Set${capName}Col(string value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set${capName}Col\", value));
        |""".stripMargin

  override private[ml] def dotnetGetter(capName: String): String = {
    dotnetType match {
      case "TimeSeriesPoint[]" |
           "TargetInput[]" |
           "TextAndTranslation[]" |
           "TextAnalyzeTask[]" =>
        s"""|public $dotnetType Get$capName()
            |{
            |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
            |    var jvmObjects = (JvmObjectReference[])jvmObject.Invoke("array");
            |    $dotnetType result =
            |        new ${dotnetType.substring(0, dotnetType.length - 2)}[jvmObjects.Length];
            |    for (int i = 0; i < result.Length; i++)
            |    {
            |        result[i] = new ${dotnetType.substring(0, dotnetType.length - 2)}(jvmObjects[i]);
            |    }
            |    return result;
            |}
            |""".stripMargin
      case _ =>
        s"""|public $dotnetType Get$capName() =>
            |    ($dotnetType)Reference.Invoke(\"get$capName\");
            |""".stripMargin
    }
  }

}

// Use this class if you want to extend JsonEncodableParam for Cognitive services param
class CognitiveServiceStructParam[T: TypeTag](parent: Params,
                                              name: String,
                                              doc: String,
                                              isValid: T => Boolean = (_: T) => true)
                                             (@transient implicit val dataFormat: JsonFormat[T])
  extends JsonEncodableParam[T](parent, name, doc, isValid)
    with WrappableParam[T] {

  override def pyValue(v: T): String = PythonWrappableParam.pyDefaultRender(v)

  override def rValue(v: T): String = RWrappableParam.rDefaultRender(v)

  override private[ml] def dotnetGetter(capName: String): String = {
    dotnetType match {
      case "DiagnosticsInfo" =>
        s"""|public $dotnetType Get$capName()
            |{
            |    var jvmObject = (JvmObjectReference)Reference.Invoke(\"get$capName\");
            |    return new $dotnetType(jvmObject);
            |}
            |""".stripMargin
      case _ =>
        s"""|public $dotnetType Get$capName() =>
            |    ($dotnetType)Reference.Invoke(\"get$capName\");
            |""".stripMargin
    }
  }

  override private[ml] def dotnetTestValue(v: T): String = DotnetWrappableParam.dotnetDefaultRender(v)

  override private[ml] def dotnetTestSetterLine(v: T): String = {
    typeOf[T].toString match {
      case t if t == "Seq[com.microsoft.azure.synapse.ml.services.TextAnalyzeTask]" =>
        s"""Set${dotnetName(v).capitalize}(new TextAnalyzeTask[]{${dotnetTestValue(v)}})"""
      case _ => s"""Set${dotnetName(v).capitalize}(${dotnetTestValue(v)})"""
    }
  }

  private[ml] def dotnetType: String = typeOf[T].toString match {
    case "Seq[com.microsoft.azure.synapse.ml.services.text.TextAnalyzeTask]" => "TextAnalyzeTask[]"
    case "com.microsoft.azure.synapse.ml.services.anomaly.DiagnosticsInfo" => "DiagnosticsInfo"
    case _ => throw new Exception(s"unsupported type ${typeOf[T].toString}, please add implementation")
  }
}
