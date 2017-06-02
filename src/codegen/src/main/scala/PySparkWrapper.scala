// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import scala.collection.mutable.ListBuffer
import scala.tools.nsc.util.DocStrings

import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.Param

import com.microsoft.ml.spark.FileUtilities._
import Config._

/**
  * :: DeveloperApi ::
  * Abstraction for PySpark wrapper generators.
  */
abstract class PySparkWrapper(entryPoint: PipelineStage,
                              entryPointName: String,
                              entryPointQualifiedName: String) {

  private val ScopeDepth = "    "
  private val additionalImports = Map(
    ("complexTypes",
      s"from ${toZipDir.getName}.TypeConversionUtils import generateTypeConverter, complexTypeConverter"),
    ("utils", s"from ${toZipDir.getName}.Utils import *")
  )

  def toPySpark(): String = {
    val output = new StringBuilder()
    ""
  }

  protected val classTemplate = Seq(
    copyrightLines.mkString("\n"),
    "",
    "import sys",
    "if sys.version >= '3':",
    "    basestring = str",
    "",
    "from pyspark.ml.param.shared import *",
    "from pyspark import keyword_only",
    "from pyspark.ml.util import JavaMLReadable, JavaMLWritable",
    "from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel",
    "from pyspark.ml.common import inherit_doc",
    "%1$s",
    "","",
    "@inherit_doc",
    "class %2$s(%3$s):",
    "    \"\"\"",
    "    %9$s",
    "%11$s",
    "    \"\"\"",
    "",
    "    @keyword_only",
    "    def __init__(self, %4$s):",
    "        super(%2$s, self).__init__()",
    "        self._java_obj = self._new_java_obj(\"%5$s\")",
    "%6$s",
    // since 2.1.1, kwargs is an instance attribute...
    "        if hasattr(self, \"_input_kwargs\"):",
    "            kwargs = self._input_kwargs",
    "        else:",
    //     ... so this can be removed when we drop support for 2.1.0
    "            kwargs = self.__init__._input_kwargs",
    "        self.setParams(**kwargs)",
    "",
    "    @keyword_only",
    "    def setParams(self, %4$s):",
    "        \"\"\"",
    "        Set the (keyword only) parameters","",
    "%10$s",
    "        \"\"\"",
    "        if hasattr(self, \"_input_kwargs\"):",
    "            kwargs = self._input_kwargs",
    "        else:",
    //     ... same here: remove when we drop support for 2.1.0
    "            kwargs = self.setParams._input_kwargs",
    "        return self._set(**kwargs)\n" +
    "%7$s",
    "%8$s",
    "")

  protected val defineParamsTemplate =
    "        self.%1$s = Param(self, \"%1$s\", \"%2$s\")"
  // Complex parameters need type converters
  protected val defineComplexParamsTemplate =
    "        self.%1$s = Param(self, \"%1$s\", \"%2$s\", %3$s)"
  protected val setParamDefaultTemplate =
    "        self._setDefault(%1$s=%2$s)"
  protected val setParamDefaultWithGuidTemplate =
    "        self._setDefault(%1$s=self.uid + \"%2$s\")"
  protected val setTemplate =
    Seq("",
      "    def set%1$s(self, value):",
      "        \"\"\"\n\n%4$s:param %3$s %5$s\n%4$s\"\"\"",
      "        self._set(%2$s=value)",
      "        return self")
  protected val getTemplate =
    Seq("",
      "    def get%1$s(self):",
      "        \"\"\"",
      "        :return: %2$s",
      "        :rtype: %3$s",
      "        \"\"\"",
      "        return self.getOrDefault(self.%2$s)",
      "")
  protected val getComplexTemplate =
    Seq("",
      "    def get%1$s(self):",
      "        \"\"\"",
      "        :return: %2$s",
      "        :rtype: %3$s",
      "        \"\"\"",
      "        return self._cache[\"%2$s\"]")
  protected val saveLoadTemplate =
    Seq("",
      "    @classmethod",
      "    def read(cls):",
      "        \"\"\" Returns an MLReader instance for this class. \"\"\"",
      "        return JavaMMLReader(cls)",
      "",
      "    @staticmethod",
      "    def getJavaPackage():",
      "        \"\"\" Returns package name String. \"\"\"",
      "        return \"%1$s\"",
      "",
      "    @staticmethod",
      "    def _from_java(java_stage):",
      "        stage_name=%2$s.__module__",
      "        return from_java(java_stage, stage_name)","")

  // TODO: Get a brief description of the class from the scala and put it here. There is not a simple
  //       and intuitive way to do this via reflections, similar to the way that we are able to
  //       retrieve the parameter explanations, for example.
  protected val classDocTemplate =
      "This wraps the scala class %1$s\n"

  protected val paramDocTemplate =
      "%3$s:param %4$s %2$s"

  val psType: String
  private lazy val objectBaseClass: String = "Java" + psType
  private lazy val autoInheritedClasses = Seq("JavaMLReadable", "JavaMLWritable", objectBaseClass)
  // Complex types are not easily recognized by Py4j. They need special processing.
  private lazy val complexTypes =  Set[String](
    "TransformerParam",
    "TransformerArrayParam",
    "EstimatorParam")
  protected def isComplexType(paramType: String): Boolean = complexTypes.contains(paramType)

  protected def getParamExplanation(param: Param[_]): String = {
    entryPoint.explainParam(param)
  }

  protected def getPythonizedDefault(paramDefault: String, paramType: String,
                                     defaultStringIsParsable: Boolean): String =
    paramType match {
      case "BooleanParam" =>
        StringUtils.capitalize(paramDefault)
      case "DoubleParam" | "FloatParam" | "IntParam" | "LongParam" =>
        paramDefault
      case x if x == "Param" || defaultStringIsParsable =>
        "\"" + paramDefault + "\""
      case _ =>
        "None"
    }

  protected def getPythonizedDataType(paramType: String): String =
    paramType match {
      case "BooleanParam" => "bool"
      case "IntParam" => "int"
      case "LongParam" => "long"
      case "FloatParam" => "float"
      case "DoubleParam" => "double"
      case "StringParam" => "str"
      case "Param" => "str"
      case "StringArrayParam" => "list of str"
      case "MapArrayParam" => "dict of str to list of str"
      case _ => "object"
    }

  protected def getParamDefault(param: Param[_]): (String, String) = {
    var paramDefault:   String = null
    var pyParamDefault: String = "None"
    var autogenSuffix:  String = null
    var defaultStringIsParsable: Boolean = true

    if (entryPoint.hasDefault(param)) {
      val paramParent: String = param.parent
      paramDefault = entryPoint.getDefault(param).get.toString
      if (paramDefault.toLowerCase.contains(paramParent.toLowerCase))
        autogenSuffix = paramDefault.substring(paramDefault.lastIndexOf(paramParent)
                                               + paramParent.length)
      else {
        try{
          entryPoint.getParam(param.name).w(paramDefault)
        }
        catch{
          case e: Exception =>
            defaultStringIsParsable = false
        }
        pyParamDefault = getPythonizedDefault(paramDefault,
          param.getClass.getSimpleName, defaultStringIsParsable)
      }
    }
    (pyParamDefault, autogenSuffix)
  }

  private def defineParam(param: Param[_]): String = {
    defineParamsTemplate
  }

  protected def getPysparkWrapperBase: String = {
    // Construct relevant strings
    val imports = ListBuffer[String](additionalImports("utils"))
    val inheritedClasses = ListBuffer[String]()
    inheritedClasses ++= autoInheritedClasses
    val paramsAndDefaults           = ListBuffer[String]()
    val paramDefinitionsAndDefaults = ListBuffer[String]()
    val paramGettersAndSetters      = ListBuffer[String]()
    val paramDocList                = ListBuffer[String]()
    val classParamDocList           = ListBuffer[String]()

    // Iterate over the params to build strings
    val allParams: Array[Param[_]] = entryPoint.params
    // Check for complex types
    if(allParams.exists(p => isComplexType(p.getClass.getSimpleName))){
      // Add special imports
      imports += additionalImports("complexTypes")
      // Add cache
      paramDefinitionsAndDefaults += ScopeDepth * 2 + "self._cache = {}"
    }
    for (param <- allParams) {
      val pname = param.name
      val docType = getPythonizedDataType(param.getClass.getSimpleName)
      paramGettersAndSetters +=
        setTemplate.mkString("\n").format(StringUtils.capitalize(pname), pname, docType, ScopeDepth * 2,
          getParamExplanation(param))
      if(isComplexType(param.getClass.getSimpleName)){
        paramDefinitionsAndDefaults +=
          defineComplexParamsTemplate.format(
            pname, getParamExplanation(param),
            s"""generateTypeConverter("$pname", self._cache, complexTypeConverter)""")
        paramGettersAndSetters +=
          getComplexTemplate.mkString("\n").format(StringUtils.capitalize(pname), pname, param.getClass.getSimpleName)
        paramDocList +=
          paramDocTemplate.format(pname, getParamExplanation(param), ScopeDepth * 2, param.getClass.getSimpleName)
        classParamDocList +=
          paramDocTemplate.format(pname, getParamExplanation(param), ScopeDepth, param.getClass.getSimpleName)
      }
      else{
        paramDefinitionsAndDefaults +=
          defineParamsTemplate.format(pname, getParamExplanation(param))
        paramGettersAndSetters +=
          getTemplate.mkString("\n").format(StringUtils.capitalize(pname), pname, docType)
        paramDocList +=
          paramDocTemplate.format(pname, getParamExplanation(param), ScopeDepth * 2, docType)
        classParamDocList +=
          paramDocTemplate.format(pname, getParamExplanation(param), ScopeDepth, param.getClass.getSimpleName)
      }

      val (pyParamDefault, autogenSuffix) = getParamDefault(param)
      paramsAndDefaults += pname + "=" + pyParamDefault

      if (pyParamDefault != "None") {
        paramDefinitionsAndDefaults += setParamDefaultTemplate.format(pname, pyParamDefault)
      } else if (autogenSuffix != null) {
        paramDefinitionsAndDefaults += setParamDefaultWithGuidTemplate.format(pname, autogenSuffix)
      }
    }

    // Build strings
    val importsString = imports.mkString("\n")
    val inheritanceString = inheritedClasses.mkString(", ")
    val classParamsString = paramsAndDefaults.mkString(", ")
    val paramDefinitionsAndDefaultsString = paramDefinitionsAndDefaults.mkString("\n")
    val paramGettersAndSettersString = paramGettersAndSetters.mkString("\n")
    val saveLoadString = saveLoadTemplate.mkString("\n").format(entryPointQualifiedName, entryPointName)
    val classDocString = classDocTemplate.format(entryPointName)
    val paramDocString = paramDocList.mkString("\n")
    val classParamDocString = classParamDocList.mkString("\n")

    String.format(classTemplate.mkString("\n"), importsString, entryPointName, inheritanceString,
                    classParamsString, entryPointQualifiedName,
                    paramDefinitionsAndDefaultsString, paramGettersAndSettersString, saveLoadString,
                    classDocString, paramDocString, classParamDocString) + "\n"
  }

  def pysparkWrapperBuilder(): String = {
    getPysparkWrapperBase
  }

  def writeWrapperToFile(dir: File): Unit = {
    writeFile(new File(dir, entryPointName + ".py"), pysparkWrapperBuilder())
  }
}

class SparkTransformerWrapper(entryPoint: Transformer,
                              entryPointName: String,
                              entryPointQualifiedName: String)
    extends PySparkWrapper(entryPoint,
                           entryPointName,
                           entryPointQualifiedName) {

  override val psType = "Transformer"
}

class SparkEstimatorWrapper(entryPoint: Estimator[_],
                            entryPointName: String,
                            entryPointQualifiedName: String,
                            companionModelName: String,
                            companionModelQualifiedName: String)
  extends PySparkWrapper(entryPoint,
                         entryPointName,
                         entryPointQualifiedName) {

  private val createModelStringTemplate = Seq(
    "    def _create_model(self, java_model):",
    "        return %1$s(java_model)",
    "").mkString("\n")

  private val modelClassString = Seq(
    "class %1$s(JavaModel, JavaMLWritable, JavaMLReadable):",
    "    \"\"\"",
    "    Model fitted by :class:`%2$s`.",
    "    This class is left empty on purpose.",
    "    All necessary methods are exposed through inheritance.",
    "    \"\"\"",
    "").mkString("\n")

  override def pysparkWrapperBuilder(): String = {
    Seq(super.pysparkWrapperBuilder,
          createModelStringTemplate.format(companionModelName),
          modelClassString.format(companionModelName, entryPointName),
          saveLoadTemplate.mkString("\n").format(companionModelQualifiedName, companionModelName),
          "").mkString("\n")
  }

  override val psType = "Estimator"

}
