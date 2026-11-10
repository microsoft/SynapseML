// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, StringArrayParam}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import spray.json.DefaultJsonProtocol._

// Nesting does not hide these JVM classes; discovery must exclude their test artifact.
private[codegen] object PyCodegenFixtures {

  class TypedPythonStage(override val uid: String = "typedPythonStage")
    extends Transformer with Wrappable {

    override protected lazy val classNameHelper: String = "TypedPythonStage"

    val count = new DoubleParam(this, "count", "numeric value")
    val labels = new StringArrayParam(this, "labels", "labels")
    val model = new com.microsoft.azure.synapse.ml.param.ServiceParam[String](
      this, "model", "model name")
    val text = new Param[String](this, "text", "text value")

    def getModel: String = ""
    def setAlias(value: String): this.type = this
    def setPositionalAlias(value: String): this.type = this

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def setAlias(self, \
           |    *, value):
           |    return self
           |
           |def setPositionalAlias(self, \
           |    value, /):
           |    return self
           |
           |def clear(self, param):
           |    return None
           |
           |def copy(self, extra=None):
           |    return self
           |
           |def _identity(method):
           |    return method
           |
           |def loadOutput(spark, outputPath):
           |    return None
           |
           |@staticmethod
           |# Keep the binding decorator across another decorator and a blank line.
           |@_identity
           |
           |def loadOutput(spark, outputPath):
           |    return None
           |
           |@staticmethod
           |def fromPath(cls, path):
           |    return None
           |
           |@_identity
           |@classmethod
           |
           |# Comments do not end a decorator block.
           |def fromPath(cls, path):
           |    return cls()
           |
           |@staticmethod
           |def describe(self):
           |    return None
           |
           |def describe(self):
           |    def check_keys(item):
           |        return item
           |    def getModelCol():
           |        return check_keys("")
           |    return getModelCol()
           |""".stripMargin

    override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

    override def transformSchema(schema: StructType): StructType = schema

    override def copy(extra: ParamMap): TypedPythonStage = defaultCopy(extra)
  }

  class ForeignParamPythonStage extends TypedPythonStage("foreignParamPythonStage") {

    override protected lazy val classNameHelper: String = "ForeignParamPythonStage"

    override val text = new Param[String]("otherStage", "text", "text value")
  }

  def conflictingAliasNames: Seq[(String, String)] = Seq(
    "model" -> "modelCol", "caf\u00e9" -> "cafe\u0301Col",
    "mo\u212adel" -> "moKdelCol", "model" -> "model\uA7F2ol")

  class ConflictingServiceAliasStage(
      override val uid: String = "conflictingServiceAliasStage",
      paramName: String = "model", aliasName: String = "modelCol")
    extends TypedPythonStage(uid) {

    override protected lazy val classNameHelper: String = "ConflictingServiceAliasStage"

    override val model = new com.microsoft.azure.synapse.ml.param.ServiceParam[String](
      this, paramName, "model name")
    val modelCol = new Param[String](this, aliasName, "conflicting real parameter")
  }

  class ConflictingServiceAliasMethodStage(
      override val uid: String = "conflictingServiceAliasMethodStage", declaration: String = "def ",
      paramName: String = "model", accessor: String = "get", methodParamName: Option[String] = None)
    extends TypedPythonStage(uid) {

    override protected lazy val classNameHelper: String = "ConflictingServiceAliasMethodStage"

    override val model = new com.microsoft.azure.synapse.ml.param.ServiceParam[String](
      this, paramName, "model name")
    val conflictingMethod: String = s"$accessor${paramName.capitalize}Col"
    private val declaredMethod = s"$accessor${methodParamName.getOrElse(paramName).capitalize}Col"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|$declaration$declaredMethod(self, *args):
           |    return "model"
           |""".stripMargin
  }

  def conflictingAliasStages: Seq[(String, TypedPythonStage, String)] = {
    val declarations = Seq("def ", "async def ", "async \\\ndef ", "async\\\ndef\\\n")
    val ascii = declarations.map { declaration =>
      (declaration.replace("\n", "\\n"),
        new ConflictingServiceAliasMethodStage(declaration = declaration), "getModelCol")
    } :+ ("conditional", new ConflictingConditionalAliasMethodStage(), "getModelCol")
    val unicode = for {
      paramName <- Seq("caf\u00e9", "mod\u0301el", "mo\u2118del")
      accessor <- Seq("get", "set")
    } yield {
      val stage = new ConflictingServiceAliasMethodStage(paramName = paramName, accessor = accessor)
      (s"Unicode ${stage.conflictingMethod}", stage, stage.conflictingMethod)
    }
    val normalized = for {
      (paramName, methodParamName) <- Seq("caf\u00e9" -> "cafe\u0301", "mo\u212adel" -> "moKdel")
      accessor <- Seq("get", "set")
    } yield {
      val stage = new ConflictingServiceAliasMethodStage(paramName = paramName,
        accessor = accessor, methodParamName = Some(methodParamName))
      (s"normalized Unicode ${stage.conflictingMethod}", stage, stage.conflictingMethod)
    }
    val unicode14 = new ConflictingServiceAliasMethodStage() {
      override def pyAdditionalMethods: String =
        "def getModel\uA7F2ol(self):\n    return None\n"
    }
    ascii ++ unicode ++ normalized :+ ("Unicode 14", unicode14, "getModelCol")
  }

  class ConditionalPythonStage extends TypedPythonStage("conditionalPythonStage") {

    override protected lazy val classNameHelper: String = "ConditionalPythonStage"

    private val conditionalAlternatives =
      s"""|intorstr = str
         |${"\u2118"} = int
         |not${"\u2118"} = str
         |def annotated(self, value:list[str], quoted:"left right") -> dict[str,int]:
         |    return value
         |def literalChange(self, value:"left right"):
         |    return value
         |def keywordChange(self, value:int or str):
         |    return value
         |def identifierBoundary(self, value:not${"\u2118"}):
         |    return value
         |if True:
         |    def annotated(self, value : list[ str ], quoted : "left right") -> dict[ str , int ]:
         |        return value
         |    def literalChange(self, value: "leftright"):
         |        return value
         |    def keywordChange(self, value: intorstr):
         |        return value
         |    def identifierBoundary(self, value:not ${"\u2118"}):
         |        return value
         |    def certain(self):
         |        return None
         |    async def conditionalAsync(self, value):
         |        return value
         |    @staticmethod
         |    def bindingChange(value):
         |        return value
         |    def formatted(self,value):
         |        return value
         |else:
         |    def conditionalAsync(self):
         |        return None
         |    def bindingChange(self, value):
         |        return value
         |    def formatted(self, value):
         |        return value
         |""".stripMargin

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|def maybe(self):
           |    return None
           |
           |if True:
           |    def maybe(self, value):
           |        return value
           |    def certain(self, value):
           |        return value
           |    @staticmethod
           |    # A conditional method is still a member of the generated class.
           |    def conditionalValue(value):
           |        def hiddenConditional():
           |            return value
           |        return hiddenConditional()
           |
           |    class NestedHelper:
           |        def getModelCol(self):
           |            return "nested"
           |        def nestedOnly(self):
           |            return None
           |
           |def certain(self):
           |    return None
           |
           |try:
           |    def tryValue(self):
           |        return None
           |except RuntimeError:
           |    def fallbackValue(self):
           |        return None
           |
           |def relabel(self):
           |    return None
           |
           |def ${"\uFF52"}elabel(self, value):
           |    return value
           |
           |def refresh(self):
           |    return None
           |
           |async def refresh(self, value):
           |    return value
           |""".stripMargin + conditionalAlternatives
  }

  private val LambdaDefaultArguments = Seq(
    "key=lambda value: value" -> "Any",
    "key=lambda: None" -> "Any",
    "key=[lambda left,right: left]" -> "Any",
    "key=lambda value=lambda: None: value" -> "Any",
    """key: "Callable[[int], int]" = lambda value: value""" -> "\"Callable[[int], int]\"")

  val ContinuedDefaultStubs: Seq[String] =
    Seq("def continuedDefault(self, labels: Any = ..., note: Any = ..., metadata: Any = ..., " +
      "reference: Any = ...) -> Any: ...") ++ LambdaDefaultArguments.zipWithIndex.map { case ((_, annotation), i) =>
      s"def lambdaDefault$i(self, key: $annotation = ...) -> Any: ..."
    }

  class ContinuedDefaultPythonStage extends TypedPythonStage("continuedDefaultPythonStage") {
    override protected lazy val classNameHelper: String = "ContinuedDefaultPythonStage"
    override def pyAdditionalMethods: String = super.pyAdditionalMethods + s"lambda${"\u2118"} = None\n" +
      "def continuedDefault(self, \\\n    labels=[\"left\", \"right\"], note=\"left,right\", " +
      "metadata={\"left\": \"right,left\", \"right\": \"value\"}, " +
      s"reference=lambda${"\u2118"}):\n    return labels\n" +
      "def unsupportedLambda(self, \\\n    handler=lambda left,right: right):\n    return handler\n" +
      LambdaDefaultArguments.zipWithIndex.map { case ((argument, _), i) =>
        s"def lambdaDefault$i(self, $argument):\n    return key\n"
      }.mkString +
      "def afterScope(self):\n    return None\n"
  }

  class ConflictingConditionalAliasMethodStage extends TypedPythonStage("conflictingConditionalAliasMethodStage") {

    override protected lazy val classNameHelper: String = "ConflictingConditionalAliasMethodStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|if True:
           |    try:
           |        def getModelCol(self):
           |            return "model"
           |    except RuntimeError:
           |        pass
           |""".stripMargin
  }

  class MultilineFunctionPythonStage extends TypedPythonStage("multilineFunctionPythonStage") {

    override protected lazy val classNameHelper: String = "MultilineFunctionPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def describe(
           |    self,
           |    label=")",
           |    # A closing delimiter in a comment does not finish this header: )
           |):
           |    def getModelCol():
           |        return label
           |    return getModelCol()
           |
           |def afterScope(self):
           |    return None
           |""".stripMargin
  }

  class MultilineClassPythonStage extends TypedPythonStage("multilineClassPythonStage") {

    override protected lazy val classNameHelper: String = "MultilineClassPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|class NestedHelper(
           |    object,
           |):
           |    def setModelCol(self, value):
           |        return value
           |    def nestedOnly(self):
           |        return None
           |
           |def afterScope(self):
           |    return None
           |""".stripMargin
  }

  class ContinuedScopePythonStage(tight: Boolean = false) extends TypedPythonStage("continuedScopePythonStage") {

    override protected lazy val classNameHelper: String = "ContinuedScopePythonStage"

    override def pyAdditionalMethods: String = {
      val classHeader = if (tight) "class\\\nNestedHelper:" else "\\\nclass NestedHelper:"
      val asyncHeader = if (tight) "async\\\ndef\\\nasyncScope(self):" else "async \\\ndef asyncScope(self):"
      super.pyAdditionalMethods +
        s"""|$classHeader
           |    def getModelCol(self):
           |        return "nested"
           |    def nestedOnly(self):
           |        return None
           |
           |$asyncHeader
           |    def getModelCol():
           |        return "nested"
           |    return getModelCol()
           |
           |def afterScope(self):
           |    return None
           |""".stripMargin
    }
  }

  class MultilineStringPythonStage(quote: String) extends TypedPythonStage("multilineStringPythonStage") {

    override protected lazy val classNameHelper: String = "MultilineStringPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|def describe(
            |    self,
            |    label=$quote(
            |    ' " [ { # still inside the string
            |# A string line can end its own literal: $quote,
            |):
            |    def getModelCol():
            |        return label
            |    return getModelCol()
            |
            |def afterScope(self):
            |    return None
            |""".stripMargin
  }

  class ConflictingMultilineStringAliasMethodStage(quote: String) extends MultilineStringPythonStage(quote) {

    override protected lazy val classNameHelper: String = "ConflictingMultilineStringAliasMethodStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def getModelCol(self):
           |    return "model"
           |""".stripMargin
  }

  class BodyStringPythonStage(scopeHeader: String, methodIndent: String = "")
    extends TypedPythonStage("bodyStringPythonStage") {

    override protected lazy val classNameHelper: String = "BodyStringPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|$scopeHeader
            |    label = '''
            |This is string content, not a dedent.
            |'''
            |    def getModelCol():
            |        return "nested"
            |    def nestedOnly():
            |        return None
            |
            |${methodIndent}def afterScope(self):
            |    return None
            |""".stripMargin
  }

  class ConflictingBodyStringAliasMethodStage(scopeHeader: String, methodIndent: String = "")
    extends TypedPythonStage("conflictingBodyStringAliasMethodStage") {

    override protected lazy val classNameHelper: String = "ConflictingBodyStringAliasMethodStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|$scopeHeader
            |    label = '''
            |def notAHeader(
            |'''
            |
            |${methodIndent}def getModelCol(self):
            |    return "model"
            |""".stripMargin
  }

  class ClassStatementStringPythonStage(
      controlBlock: Boolean, methodName: String, declaration: String = "def ",
      literal: String = "'''\ndef notAHeader(\n'''")
    extends TypedPythonStage("classStatementStringPythonStage") {

    override protected lazy val classNameHelper: String = "ClassStatementStringPythonStage"

    override def pyAdditionalMethods: String = {
      val prefix = if (controlBlock) "if True:\n    " else ""
      super.pyAdditionalMethods +
        s"""|${prefix}label = $literal
            |
            |$declaration$methodName(self):
            |    return None
            |""".stripMargin
    }
  }

  class DecoratedPythonStage(binding: String = "@%s # binding") extends TypedPythonStage("decoratedPythonStage") {

    override protected lazy val classNameHelper: String = "DecoratedPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        s"""|def _with_label(label):
           |    return lambda method: method
           |
           |${binding.format("staticmethod")}
           |def commentedStatic(value):
           |    return value
           |
           |${binding.format("classmethod")}
           |def commentedClass(cls, value):
           |    return cls()
           |
           |@staticmethod
           |@_with_label(
           |    {"labels": ["(", ")"]},  # )
           |)
           |def decoratedStatic(value):
           |    return value
           |
           |@classmethod
           |@_with_label(
           |    '''(
           |# still inside the label: ''',
           |)
           |def decoratedClass(cls, value):
           |    return cls()
           |
           |@staticmethod
           |@_with_label \\
           |    ("label")
           |def continuedStatic(value):
           |    return value
           |
           |def plainValue(self):
           |    return None
           |""".stripMargin
  }

  class TypedPythonModel(override val uid: String = "typedPythonModel")
    extends Model[TypedPythonModel] with Wrappable {

    override protected lazy val classNameHelper: String = "TypedPythonModel"

    override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

    override def transformSchema(schema: StructType): StructType = schema

    override def copy(extra: ParamMap): TypedPythonModel = defaultCopy(extra)
  }

  class TypedPythonEstimator(override val uid: String = "typedPythonEstimator")
    extends Estimator[TypedPythonModel] with Wrappable {

    override protected lazy val classNameHelper: String = "TypedPythonEstimator"

    override protected def companionModelClassName: String =
      "com.microsoft.azure.synapse.ml.codegen.TypedPythonModel"

    override def fit(dataset: Dataset[_]): TypedPythonModel = new TypedPythonModel()

    override def transformSchema(schema: StructType): StructType = schema

    override def copy(extra: ParamMap): TypedPythonEstimator = defaultCopy(extra)
  }

  class TypedPythonClassificationModel(override val uid: String = "typedPythonClassificationModel")
    extends ProbabilisticClassificationModel[Vector, TypedPythonClassificationModel] with Wrappable {

    override protected lazy val classNameHelper: String = "TypedPythonClassificationModel"

    override def numClasses: Int = 2

    override def predictRaw(features: Vector): Vector = Vectors.dense(0.0, 1.0)

    override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = rawPrediction

    override def copy(extra: ParamMap): TypedPythonClassificationModel = defaultCopy(extra)
  }

  class TypedPythonClassifier(override val uid: String = "typedPythonClassifier")
    extends ProbabilisticClassifier[Vector, TypedPythonClassifier, TypedPythonClassificationModel] with Wrappable {

    override protected lazy val classNameHelper: String = "TypedPythonClassifier"

    override protected def companionModelClassName: String =
      "com.microsoft.azure.synapse.ml.codegen.TypedPythonClassificationModel"

    override protected def train(dataset: Dataset[_]): TypedPythonClassificationModel =
      new TypedPythonClassificationModel()

    override def copy(extra: ParamMap): TypedPythonClassifier = defaultCopy(extra)
  }
}
