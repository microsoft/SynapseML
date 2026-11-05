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

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def setAlias(self, value):
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
           |@staticmethod
           |# Keep the binding decorator across another decorator and a blank line.
           |@_identity
           |
           |def loadOutput(spark, outputPath):
           |    return None
           |
           |@_identity
           |@classmethod
           |
           |# Comments do not end a decorator block.
           |def fromPath(cls, path):
           |    return cls()
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

  class ConflictingServiceAliasStage(override val uid: String = "conflictingServiceAliasStage")
    extends TypedPythonStage(uid) {

    override protected lazy val classNameHelper: String = "ConflictingServiceAliasStage"

    val modelCol = new Param[String](this, "modelCol", "conflicting real parameter")
  }

  class ConflictingServiceAliasMethodStage(override val uid: String = "conflictingServiceAliasMethodStage")
    extends TypedPythonStage(uid) {

    override protected lazy val classNameHelper: String = "ConflictingServiceAliasMethodStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def getModelCol(self):
           |    return "model"
           |""".stripMargin
  }

  class ConditionalPythonStage extends TypedPythonStage("conditionalPythonStage") {

    override protected lazy val classNameHelper: String = "ConditionalPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|if True:
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
           |try:
           |    def tryValue(self):
           |        return None
           |except RuntimeError:
           |    def fallbackValue(self):
           |        return None
           |""".stripMargin
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

  class DecoratedPythonStage extends TypedPythonStage("decoratedPythonStage") {

    override protected lazy val classNameHelper: String = "DecoratedPythonStage"

    override def pyAdditionalMethods: String =
      super.pyAdditionalMethods +
        """|def _with_label(label):
           |    return lambda method: method
           |
           |@staticmethod  # no instance binding
           |def commentedStatic(value):
           |    return value
           |
           |@classmethod  # bind to the class
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
           |@_with_label \
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
