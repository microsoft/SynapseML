// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

import com.microsoft.azure.synapse.ml.codegen.{CodegenConfig, Wrappable}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.regression.LinearRegression

// Wrapper for codegen system
trait WrappableParam[T] extends DotnetWrappableParam[T] {

  // Corresponding dotnet type used for codegen setters
  def dotnetType: String

  // Corresponding dotnet type used for codegen getters
  // Override this if dotnet return type is different from the set type
  def dotnetReturnType: String = dotnetType

  // Implement this for dotnet codegen setter body
  def dotnetSetter(dotnetClassName: String, capName: String, dotnetClassWrapperName: String): String = {
    s"""|public $dotnetClassName Set$capName($dotnetType value) =>
        |    $dotnetClassWrapperName(Reference.Invoke(\"set$capName\", (object)value));
        |""".stripMargin
  }

  // Implement this for dotnet codegen getter body
  def dotnetGetter(capName: String): String = {
    s"""|public $dotnetReturnType Get$capName() =>
        |    ($dotnetReturnType)Reference.Invoke(\"get$capName\");
        |""".stripMargin
  }

}

//object WrappableExtensions {
//  implicit def toWrappable(in: PipelineStage): WrappablePipelineStage = new WrappablePipelineStage(in)
//
//  implicit def fromWrappable(in: WrappablePipelineStage): PipelineStage = in.stage
//}
//
//class WrappablePipelineStage(val stage: PipelineStage) extends Wrappable {
//
//  override protected val thisStage: Params = stage
//
//  override def copy(extra: ParamMap): Params = stage.copy(extra)
//
//  override val uid: String = stage.uid
//}
//
//object DotnetTest extends App {
//
//  import WrappableExtensions._
//
//  val Config = CodegenConfig(
//    rVersion = "1.0.0",
//    name = "mmlspark-cognitive",
//    packageName = "mmlspark",
//    pythonizedVersion = "1.0.0.dev1",
//    version = "1.0.0-rc3-154-20479925-SNAPSHOT",
//    dotnetVersion = "",
//    jarName = None,
//    topDir = "D:\\repos\\SynapseML\\dotnetClasses",
//    targetDir = "D:\\repos\\SynapseML\\dotnetClasses\\")
//
////  new LinearRegression().makeDotnetFile(Config)
////  new LogisticRegression().makeDotnetFile(Config)
////  new StringIndexer().makeDotnetFile(Config)
////  new StringIndexerModel(Array("test")).makeDotnetFile(Config)
//  new ALS().makeDotnetFile(Config)
//
//}
