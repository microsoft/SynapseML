// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

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
