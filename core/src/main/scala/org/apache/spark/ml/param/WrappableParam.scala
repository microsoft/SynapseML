// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

trait WrappableParam[T] extends DotnetWrappableParam[T] {

  // Corresponding dotnet type used for codegen setters and getters
  def dotnetType: String

  // Override this if dotnet return type is different from the set type
  def dotnetReturnType: String = dotnetType

}
