// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.param

trait WrappableParam[T] extends DotnetWrappableParam[T] {

  def dotnetType: String

  def dotnetReturnType: String = dotnetType

}
