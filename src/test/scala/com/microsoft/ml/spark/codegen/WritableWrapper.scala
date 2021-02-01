// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File

/** API for writing a wrapper to file
  */
abstract class WritableWrapper {
  def writeWrapperToFile(dir: File): Unit
}
