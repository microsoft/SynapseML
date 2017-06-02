// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

class ValidateEnvironmentUtils extends TestBase {

  // This is more of a run harness as asserting this is obviously dumb
  ignore("Test env features") {
    println(EnvironmentUtils.IsWindows)
    println(EnvironmentUtils.GPUCount)
  }

}
