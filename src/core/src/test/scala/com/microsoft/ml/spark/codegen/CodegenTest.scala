// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import com.microsoft.ml.spark.core.test.base.TestBase

class CodegenTest extends TestBase {

  // This is needed because IJ has a bug where it does not run things using SBTs run settings/task
  ignore("uncomment this test to debug codegen in IntelliJ"){
    CodeGen.main(Array())
  }
}
