// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class WrappableTests extends TestBase {

  test("test CompanionModelClassName") {
    val regressorCompanionModelClassName = new TestRegressor().getCompanionModelClassName
    assert(regressorCompanionModelClassName == "com.microsoft.azure.synapse.ml.codegen.TestRegressorModel")
  }
}
