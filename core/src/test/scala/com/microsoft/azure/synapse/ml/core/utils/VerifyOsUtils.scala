// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyOsUtils extends TestBase {

  test("IsWindows returns a boolean based on os.name property") {
    val osName = System.getProperty("os.name").toLowerCase()
    val expected = osName.indexOf("win") >= 0
    assert(OsUtils.IsWindows === expected)
  }

  test("IsWindows is consistent across multiple accesses") {
    val first = OsUtils.IsWindows
    val second = OsUtils.IsWindows
    assert(first === second)
  }
}
