// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyJarLoadingUtils extends TestBase {

  test("className strips .class extension and converts slashes to dots") {
    assert(JarLoadingUtils.className("com/example/MyClass.class") === "com.example.MyClass")
  }

  test("className returns input unchanged if no .class extension") {
    assert(JarLoadingUtils.className("com.example.MyClass") === "com.example.MyClass")
  }

  test("className handles nested class paths") {
    assert(JarLoadingUtils.className("a/b/c/D.class") === "a.b.c.D")
  }

  test("className handles simple filename") {
    assert(JarLoadingUtils.className("Main.class") === "Main")
  }

  test("OsUtils.IsWindows returns a boolean") {
    // We can't assert the value since it depends on the OS,
    // but we can assert the code path executes without error
    assert(OsUtils.IsWindows.isInstanceOf[Boolean])
  }
}
