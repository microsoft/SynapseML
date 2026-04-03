// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyGenerationUtils extends TestBase {

  test("indent adds correct number of spaces") {
    val input = "line1\nline2\nline3"
    val result = GenerationUtils.indent(input, 1)
    assert(result === "    line1\n    line2\n    line3")
  }

  test("indent with multiple tabs") {
    val input = "hello"
    val result = GenerationUtils.indent(input, 3)
    assert(result === "            hello")
  }

  test("indent with zero tabs") {
    val input = "hello\nworld"
    val result = GenerationUtils.indent(input, 0)
    assert(result === "hello\nworld")
  }

  test("camelToSnake converts simple camelCase") {
    assert(GenerationUtils.camelToSnake("maxIter") === "max_iter")
  }

  test("camelToSnake converts single word") {
    assert(GenerationUtils.camelToSnake("hello") === "hello")
  }

  test("camelToSnake handles leading uppercase") {
    assert(GenerationUtils.camelToSnake("GBTClassifier") === "gbt_classifier")
  }

  test("camelToSnake handles multiple uppercase transitions") {
    assert(GenerationUtils.camelToSnake("minInstancesPerNode") === "min_instances_per_node")
  }

  test("camelToSnake handles all uppercase") {
    assert(GenerationUtils.camelToSnake("ABC") === "abc")
  }

  test("camelToSnake handles digits as word boundaries") {
    assert(GenerationUtils.camelToSnake("spark3Version") === "spark_3_version")
  }
}
