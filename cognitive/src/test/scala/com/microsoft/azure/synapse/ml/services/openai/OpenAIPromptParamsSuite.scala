// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.scalatest.funsuite.AnyFunSuite

class OpenAIPromptParamsSuite extends AnyFunSuite {

  private def javaMap(values: (String, String)*): java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String]()
    values.foreach { case (key, value) => result.put(key, value) }
    result
  }

  test("Java setPostProcessingOptions should infer csv mode from delimiter") {
    val prompt = new OpenAIPrompt()

    prompt.setPostProcessingOptions(javaMap("delimiter" -> ";"))

    assert(prompt.getPostProcessing === "csv")
  }

  test("Java setPostProcessingOptions should require regexGroup with regex") {
    val prompt = new OpenAIPrompt()

    intercept[IllegalArgumentException] {
      prompt.setPostProcessingOptions(javaMap("regex" -> ".*"))
    }
  }

  test("Java setPostProcessingOptions should reject options rejected by Scala overload") {
    val scalaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(Map("invalidOption" -> "value"))
    }
    val javaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(javaMap("invalidOption" -> "value"))
    }

    assert(javaError.getMessage === scalaError.getMessage)
  }
}
