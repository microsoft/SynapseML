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

  test("Scala and Java setPostProcessingOptions should infer csv and json modes") {
    Seq(
      Map("delimiter" -> ";") -> "csv",
      Map("jsonSchema" -> "value STRING") -> "json"
    ).foreach { case (options, expectedMode) =>
      val scalaPrompt = new OpenAIPrompt().setPostProcessingOptions(options)
      val javaPrompt = new OpenAIPrompt().setPostProcessingOptions(javaMap(options.toSeq: _*))

      assert(scalaPrompt.getPostProcessing === expectedMode)
      assert(scalaPrompt.getPostProcessingOptions === options)
      assert(javaPrompt.getPostProcessing === expectedMode)
      assert(javaPrompt.getPostProcessingOptions === options)
    }
  }

  test("Scala and Java setPostProcessingOptions should accept valid regex options") {
    val options = Map("regex" -> "value=(.*)", "regexGroup" -> "1")
    val scalaPrompt = new OpenAIPrompt().setPostProcessingOptions(options)
    val javaPrompt = new OpenAIPrompt().setPostProcessingOptions(javaMap(options.toSeq: _*))

    assert(scalaPrompt.getPostProcessing === "regex")
    assert(scalaPrompt.getPostProcessingOptions === options)
    assert(javaPrompt.getPostProcessing === "regex")
    assert(javaPrompt.getPostProcessingOptions === options)
  }

  test("Scala and Java setPostProcessingOptions should require regexGroup with regex") {
    val scalaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(Map("regex" -> ".*"))
    }
    val javaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(javaMap("regex" -> ".*"))
    }

    assert(scalaError.getMessage === "requirement failed: regexGroup must be specified with regex")
    assert(javaError.getMessage === scalaError.getMessage)
  }

  test("Scala and Java setPostProcessingOptions should reject unsupported options") {
    val scalaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(Map("invalidOption" -> "value"))
    }
    val javaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(javaMap("invalidOption" -> "value"))
    }

    assert(javaError.getMessage === scalaError.getMessage)
  }

  test("Scala and Java setPostProcessingOptions should reject conflicting explicit modes") {
    Seq(
      ("json", Map("delimiter" -> ","), "csv"),
      ("csv", Map("jsonSchema" -> "value STRING"), "json"),
      ("json", Map("regex" -> ".*", "regexGroup" -> "0"), "regex")
    ).foreach { case (explicitMode, options, inferredMode) =>
      val scalaError = intercept[IllegalArgumentException] {
        new OpenAIPrompt()
          .setPostProcessing(explicitMode)
          .setPostProcessingOptions(options)
      }
      val javaError = intercept[IllegalArgumentException] {
        new OpenAIPrompt()
          .setPostProcessing(explicitMode)
          .setPostProcessingOptions(javaMap(options.toSeq: _*))
      }

      assert(scalaError.getMessage === s"requirement failed: postProcessing must be '$inferredMode'")
      assert(javaError.getMessage === scalaError.getMessage)
    }
  }
}
