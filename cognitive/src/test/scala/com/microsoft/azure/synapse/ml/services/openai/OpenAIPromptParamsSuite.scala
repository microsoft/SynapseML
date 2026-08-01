// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

class OpenAIPromptParamsSuite extends TestBase {

  private def generatedPythonClass: String =
    classOf[OpenAIPrompt].getMethod("pythonClass").invoke(new OpenAIPrompt()).asInstanceOf[String]
  private def javaMap(values: (String, String)*): java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String]()
    values.foreach { case (key, value) => result.put(key, value) }
    result
  }

  private def occurrenceCount(value: String, substring: String): Int =
    value.sliding(substring.length).count(_ == substring)

  private def assertInvalidOptions(options: Map[String, String], message: String): Unit = {
    val scalaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(options)
    }
    val javaError = intercept[IllegalArgumentException] {
      new OpenAIPrompt().setPostProcessingOptions(javaMap(options.toSeq: _*))
    }

    assert(scalaError.getMessage === message)
    assert(javaError.getMessage === message)
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
    assertInvalidOptions(Map("invalidOption" -> "value"), "Invalid post processing options")
    assertInvalidOptions(
      Map("delimiter" -> ",", "jsonSchema" -> "value STRING"),
      "Invalid post processing options"
    )
    assertInvalidOptions(
      Map("delimiter" -> ",", "regexGroup" -> "0"),
      "Invalid post processing options"
    )
  }

  test("Scala and Java setPostProcessingOptions should preserve empty options") {
    val scalaPrompt = new OpenAIPrompt().setPostProcessingOptions(Map.empty[String, String])
    val javaPrompt = new OpenAIPrompt().setPostProcessingOptions(javaMap())

    assert(scalaPrompt.getPostProcessing === "")
    assert(scalaPrompt.getPostProcessingOptions.isEmpty)
    assert(javaPrompt.getPostProcessing === "")
    assert(javaPrompt.getPostProcessingOptions.isEmpty)
  }

  test("Json and regex modes should require their options") {
    Seq(
      "json" -> "jsonSchema must be specified with json postProcessing",
      "regex" -> "regex and regexGroup must be specified with regex postProcessing"
    ).foreach { case (mode, message) =>
      Seq(
        new OpenAIPrompt().setPostProcessing(mode),
        new OpenAIPrompt().setPostProcessing(mode)
      ).zipWithIndex.foreach { case (prompt, index) =>
        val setterError = intercept[IllegalArgumentException] {
          if (index == 0) {
            prompt.setPostProcessingOptions(Map.empty[String, String])
          } else {
            prompt.setPostProcessingOptions(javaMap())
          }
        }
        assert(setterError.getMessage === s"requirement failed: $message")

        val parserError = intercept[IllegalArgumentException] {
          prompt.transformSchema(StructType(Nil))
        }
        assert(parserError.getMessage === s"requirement failed: $message")
      }
    }
  }

  test("Scala and Java setPostProcessingOptions should reject malformed parser values") {
    Seq(
      Map("jsonSchema" -> "not a schema") -> "Invalid jsonSchema",
      Map("jsonSchema" -> "STRING") -> "Invalid jsonSchema",
      Map("jsonSchema" -> "MAP<INT, STRING>") -> "Invalid jsonSchema",
      Map("jsonSchema" -> "STRUCT<x: MAP<INT, STRING>>") -> "Invalid jsonSchema",
      Map("jsonSchema" -> "STRUCT<x: VARCHAR(10)>") -> "Invalid jsonSchema",
      Map("delimiter" -> "[") -> "Invalid delimiter",
      Map("regex" -> "([", "regexGroup" -> "1") -> "Invalid regex",
      Map("regex" -> "(.*)", "regexGroup" -> "not-an-integer") ->
        "regexGroup must be a non-negative integer",
      Map("regex" -> "(.*)", "regexGroup" -> "2") ->
        "regexGroup exceeds the number of capture groups"
    ).foreach { case (options, message) =>
      assertInvalidOptions(options, message)
    }
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

      assert(scalaError.getMessage === s"postProcessing must be '$inferredMode'")
      assert(javaError.getMessage === scalaError.getMessage)
    }
  }

  test("Mode setters and clear should preserve post-processing invariants") {
    Seq(
      new OpenAIPrompt().setPostProcessingOptions(Map("delimiter" -> ";")),
      new OpenAIPrompt().setPostProcessingOptions(javaMap("delimiter" -> ";"))
    ).foreach { prompt =>
      Seq("", "json").foreach { mode =>
        val error = intercept[IllegalArgumentException] {
          prompt.setPostProcessing(mode)
        }
        assert(error.getMessage === "postProcessing must be 'csv'")
      }

      prompt.setPostProcessing("csv")
      prompt.clear(prompt.postProcessing)
      assert(!prompt.isSet(prompt.postProcessing))
      assert(prompt.getPostProcessing === "")
      assert(prompt.getPostProcessingOptions === Map("delimiter" -> ";"))
      val clearError = intercept[IllegalArgumentException] {
        prompt.transformSchema(StructType(Nil))
      }
      assert(clearError.getMessage === "postProcessing must be 'csv'")
    }
  }

  test("Copy should preserve explicit mode provenance") {
    val explicitEmpty = new OpenAIPrompt().setPostProcessing("")
    val copied = explicitEmpty.copy(ParamMap.empty).asInstanceOf[OpenAIPrompt]

    val error = intercept[IllegalArgumentException] {
      copied.setPostProcessingOptions(Map("delimiter" -> ";"))
    }
    assert(error.getMessage === "postProcessing must be 'csv'")

    val csvPrompt = new OpenAIPrompt().setPostProcessingOptions(Map("delimiter" -> ";"))
    val copyError = intercept[IllegalArgumentException] {
      csvPrompt.copy(ParamMap(csvPrompt.postProcessing -> ""))
    }
    assert(copyError.getMessage === "postProcessing must be 'csv'")
  }

  test("Invalid cleared mode state should not be persisted") {
    spark
    val path = tmpDir.resolve("cleared-mode").toString
    new OpenAIPrompt().setPostProcessingOptions(Map("delimiter" -> "|"))
      .write.overwrite().save(path)

    val prompt = new OpenAIPrompt().setPostProcessingOptions(Map("delimiter" -> ";"))
    val writer = prompt.write.overwrite()
    prompt.clear(prompt.postProcessing)

    val error = intercept[IllegalArgumentException] {
      writer.save(path)
    }
    assert(error.getMessage === "postProcessing must be 'csv'")

    val preserved = OpenAIPrompt.load(path)
    assert(preserved.getPostProcessing === "csv")
    assert(preserved.getPostProcessingOptions === Map("delimiter" -> "|"))
  }

  test("Legacy loaded empty mode should support Scala and Java option setters") {
    spark
    val legacyPrompt = new OpenAIPrompt().setPostProcessingOptions(Map("delimiter" -> ";"))
    legacyPrompt.set(legacyPrompt.postProcessing, "")
    val path = tmpDir.resolve("legacy-empty-mode").toString
    legacyPrompt.write.overwrite().save(path)

    val scalaLoaded = OpenAIPrompt.load(path)
    scalaLoaded.setPostProcessingOptions(Map("delimiter" -> ":"))
    assert(scalaLoaded.getPostProcessing === "csv")
    assert(scalaLoaded.getPostProcessingOptions === Map("delimiter" -> ":"))

    val javaLoaded = OpenAIPrompt.load(path)
    javaLoaded.setPostProcessingOptions(javaMap("delimiter" -> "|"))
    assert(javaLoaded.getPostProcessing === "csv")
    assert(javaLoaded.getPostProcessingOptions === Map("delimiter" -> "|"))
  }

  test("Generated Python should contain validated setters and setParams implementation") {
    val generatedClass = generatedPythonClass

    assert(occurrenceCount(generatedClass, "def setPostProcessingOptions") === 1)
    assert(occurrenceCount(generatedClass, "def setPostProcessing(") === 1)
    assert(occurrenceCount(generatedClass, "def setParams") === 1)
    assert(generatedClass.contains("kwargs = dict(kwargs)"))
    assert(generatedClass.contains("post_processing_options = kwargs.pop(\"postProcessingOptions\", None)"))
    assert(generatedClass.contains("postProcessingOptions must be a mapping"))
    assert(generatedClass.contains("_validate_post_processing_options"))
    assert(generatedClass.contains("_validate_post_processing(value)"))
    assert(generatedClass.contains("OpenAIPromptPostProcessing.validateAndInferMode"))
    assert(generatedClass.contains("OpenAIPromptPostProcessing.validateMode"))
    assert(generatedClass.contains("OpenAIPromptPostProcessing.validateModeWithOptions"))
    assert(generatedClass.contains("isinstance(options, JavaObject)"))
    assert(generatedClass.contains("_normalize_post_processing_options"))
    assert(generatedClass.contains("_set_params_atomically"))
    assert(generatedClass.contains("_post_processing_explicitly_set"))
    assert(generatedClass.contains("def clear(self, param)"))
    assert(generatedClass.contains("def copy(self, extra=None)"))
    assert(generatedClass.contains("self._set(postProcessingOptions=value)"))
    assert(!generatedClass.contains("_post_processing_validation"))
    assert(!generatedClass.contains("applyPrevalidated"))
    assert(generatedClass.contains("_jvm.java.util.HashMap()"))
  }

  test("OpenAIPrompt Python setParams override should fail on template drift") {
    val error = intercept[IllegalArgumentException] {
      OpenAIPromptPythonOverrides.setParamsFunc("drifted template")
    }

    assert(error.getMessage === "requirement failed: OpenAIPrompt Python setParams template did not match")
  }

  test("Raw and copied postProcessingOptions should preserve legacy inference and enforce conflicts") {
    val rawPrompt = new OpenAIPrompt()
    rawPrompt.set(rawPrompt.postProcessingOptions, Map("delimiter" -> ","))
    val rawMissingModeError = intercept[IllegalArgumentException] {
      rawPrompt.transformSchema(StructType(Nil))
    }
    assert(rawMissingModeError.getMessage === "postProcessing must be 'csv'")

    val legacySource = new OpenAIPrompt()
    val legacyCopy = legacySource.copy(ParamMap(
      legacySource.postProcessingOptions -> Map("delimiter" -> ";")
    )).asInstanceOf[OpenAIPrompt]
    assert(legacyCopy.getPostProcessing === "csv")
    spark
    val copiedOptionsSchema = legacyCopy.transformSchema(StructType(Nil))
    assert(copiedOptionsSchema(legacyCopy.getOutputCol).dataType === ArrayType(StringType))

    val legacyEmptyModeSource = new OpenAIPrompt()
    legacyEmptyModeSource.set(legacyEmptyModeSource.postProcessing, "")
    legacyEmptyModeSource.set(
      legacyEmptyModeSource.postProcessingOptions,
      Map("delimiter" -> ",")
    )
    val legacyEmptyModeCopy = legacyEmptyModeSource.copy(ParamMap.empty).asInstanceOf[OpenAIPrompt]
    val legacyEmptyModeSchema = legacyEmptyModeCopy.transformSchema(StructType(Nil))
    assert(legacyEmptyModeSchema(legacyEmptyModeCopy.getOutputCol).dataType === ArrayType(StringType))

    val mismatchedPrompt = new OpenAIPrompt()
    mismatchedPrompt.set(mismatchedPrompt.postProcessing, "csv")
    mismatchedPrompt.set(mismatchedPrompt.postProcessingOptions, Map("jsonSchema" -> "value STRING"))
    val mismatchedModeError = intercept[IllegalArgumentException] {
      mismatchedPrompt.transformSchema(StructType(Nil))
    }
    assert(mismatchedModeError.getMessage === "postProcessing must be 'json'")

    val sourcePrompt = new OpenAIPrompt()
    val copiedModeError = intercept[IllegalArgumentException] {
      sourcePrompt.copy(ParamMap(
        sourcePrompt.postProcessing -> "csv",
        sourcePrompt.postProcessingOptions -> Map("jsonSchema" -> "value STRING")
      ))
    }
    assert(copiedModeError.getMessage === "postProcessing must be 'json'")

    val malformedCopyError = intercept[IllegalArgumentException] {
      sourcePrompt.copy(ParamMap(
        sourcePrompt.postProcessingOptions -> Map("delimiter" -> "[")
      ))
    }
    assert(malformedCopyError.getMessage === "Invalid delimiter")

    val malformedPrompt = new OpenAIPrompt()
    malformedPrompt.set(malformedPrompt.postProcessing, "regex")
    malformedPrompt.set(malformedPrompt.postProcessingOptions, Map("regex" -> "([", "regexGroup" -> "1"))
    val malformedError = intercept[IllegalArgumentException] {
      malformedPrompt.transformSchema(StructType(Nil))
    }
    assert(malformedError.getMessage === "Invalid regex")
  }
}
