// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.sql.types.{ArrayType, CharType, DataType, MapType, StringType, StructType, VarcharType}

import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.util.Try

private[openai] object OpenAIPromptPostProcessing {

  private val SupportedModes = Set("", "csv", "json", "regex")
  private val ModesByOption = Map(
    "delimiter" -> "csv",
    "jsonSchema" -> "json",
    "regex" -> "regex"
  )
  private val SupportedOptions = ModesByOption.keySet + "regexGroup"

  private def hasValidJsonMapKeys(dataType: DataType): Boolean = {
    dataType match {
      case StructType(fields) => fields.forall(field => hasValidJsonMapKeys(field.dataType))
      case ArrayType(elementType, _) => hasValidJsonMapKeys(elementType)
      case MapType(StringType, valueType, _) => hasValidJsonMapKeys(valueType)
      case _: MapType => false
      case _: CharType | _: VarcharType => false
      case _ => true
    }
  }

  private def validateJsonSchema(schema: String): Unit = {
    val validJsonSchema = Try(DataType.fromDDL(schema)).toOption.exists {
      case dataType: StructType => hasValidJsonMapKeys(dataType)
      case dataType: ArrayType => hasValidJsonMapKeys(dataType)
      case dataType @ MapType(StringType, _, _) => hasValidJsonMapKeys(dataType)
      case _ => false
    }
    if (!validJsonSchema) {
      throw new IllegalArgumentException("Invalid jsonSchema")
    }
  }

  private def validateRegex(options: Map[String, String]): Unit = {
    require(options.contains("regexGroup"), "regexGroup must be specified with regex")
    val pattern = Try(Pattern.compile(options("regex")))
      .getOrElse(throw new IllegalArgumentException("Invalid regex"))
    val regexGroup = Try(options("regexGroup").toInt).toOption
      .filter(_ >= 0)
      .getOrElse(throw new IllegalArgumentException("regexGroup must be a non-negative integer"))
    if (regexGroup > pattern.matcher("").groupCount()) {
      throw new IllegalArgumentException("regexGroup exceeds the number of capture groups")
    }
  }

  private def validateOption(modeOption: String, options: Map[String, String]): Unit = {
    modeOption match {
      case "jsonSchema" => validateJsonSchema(options("jsonSchema"))
      case "delimiter" =>
        if (Try(Pattern.compile(options("delimiter"))).isFailure) {
          throw new IllegalArgumentException("Invalid delimiter")
        }
      case "regex" => validateRegex(options)
      case _ =>
    }
  }

  def inferMode(options: Map[String, String]): Option[String] = {
    if (options.isEmpty) {
      None
    } else {
      val unsupportedOptions = options.keySet -- SupportedOptions
      val modeOptions = options.keySet.intersect(ModesByOption.keySet)
      if (unsupportedOptions.nonEmpty ||
          modeOptions.size != 1 ||
          (options.contains("regexGroup") && !options.contains("regex"))) {
        throw new IllegalArgumentException("Invalid post processing options")
      }

      val modeOption = modeOptions.head
      validateOption(modeOption, options)
      Some(ModesByOption(modeOption))
    }
  }

  def validateModeOptions(postProcessing: String, options: Map[String, String]): Unit = {
    postProcessing match {
      case "json" =>
        require(options.contains("jsonSchema"), "jsonSchema must be specified with json postProcessing")
      case "regex" =>
        require(
          options.contains("regex") && options.contains("regexGroup"),
          "regex and regexGroup must be specified with regex postProcessing"
        )
      case _ =>
    }
  }

  def validateModeValue(actualMode: String, expectedMode: String): Unit = {
    if (actualMode != expectedMode) {
      throw new IllegalArgumentException(s"postProcessing must be '$expectedMode'")
    }
  }

  private def validateSupportedMode(mode: String): Unit = {
    if (!SupportedModes.contains(mode)) {
      throw new IllegalArgumentException(s"Unsupported postProcessing mode '$mode'")
    }
  }

  def validateAndInferMode(options: java.util.HashMap[String, String], postProcessing: String): String = {
    val scalaOptions = options.asScala.toMap
    val configuredMode = Option(postProcessing)
    configuredMode.foreach(validateSupportedMode)
    val inferredMode = inferMode(scalaOptions)
    inferredMode.foreach { expectedMode =>
      configuredMode.foreach(mode => validateModeValue(mode, expectedMode))
    }
    val effectiveMode = inferredMode.orElse(configuredMode).getOrElse("")
    validateModeOptions(effectiveMode, scalaOptions)
    effectiveMode
  }

  def validateMode(prompt: OpenAIPrompt, postProcessing: String): Unit = {
    validateSupportedMode(postProcessing)
    inferMode(prompt.getPostProcessingOptions)
      .foreach(expectedMode => validateModeValue(postProcessing, expectedMode))
  }

  def validateModeWithOptions(options: java.util.HashMap[String, String], postProcessing: String): Unit = {
    validateSupportedMode(postProcessing)
    inferMode(options.asScala.toMap)
      .foreach(expectedMode => validateModeValue(postProcessing, expectedMode))
  }
}
