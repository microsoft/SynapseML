// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.fasterxml.jackson.core.{JsonProcessingException, JsonToken}
import com.microsoft.azure.synapse.ml.services.ai.AIJson
import org.apache.spark.sql.{Column, functions => F, types => T}
import org.apache.spark.sql.internal.SQLConfInjections
import org.apache.spark.sql.types.DataType

trait OutputParser {
  def parse(responseCol: Column): Column

  def outputSchema: T.DataType
}

class PassThroughParser extends OutputParser {
  def parse(responseCol: Column): Column = responseCol

  def outputSchema: T.DataType = T.StringType
}

class DelimiterParser(val delimiter: String) extends OutputParser {
  def parse(responseCol: Column): Column = F.split(F.trim(responseCol), delimiter)

  def outputSchema: T.DataType = T.ArrayType(T.StringType)
}

class JsonParser(val schema: String, options: Map[String, String]) extends OutputParser {
  private val cleanJsonString: Column => Column = col =>
    F.regexp_replace(
      F.trim(col),
      """^(```|''')?\s*json\s*|(```|''')$""",
      ""
    )

  def parse(responseCol: Column): Column =
    F.from_json(cleanJsonString(responseCol), schema, options)

  private def acceptsSingletonObject: Boolean = outputSchema match {
    case T.ArrayType(_: T.StructType, _) => true
    case _ => false
  }

  private[services] def hasParseError(responseCol: Column): Column = {
    val factory = SQLConfInjections.jsonFactory(options)
    val acceptsSingleton = acceptsSingletonObject
    val diagnosticJson = F.udf((text: String) => Option(text).flatMap { value =>
      val parser = factory.createParser(value)
      try {
        Option(parser.nextToken()).flatMap { root =>
          if (!AIJson.hasValidStringTokens(parser) || Option(parser.nextToken()).nonEmpty) None
          else {
            // Spark accepts an object as a singleton array only at the document root.
            val nested = if (acceptsSingleton && root == JsonToken.START_OBJECT) "[" + value + "\n]" else value
            Some("{\"value\":" + nested + "\n}")
          }
        }
      } catch {
        case _: JsonProcessingException => None
      } finally {
        parser.close()
      }
    })
    // A struct envelope exposes corrupt records for nested conversions in array and map roots as well.
    val corrupt = "__synapseml_corrupt_record"
    val checkedSchema = new T.StructType().add("value", outputSchema).add(corrupt, T.StringType)
    val cleaned = cleanJsonString(responseCol)
    val wrapped = diagnosticJson(cleaned)
    // The root check enforces the original limit before adding either diagnostic envelope.
    val extraDepth = if (acceptsSingleton) 2 else 1
    val depth = math.min(Int.MaxValue.toLong,
      factory.streamReadConstraints().getMaxNestingDepth.toLong + extraDepth)
    val checkedOptions = options.filterNot { case (key, _) =>
      key.equalsIgnoreCase("maxNestingDepth") || key.equalsIgnoreCase("columnNameOfCorruptRecord")
    } ++ Map("maxNestingDepth" -> depth.toString, "columnNameOfCorruptRecord" -> corrupt)
    wrapped.isNull || F.from_json(wrapped, checkedSchema, checkedOptions).getField(corrupt).isNotNull
  }

  def outputSchema: T.DataType = DataType.fromDDL(schema)
}

class RegexParser(val regex: String, val groupIdx: Int) extends OutputParser {
  def parse(responseCol: Column): Column = F.regexp_extract(responseCol, regex, groupIdx)

  def outputSchema: T.DataType = T.StringType
}
