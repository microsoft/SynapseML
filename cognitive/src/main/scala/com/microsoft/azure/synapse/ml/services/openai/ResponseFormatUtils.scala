// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

object ResponseFormatUtils {
  private val SchemaHints = Set(
    "properties",
    "required",
    "additionalProperties",
    "$schema",
    "items"
  )
  private val JsonTypeKeywords = Set(
    "object",
    "array",
    "string",
    "number",
    "integer",
    "boolean",
    "null"
  )

  def normalize(value: Map[String, Any]): Map[String, Any] = {
    requireNonNull(value)
    val tOpt = rfType(value)
    val isTextOrJson = tOpt.exists(t => t == "text" || t == "json_object")
    val isJsonSchemaT = tOpt.contains("json_schema")
    val shouldFlatten = tOpt match {
      case Some(t) => JsonTypeKeywords.contains(t) || looksLikeInnerSchema(value)
      case None    => isInnerSchemaCandidate(value)
    }

    if (isTextOrJson) {
      Map("type" -> tOpt.get)
    } else if (isJsonSchemaT) {
      normalizeJsonSchema(value)
    } else if (shouldFlatten) {
      flattenToFlatJsonSchema(value)
    } else if (tOpt.isEmpty) {
      requireBareJsonSchemaShape(value)
      Map("type" -> "json_schema") ++ value
    } else {
      val other = tOpt.get
      throw new IllegalArgumentException(
        "Unsupported response_format type: '" + other + "'. Allowed: 'text', 'json_object', 'json_schema'.")
    }
  }

  private def normalizeJsonSchema(value: Map[String, Any]): Map[String, Any] = {
    val hasNested = value.contains("json_schema")
    val hasFlat = value.contains("name") && value.contains("schema")
    if (!hasNested && !hasFlat) {
      throw new IllegalArgumentException(
        "json_schema requires nested 'json_schema' or top-level 'name' and 'schema'.")
    }
    if (hasFlat) {
      val base = Map(
        "type" -> "json_schema",
        "name" -> value("name"),
        "schema" -> value("schema")
      )
      if (value.contains("strict")) base + ("strict" -> value("strict")) else base
    } else {
      val js = value("json_schema").asInstanceOf[Map[String, Any]]
      val name = js.getOrElse("name",
        throw new IllegalArgumentException("json_schema.name required"))
      val schema = js.getOrElse("schema",
        throw new IllegalArgumentException("json_schema.schema required"))
      val base = Map("type" -> "json_schema", "name" -> name, "schema" -> schema)
      js.get("strict").map(v => base + ("strict" -> v)).getOrElse(base)
    }
  }

  private def requireBareJsonSchemaShape(value: Map[String, Any]): Unit = {
    val hasName = value.contains("name")
    val hasSchema = value.contains("schema")
    if (!hasName || !hasSchema) {
      throw new IllegalArgumentException(
        "Bare schema Map must include 'name' and 'schema' keys.")
    }
  }

  private def rfType(value: Map[String, Any]): Option[String] =
    value.get("type").map(_.toString.trim.toLowerCase)

  private def looksLikeInnerSchema(m: Map[String, Any]): Boolean = {
    val keys = m.keySet.map(_.toString)
    keys.exists(SchemaHints.contains)
  }

  private def isInnerSchemaCandidate(value: Map[String, Any]): Boolean = {
    val tOpt = rfType(value)
    val isKnown = tOpt.exists(t => t == "text" || t == "json_object" || t == "json_schema")
    val isJsonType = tOpt.exists(JsonTypeKeywords.contains)
    (!isKnown && (looksLikeInnerSchema(value) || isJsonType))
  }

  private def flattenToFlatJsonSchema(value: Map[String, Any]): Map[String, Any] = {
    val name = value.get("name").map(_.toString).getOrElse("response_schema")
    val strictOpt = value.get("strict")
    val innerSchema = value - "name" - "strict"
    val base = Map("type" -> "json_schema", "name" -> name, "schema" -> innerSchema)
    strictOpt.map(v => base + ("strict" -> v)).getOrElse(base)
  }

  private def requireNonNull(v: Any): Unit = {
    if (v == null) {
      throw new IllegalArgumentException("response_format must be a non-null Map")
    }
  }
}
