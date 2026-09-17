// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.codegen.Wrappable
import com.microsoft.azure.synapse.ml.param.AnyJsonFormat

trait HasOpenAIResponseSchema extends Wrappable {

  def setResponseFormat(value: Map[String, Any]): this.type

  /** Request strict structured output from an inner JSON Schema, without its response-format envelope. */
  def setResponseSchema(schema: Map[String, Any]): this.type =
    setResponseSchema(schema, "response_schema", strict = true)

  def setResponseSchema(schema: Map[String, Any], name: String): this.type =
    setResponseSchema(schema, name, strict = true)

  def setResponseSchema(schema: Map[String, Any], strict: Boolean): this.type =
    setResponseSchema(schema, "response_schema", strict)

  /**
    * Wrap an inner JSON Schema for the stage's API. The schema is preserved without inferring
    * required fields, changing additionalProperties, or converting Spark data types.
    * Use scala.collection.immutable.ListMap for objects whose key order matters;
    * a plain Map does not guarantee insertion order.
    */
  def setResponseSchema(schema: Map[String, Any], name: String, strict: Boolean): this.type = {
    require(schema != null && schema.nonEmpty, "schema must be a non-empty JSON Schema map")
    require(name != null && name.matches("[A-Za-z0-9_-]{1,64}"),
      "schema name must contain 1 to 64 letters, digits, underscores, or hyphens")
    // Reject unsupported nested values before changing the stage's parameters.
    AnyJsonFormat.anyFormat.write(schema)
    setResponseFormat(Map(
      "type" -> "json_schema",
      "name" -> name,
      "strict" -> strict,
      "schema" -> schema
    ))
  }

  override def pyAdditionalMethods: String = {
    val docQuotes = "\"\"\""
    super.pyAdditionalMethods + s"""
      |def setResponseSchema(self, schema, name="response_schema", strict=True):
      |    $docQuotes
      |    Request structured output from a JSON Schema dictionary.
      |
      |    The response-format envelope is added automatically for this stage's API.
      |    The schema itself is not modified. For strict output, supply a schema
      |    supported by the model, including required and additionalProperties.
      |
      |    Args:
      |        schema (dict): Inner JSON Schema, without response-format metadata.
      |        name (str): Schema name, defaulting to "response_schema".
      |        strict (bool): Request strict schema adherence, defaulting to True.
      |    $docQuotes
      |    if not isinstance(schema, dict):
      |        raise TypeError("schema must be a JSON Schema dictionary")
      |    if not isinstance(strict, bool):
      |        raise TypeError("strict must be a boolean")
      |    jvm = SparkContext._active_spark_context._jvm
      |
      |    def _convert(value):
      |        if isinstance(value, dict):
      |            result = jvm.java.util.LinkedHashMap()
      |            for key, item in value.items():
      |                if not isinstance(key, str):
      |                    raise TypeError("schema dictionary keys must be strings")
      |                result.put(key, _convert(item))
      |            return result
      |        if isinstance(value, list):
      |            result = jvm.java.util.ArrayList()
      |            for item in value:
      |                result.add(_convert(item))
      |            return result
      |        return value
      |
      |    java_schema = jvm.com.microsoft.azure.synapse.ml.param.ServiceParam.toMap(_convert(schema))
      |    self._java_obj = self._java_obj.setResponseSchema(java_schema, name, strict)
      |    return self
      |""".stripMargin
  }
}
