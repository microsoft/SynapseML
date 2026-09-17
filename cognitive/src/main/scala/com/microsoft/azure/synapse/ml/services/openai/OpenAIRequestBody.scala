// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.param.AnyJsonFormat.anyFormat
import spray.json.DefaultJsonProtocol._
import spray.json._

private[openai] final class OpenAIRequestBody private(
    formatPath: List[String],
    cachedValue: Option[Map[_, _]]) extends Serializable {

  // Defer serialization until a request is needed, preserving skipped-row and error handling behavior.
  private lazy val cacheable: Boolean = cachedValue.exists(OpenAIRequestBody.isImmutable)
  private lazy val cachedJson: String = anyFormat.write(cachedValue.get).compactPrint

  private def matches(value: Any): Boolean = value match {
    case reference: AnyRef => cachedValue.exists(_ eq reference)
    case _ => false
  }

  private def write(value: Any, path: List[String], builder: java.lang.StringBuilder): Unit = {
    if (path.isEmpty && matches(value) && cacheable) {
      builder.append(cachedJson)
    } else {
      (value, path) match {
        case (fields: Map[_, _], key :: remaining) =>
          builder.append('{')
          var first = true
          fields.foreach {
            case (name: String, entry) =>
              if (!first) builder.append(',')
              first = false
              builder.append(JsString(name).compactPrint).append(':')
              write(entry, if (name == key) remaining else Nil, builder)
            case _ =>
              throw new IllegalArgumentException("JSON object keys must be strings")
          }
          builder.append('}')
        case _ =>
          builder.append(anyFormat.write(value).compactPrint)
      }
    }
  }

  def encode(payload: Map[String, Any]): String = {
    if (cachedValue.isEmpty) {
      payload.toJson.compactPrint
    } else {
      val builder = new java.lang.StringBuilder()
      write(payload, formatPath, builder)
      builder.toString
    }
  }
}

private[openai] object OpenAIRequestBody {
  val Uncached: OpenAIRequestBody = new OpenAIRequestBody(Nil, None)

  private def isImmutable(value: Any): Boolean = value match {
    case fields: Map[_, _] => fields.values.forall(isImmutable)
    case values: scala.collection.immutable.Seq[_] => values.forall(isImmutable)
    case _: scala.collection.Seq[_] => false
    case _ => true
  }

  private def scalar(value: Option[Either[Map[String, Any], String]]): Option[Map[String, Any]] =
    value.collect { case Left(format) if format != null => format }

  def chat(value: Option[Either[Map[String, Any], String]]): OpenAIRequestBody =
    new OpenAIRequestBody(List("response_format"), scalar(value))

  def responses(value: Option[Either[Map[String, Any], String]]): OpenAIRequestBody = {
    val format = scalar(value).flatMap(_.get("format")).collect { case fields: Map[_, _] => fields }
    new OpenAIRequestBody(List("text", "format"), format)
  }
}

private[openai] final class OpenAIRequestBodyCache(
    create: Option[Either[Map[String, Any], String]] => OpenAIRequestBody) {

  @volatile private var current =
    (Option.empty[Either[Map[String, Any], String]], OpenAIRequestBody.Uncached)

  def encode(payload: Map[String, Any], value: Option[Either[Map[String, Any], String]]): String = {
    val previous = current
    val encoder = if (previous._1.orNull eq value.orNull) {
      previous._2
    } else {
      synchronized {
        if (!(current._1.orNull eq value.orNull)) current = (value, create(value))
        current._2
      }
    }
    encoder.encode(payload)
  }
}
