// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.ai

import com.microsoft.azure.synapse.ml.services.openai.OpenAIAttachmentUtils
import spray.json.{JsArray, JsBoolean, JsNull, JsObject, JsString, JsValue}

/** Shared validation for the native leaves and the facade, without rewriting provider JSON. */
private[services] object NativeQuestions {
  private val MaxChoices = 255
  private val MaxImages = 128
  private val InlineImagePrefix = "data:image/"
  private val UrlEscape = "%[0-9a-fA-F]{2}".r
  private val UrlToken = "[a-zA-Z0-9!$&'*+._~-]+".r
  private def check(condition: Boolean, message: String): Unit =
    require(condition, message)

  private def obj(value: JsValue): Map[String, JsValue] = value match {
    case JsObject(fields) => fields
    case _ => throw new IllegalArgumentException("Native question or input member must be an object")
  }

  private def array(value: JsValue): Vector[JsValue] = value match {
    case JsArray(values) if values.nonEmpty => values
    case _ => throw new IllegalArgumentException("Native question or input array must be nonempty")
  }

  private def text(value: JsValue): String = value match {
    case JsString(s) if s.trim.nonEmpty => s
    case _ => throw new IllegalArgumentException("Native question or input text must be nonempty")
  }

  private def required(fields: Map[String, JsValue], name: String): JsValue =
    fields.getOrElse(name, throw new IllegalArgumentException(s"Native request requires $name"))

  private def optionalText(fields: Map[String, JsValue], name: String): Option[String] =
    fields.get(name).filterNot(_ == JsNull).map {
      case JsString(value) => value
      case _ => throw new IllegalArgumentException("Optional native text fields must be strings")
    }

  private def choiceValue(value: JsValue): Unit = value match {
    case _: JsString | _: JsBoolean =>
    case _ => throw new IllegalArgumentException("Decision choice values must be strings or booleans")
  }

  def validate(apiType: String, questions: JsValue): Unit = apiType match {
    case "systemone" => systemOne(questions)
    case "decisions" => decisions(questions)
    case _ => throw new IllegalArgumentException("Native questions require systemone or decisions")
  }

  private def systemOne(questions: JsValue): Unit = {
    val named = obj(questions)
    check(named.nonEmpty, "System One questions must be nonempty")
    named.foreach { case (name, question) =>
      check(name.trim.nonEmpty, "System One question names must be nonempty")
      val fields = obj(question)
      text(required(fields, "instructions"))
      text(required(fields, "type")) match {
        case "noul" =>
          fields.get("criteria").foreach { value =>
            val criteria = obj(value)
            check(criteria.keySet == Set("true", "false"),
              "System One noul criteria must describe true and false")
            criteria.values.foreach(text)
          }
        case "choice" =>
          val criteria = obj(required(fields, "criteria"))
          check(criteria.size >= 2, "System One choice requires at least two criteria")
          criteria.foreach { case (label, description) =>
            check(label.trim.nonEmpty, "System One choice labels must be nonempty")
            text(description)
          }
        case "score" =>
          array(required(fields, "criteria")).foreach(text)
        case _ => throw new IllegalArgumentException("Unsupported System One question type")
      }
    }
  }

  private def decisions(questions: JsValue): Unit = {
    val names = array(questions).flatMap { question =>
      val fields = obj(question)
      text(required(fields, "instructions"))
      text(required(fields, "type")) match {
        case "predicate" =>
        case "choice" =>
          val choices = array(required(fields, "choices"))
          check(choices.size >= 2 && choices.size <= MaxChoices, "Decisions requires 2 to 255 choices")
          val values = choices.map { choice =>
            val option = obj(choice)
            val value = required(option, "value")
            choiceValue(value)
            optionalText(option, "description")
            value
          }
          check(values.distinct.size == values.size, "Decision choice values must be unique")
        case "score" =>
          array(required(fields, "levels")).foreach { level =>
            val fields = obj(level)
            optionalText(fields, "description")
            text(required(fields, "label"))
          }
        case _ => throw new IllegalArgumentException("Unsupported Decisions question type")
      }
      optionalText(fields, "name")
    }
    check(names.distinct.size == names.size, "Decision question names must be unique")
  }

  /** The guide and reference disagree on hosted images; accept only their inline-image common subset. */
  def validateDecisionsInput(input: JsValue): Unit = input match {
    case _: JsString => text(input)
    case _: JsArray =>
      val images = array(input).map { message =>
        val fields = obj(message)
        check(required(fields, "role") == JsString("user"), "Decisions accepts only user messages")
        fields.get("type").filterNot(_ == JsNull).foreach { value =>
          check(value == JsString("message"), "Decisions message type must be message")
        }
        required(fields, "content") match {
          case value: JsString => text(value); 0
          case value: JsArray => array(value).map(validatePart).sum
          case _ => throw new IllegalArgumentException("Unsupported Decisions message content")
        }
      }.sum
      check(images <= MaxImages, "Decisions accepts at most 128 images")
    case _ => throw new IllegalArgumentException("Decisions input must be text or user messages")
  }

  def prepareDecisionsInput(input: JsValue): JsValue = {
    validateDecisionsInput(input)
    input match {
      case JsArray(messages) => JsArray(messages.map { message =>
        val fields = omitNulls(obj(message), Seq("type"))
        val content = fields("content") match {
          case JsArray(parts) => JsArray(parts.map { part =>
            val fields = obj(part)
            if (fields("type") == JsString("input_image")) JsObject(omitNulls(fields, Seq("file_id", "detail")))
            else part
          })
          case text => text
        }
        JsObject(fields.updated("content", content))
      })
      case text => text
    }
  }

  private def omitNulls(fields: Map[String, JsValue], names: Seq[String]): Map[String, JsValue] =
    names.foldLeft(fields) { case (result, name) =>
      if (result.get(name).contains(JsNull)) result - name else result
    }

  private def inlineImageHeader(header: String): Boolean = {
    // RFC 2397 uses URL escapes, rather than quoted strings, for MIME parameter values.
    def token(value: String): Boolean = UrlToken.pattern.matcher(UrlEscape.replaceAllIn(value, "x")).matches()
    val parts = header.split(";", -1)
    parts.length >= 2 && parts.head.regionMatches(true, 0, InlineImagePrefix, 0, InlineImagePrefix.length) &&
      token(parts.head.substring(InlineImagePrefix.length)) && parts.last.equalsIgnoreCase("base64") &&
      parts.slice(1, parts.length - 1).forall { parameter =>
        val equals = parameter.indexOf('=')
        equals > 0 && token(parameter.substring(0, equals)) && token(parameter.substring(equals + 1))
      }
  }

  private def validatePart(part: JsValue): Int = {
    val fields = obj(part)
    text(required(fields, "type")) match {
      case "input_text" =>
        text(required(fields, "text"))
        0
      case "input_image" =>
        check(fields.get("file_id").forall(_ == JsNull), "Decisions file_id images are unsupported")
        fields.get("detail").filterNot(_ == JsNull).foreach { value =>
          check(Set("low", "high", "auto", "original").contains(text(value)), "Unsupported image detail")
        }
        val url = text(required(fields, "image_url"))
        val comma = url.indexOf(',')
        check(comma > 0 && inlineImageHeader(url.substring(0, comma)),
          "Decisions images must be inline base64 image data URLs")
        val encoded = url.substring(comma + 1)
        try {
          OpenAIAttachmentUtils.validateBase64Content(encoded)
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException("Decisions image data must be valid base64")
        }
        check(encoded.nonEmpty, "Decisions image data must be nonempty")
        1
      case _ => throw new IllegalArgumentException("Unsupported Decisions input part")
    }
  }
}
