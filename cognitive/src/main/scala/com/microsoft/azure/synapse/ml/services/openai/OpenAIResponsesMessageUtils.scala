// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.{Seq => CollectionSeq}

private[openai] object OpenAIResponsesMessageUtils {
  private def runtimeType(value: Any): String =
    Option(value).map(_.getClass.getSimpleName).getOrElse("null")

  private def encodeStruct(value: Any, structType: StructType): Map[String, Any] = {
    value match {
      case row: Row =>
        if (row.length != structType.length) {
          throw new IllegalArgumentException(
            "Struct content part does not match its declared schema")
        }
        structType.fields.zipWithIndex.flatMap { case (field, index) =>
          if (row.isNullAt(index)) {
            None
          } else {
            Some(field.name -> encodeStructuredValue(row.get(index), field.dataType))
          }
        }.toMap
      case other =>
        throw new IllegalArgumentException(
          s"Expected struct content part but found ${runtimeType(other)}")
    }
  }

  private def encodeArray(value: Any, elementType: DataType): Seq[Any] = {
    value match {
      case values: CollectionSeq[_] => values.map(encodeStructuredValue(_, elementType)).toSeq
      case values: Array[_] => values.toSeq.map(encodeStructuredValue(_, elementType))
      case other =>
        throw new IllegalArgumentException(
          s"Expected array content but found ${runtimeType(other)}")
    }
  }

  private def encodeMap(value: Any, valueType: DataType): Map[String, Any] = {
    value match {
      case values: scala.collection.Map[_, _] =>
        values.iterator.map {
          case (key: String, entryValue) => key -> encodeStructuredValue(entryValue, valueType)
          case _ => throw new IllegalArgumentException("Content part map keys must be strings")
        }.filter { case (_, entryValue) => entryValue != null }.toMap
      case other =>
        throw new IllegalArgumentException(
          s"Expected map content part but found ${runtimeType(other)}")
    }
  }

  private def encodeStructuredValue(value: Any, dataType: DataType): Any = {
    if (value == null) {
      null // scalastyle:ignore null
    } else {
      dataType match {
        case structType: StructType => encodeStruct(value, structType)
        case ArrayType(elementType, _) => encodeArray(value, elementType)
        case MapType(StringType, valueType, _) => encodeMap(value, valueType)
        case _: MapType =>
          throw new IllegalArgumentException("Content part map keys must have string type")
        case _ => value
      }
    }
  }

  private def invalidRoleError(messageIndex: Int): IllegalArgumentException =
    new IllegalArgumentException(
      s"messages[$messageIndex].role must be a non-empty string")

  private def validatedRole(message: Row, messageIndex: Int): String = {
    val roleIndex = message.schema.fieldNames.indexOf("role")
    if (roleIndex < 0 || roleIndex >= message.length ||
        message.schema.fields(roleIndex).dataType != StringType || message.isNullAt(roleIndex)) {
      throw invalidRoleError(messageIndex)
    }
    message.get(roleIndex) match {
      case role: String if role.trim.nonEmpty => role
      case _ => throw invalidRoleError(messageIndex)
    }
  }

  private def encodedMessageContent(message: Row, messageIndex: Int): Any = {
    val contentIndex = message.schema.fieldNames.indexOf("content")
    if (contentIndex < 0 || contentIndex >= message.length) {
      throw new IllegalArgumentException(
        s"messages[$messageIndex].content must be a string or an array of content part objects")
    }

    message.schema.fields(contentIndex).dataType match {
      case StringType =>
        if (message.isNullAt(contentIndex)) {
          null // scalastyle:ignore null
        } else {
          message.get(contentIndex) match {
            case text: String => text
            case _ =>
              throw new IllegalArgumentException(
                s"messages[$messageIndex].content must be a string")
          }
        }
      case arrayType: ArrayType =>
        if (message.isNullAt(contentIndex)) {
          throw new IllegalArgumentException(
            s"messages[$messageIndex].content must be an array of content part objects")
        }
        encodeStructuredValue(message.get(contentIndex), arrayType)
      case other =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content has unsupported type ${other.typeName}")
    }
  }

  private def encodeResponseMessage(message: Row, messageIndex: Int): Map[String, Any] = {
    if (message == null) {
      throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
    }
    Map(
      "role" -> validatedRole(message, messageIndex),
      "content" -> encodedMessageContent(message, messageIndex)
    )
  }

  private def requireOnlyFields(
      part: Map[String, Any],
      allowed: Set[String],
      location: String): Unit = {
    if ((part.keySet -- allowed).nonEmpty) {
      throw new IllegalArgumentException(s"$location contains unsupported fields")
    }
  }

  private def requireNonEmptyString(value: Any, errorMessage: String): Unit = {
    value match {
      case text: String if text.trim.nonEmpty =>
      case _ => throw new IllegalArgumentException(errorMessage)
    }
  }

  private def validateInputTextPart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "text"), location)
    part.get("text") match {
      case Some(_: String) =>
      case _ => throw new IllegalArgumentException(s"$location requires a string 'text' field")
    }
  }

  private def validateInputImagePart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "image_url", "file_id", "detail"), location)
    val imageUrlDefined = part.get("image_url").exists { value =>
      requireNonEmptyString(
        value,
        s"$location requires a non-empty string 'image_url' or 'file_id' field")
      true
    }
    val fileIdDefined = part.get("file_id").exists { value =>
      requireNonEmptyString(
        value,
        s"$location requires a non-empty string 'image_url' or 'file_id' field")
      true
    }
    if (!imageUrlDefined && !fileIdDefined) {
      throw new IllegalArgumentException(
        s"$location requires a non-empty string 'image_url' or 'file_id' field")
    }
    part.get("detail").foreach(value =>
      requireNonEmptyString(
        value,
        s"$location 'detail' must be a non-empty string when provided"))
  }

  private def validateInputFilePart(part: Map[String, Any], location: String): Unit = {
    requireOnlyFields(part, Set("type", "file_data", "file_id", "filename"), location)
    val fileDataDefined = part.get("file_data").exists { value =>
      requireNonEmptyString(
        value,
        s"$location requires a non-empty string 'file_data' or 'file_id' field")
      true
    }
    val fileIdDefined = part.get("file_id").exists { value =>
      requireNonEmptyString(
        value,
        s"$location requires a non-empty string 'file_data' or 'file_id' field")
      true
    }
    if (!fileDataDefined && !fileIdDefined) {
      throw new IllegalArgumentException(
        s"$location requires a non-empty string 'file_data' or 'file_id' field")
    }
    part.get("filename").foreach(value =>
      requireNonEmptyString(
        value,
        s"$location 'filename' must be a non-empty string when provided"))
  }

  private def validateContentPart(part: Any, messageIndex: Int, partIndex: Int): Unit = {
    val location = s"messages[$messageIndex].content[$partIndex]"
    val fields = part match {
      case values: scala.collection.Map[_, _] =>
        values.asInstanceOf[scala.collection.Map[String, Any]].toMap
      case _ => throw new IllegalArgumentException(s"$location must be an object")
    }

    fields.get("type") match {
      case Some("input_text") => validateInputTextPart(fields, location)
      case Some("input_image") => validateInputImagePart(fields, location)
      case Some("input_file") => validateInputFilePart(fields, location)
      case Some(value: String) if value.trim.nonEmpty =>
        throw new IllegalArgumentException(
          s"$location has an unsupported type; supported types are " +
            "'input_text', 'input_image', and 'input_file'")
      case _ =>
        throw new IllegalArgumentException(
          s"$location requires a non-empty string 'type' field")
    }
  }

  private def contentItems(value: Any, messageIndex: Int): CollectionSeq[Any] = {
    val items = value match {
      case values: CollectionSeq[_] => values
      case values: Array[_] => values.toSeq
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be an array of content part objects")
    }
    if (items.isEmpty) {
      throw new IllegalArgumentException(s"messages[$messageIndex].content must not be empty")
    }
    items
  }

  private def validateEncodedMessage(message: Map[String, Any], messageIndex: Int): Unit = {
    message.get("role") match {
      case Some(role: String) if role.trim.nonEmpty =>
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].role must be a non-empty string")
    }

    message.get("content") match {
      case Some(_: String) =>
      case Some(null) => // scalastyle:ignore null
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be a string or an array of content part objects")
      case Some(content) =>
        contentItems(content, messageIndex).zipWithIndex.foreach {
          case (part, partIndex) => validateContentPart(part, messageIndex, partIndex)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"messages[$messageIndex].content must be a string or an array of content part objects")
    }
  }

  private def validateMessages(messages: CollectionSeq[Row]): Unit = {
    messages.zipWithIndex.foreach { case (message, messageIndex) =>
      if (message == null) {
        throw new IllegalArgumentException(s"messages[$messageIndex] must be an object")
      }
      val encoded = encodeResponseMessage(message, messageIndex)
      validateEncodedMessage(encoded, messageIndex)
    }
  }

  private def wrapContentParts(message: Map[String, Any]): Map[String, Any] = {
    message.get("content") match {
      case Some(value: String) =>
        val partType =
          if (message.get("role").contains("assistant")) "output_text" else "input_text"
        message.updated("content", Seq(Map("type" -> partType, "text" -> value)))
      case _ => message
    }
  }

  def validationError(messages: CollectionSeq[Row]): Option[String] = {
    Option(messages).flatMap { messageRows =>
      try {
        validateMessages(messageRows)
        None
      } catch {
        case e: IllegalArgumentException => Some(e.getMessage)
      }
    }
  }

  def encodeMessages(messages: Seq[Row]): Seq[Map[String, Any]] = {
    Option(messages).getOrElse(Seq.empty).zipWithIndex.map {
      case (message, messageIndex) =>
        val encoded = encodeResponseMessage(message, messageIndex)
        validateEncodedMessage(encoded, messageIndex)
        wrapContentParts(encoded.filter { case (_, value) => value != null })
    }
  }
}
