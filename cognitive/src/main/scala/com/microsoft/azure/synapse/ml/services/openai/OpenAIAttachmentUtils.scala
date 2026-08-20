// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.io.binary.BinaryFileReader
import org.apache.hadoop.fs.{Path => HPath}

import java.io.ByteArrayInputStream
import java.net.{URI, URLConnection}
import java.nio.charset.StandardCharsets
import java.util.{Base64, Locale}
import scala.util.Try
import scala.util.control.NonFatal

private[openai] object OpenAIAttachmentUtils {

  private case class DataUrlAttachment(
      fileName: String,
      encodedContent: String,
      fileType: String,
      mimeType: String,
      originalUrl: String)

  private val MimeSniffPrefixLength = 64
  private val Utf8Bom = Base64.getDecoder.decode("77u/")
  private val XmlDeclarationPrefix = "<?xml"

  private val MimeTypeExtensions = Map(
    "image/jpeg" -> "jpg",
    "image/png" -> "png",
    "image/gif" -> "gif",
    "image/webp" -> "webp",
    "audio/mpeg" -> "mp3",
    "audio/mp3" -> "mp3",
    "audio/wav" -> "wav",
    "audio/x-wav" -> "wav",
    "application/pdf" -> "pdf",
    "text/plain" -> "txt",
    "text/markdown" -> "md",
    "text/csv" -> "csv",
    "text/tab-separated-values" -> "tsv",
    "application/json" -> "json",
    "application/xml" -> "xml",
    "text/xml" -> "xml"
  )

  private def extensionForMimeType(mimeType: String): String =
    MimeTypeExtensions.getOrElse(mimeType.toLowerCase(Locale.ROOT), "bin")

  private def dataUrlMetadata(dataUrl: String): (String, String) = {
    val separator = dataUrl.indexOf(',')
    if (separator < 0) {
      throw new IllegalArgumentException("Data URL must contain a comma separating metadata and content")
    }
    val metadata = dataUrl.substring("data:".length, separator)
    val metadataParts = metadata.split(";").toSeq
    if (!metadataParts.drop(1).exists(_.equalsIgnoreCase("base64"))) {
      throw new IllegalArgumentException("Only base64-encoded data URLs are supported for path inputs")
    }
    val mimeType = metadataParts.headOption.filter(_.nonEmpty).getOrElse("text/plain")
    mimeType -> dataUrl.substring(separator + 1)
  }

  private def pathFilename(path: String): String = {
    val pathOnly = Try(new URI(path)).toOption
      .flatMap(uri => Option(uri.getPath))
      .filter(_.nonEmpty)
      .getOrElse(path)
    Try(new HPath(pathOnly).getName).filter(_.nonEmpty).getOrElse("attachment")
  }

  def attachmentFilename(path: String): String = {
    if (path.regionMatches(true, 0, "data:", 0, "data:".length)) {
      val (mimeType, _) = dataUrlMetadata(path)
      s"attachment.${extensionForMimeType(mimeType)}"
    } else {
      pathFilename(path)
    }
  }

  private def validateFileSize(fileDescription: String, fileSizeBytes: Long, sizeLimitMB: Option[Double]): Unit = {
    sizeLimitMB.foreach { limit =>
      val limitBytes = (limit * 1024 * 1024).toLong
      if (fileSizeBytes > limitBytes) {
        val fileSizeMB = fileSizeBytes / (1024.0 * 1024.0)
        throw new IllegalArgumentException(
          f"$fileDescription size $fileSizeMB%.2f MB exceeds limit $limit%.2f MB")
      }
    }
  }

  private def isBase64Character(char: Char): Boolean =
    char >= 'A' && char <= 'Z' ||
      char >= 'a' && char <= 'z' ||
      char >= '0' && char <= '9' ||
      char == '+' || char == '/'

  private def validateBase64Content(encodedContent: String): Unit = {
    val padding = encodedContent.reverseIterator.takeWhile(_ == '=').length
    val dataLength = encodedContent.length - padding
    val remainder = dataLength % 4
    val expectedPadding = if (remainder == 0) 0 else 4 - remainder
    val invalid = !encodedContent.iterator.take(dataLength).forall(isBase64Character) ||
      padding > 2 ||
      remainder == 1 ||
      padding > 0 && (encodedContent.length % 4 != 0 || padding != expectedPadding)
    if (invalid) {
      throw new IllegalArgumentException("Data URL contains invalid base64 content")
    }
  }

  private def prepareDataUrlAttachment(
      dataUrl: String,
      sizeLimitMB: Option[Double],
      imageExtensions: Set[String],
      audioExtensions: Set[String],
      textExtensions: Set[String]
  ): DataUrlAttachment = {
    val (declaredMimeType, encodedContent) = dataUrlMetadata(dataUrl)
    val padding = encodedContent.reverseIterator.takeWhile(_ == '=').length.min(2)
    val estimatedSize = encodedContent.length.toLong * 3L / 4L - padding
    validateFileSize("Data URL attachment", estimatedSize, sizeLimitMB)
    validateBase64Content(encodedContent)
    val mimeType = declaredMimeType.toLowerCase(Locale.ROOT)
    val fileName = s"attachment.${extensionForMimeType(mimeType)}"
    val extension = extensionForMimeType(mimeType)
    val fileType = categorizeFileType(
      mimeType, extension, imageExtensions, audioExtensions, textExtensions)
    DataUrlAttachment(fileName, encodedContent, fileType, mimeType, dataUrl)
  }

  private def decodeDataUrlContent(attachment: DataUrlAttachment): Array[Byte] = {
    try {
      Base64.getDecoder.decode(attachment.encodedContent)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException("Data URL contains invalid base64 content")
    }
  }

  private def readDataUrl(
      filePathStr: String,
      sizeLimitMB: Option[Double]
  ): (String, Array[Byte], Option[String]) = {
    val (mimeType, encodedContent) = dataUrlMetadata(filePathStr)
    val padding = encodedContent.reverseIterator.takeWhile(_ == '=').length.min(2)
    val estimatedSize = encodedContent.length.toLong * 3L / 4L - padding
    validateFileSize("Data URL attachment", estimatedSize, sizeLimitMB)
    val bytes = try {
      Base64.getDecoder.decode(encodedContent)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException("Data URL contains invalid base64 content")
    }
    (attachmentFilename(filePathStr), bytes, Some(mimeType))
  }

  private def isXmlNameStart(char: Char): Boolean =
    char.isLetter || char == '_' || char == ':'

  private def startsWithXmlElement(prefix: String): Boolean =
    prefix.length > 1 && prefix.head == '<' && isXmlNameStart(prefix.charAt(1))

  private def startsWithXmlDeclaration(prefix: String): Boolean =
    prefix.length > XmlDeclarationPrefix.length &&
      prefix.startsWith(XmlDeclarationPrefix) &&
      prefix.charAt(XmlDeclarationPrefix.length).isWhitespace

  private[openai] def inferStructuredTextMimeType(fileBytes: Array[Byte]): Option[String] = {
    val prefixBytes = fileBytes.take(MimeSniffPrefixLength)
    val contentBytes = if (prefixBytes.startsWith(Utf8Bom)) prefixBytes.drop(Utf8Bom.length) else prefixBytes
    val prefix = new String(contentBytes, StandardCharsets.UTF_8).dropWhile(_.isWhitespace)
    if (prefix.startsWith("{") || prefix.startsWith("[")) {
      Some("application/json")
    } else if (startsWithXmlElement(prefix) || startsWithXmlDeclaration(prefix)) {
      Some("application/xml")
    } else {
      None
    }
  }

  private def specificMimeType(mimeType: String): Option[String] = {
    Option(mimeType).filterNot(_.equalsIgnoreCase("application/octet-stream"))
  }

  private def inferMimeType(fileName: String, fileBytes: Array[Byte]): String = {
    specificMimeType(URLConnection.guessContentTypeFromStream(new ByteArrayInputStream(fileBytes)))
      .orElse(inferStructuredTextMimeType(fileBytes))
      .orElse(specificMimeType(URLConnection.guessContentTypeFromName(fileName)))
      .getOrElse("application/octet-stream")
  }

  private def categorizeFileType(
      mimeType: String,
      extension: String,
      imageExtensions: Set[String],
      audioExtensions: Set[String],
      textExtensions: Set[String]
  ): String = {
    def hasAllowedExtension(allowedExtensions: Set[String]): Boolean = {
      val effectiveExtension = Option(extension).filter(_.nonEmpty)
        .orElse(MimeTypeExtensions.get(mimeType))
      effectiveExtension.exists(allowedExtensions.contains)
    }

    if (mimeType == "application/pdf") "file"
    else if (mimeType.startsWith("image/") && hasAllowedExtension(imageExtensions)) "image"
    else if (mimeType.startsWith("audio/") && hasAllowedExtension(audioExtensions)) "audio"
    else if (mimeType.startsWith("text/") || hasAllowedExtension(textExtensions)) "text"
    else "unsupported"
  }

  def prepareFile(
      filePathStr: String,
      sizeLimitMB: Option[Double],
      imageExtensions: Set[String],
      audioExtensions: Set[String],
      textExtensions: Set[String]
  ): (String, Array[Byte], String, String) = {
    val isDataUrl = filePathStr.regionMatches(true, 0, "data:", 0, "data:".length)
    val (fileName, fileBytes, declaredMimeType) =
      if (isDataUrl) {
        readDataUrl(filePathStr, sizeLimitMB)
      } else {
        val fileName = attachmentFilename(filePathStr)
        val fileBytes = try {
          BinaryFileReader.readSingleFileBytes(new HPath(filePathStr))
        } catch {
          case NonFatal(_) =>
            throw new IllegalArgumentException(s"Unable to read attachment '$fileName'")
        }
        (fileName, fileBytes, None)
      }

    val fileDescription = if (isDataUrl) "Data URL attachment" else s"Attachment '$fileName'"
    validateFileSize(fileDescription, fileBytes.length, sizeLimitMB)

    val extension = fileName.lastIndexOf('.') match {
      case idx if idx >= 0 => fileName.substring(idx + 1).toLowerCase(Locale.ROOT)
      case _ => ""
    }
    val mimeType = declaredMimeType.getOrElse(inferMimeType(fileName, fileBytes)).toLowerCase(Locale.ROOT)
    val fileType = categorizeFileType(
      mimeType, extension, imageExtensions, audioExtensions, textExtensions)
    (fileName, fileBytes, fileType, mimeType)
  }

  def isDataUrl(path: String): Boolean =
    path.regionMatches(true, 0, "data:", 0, "data:".length)

  def responsesDataUrlMessage(
      dataUrl: String,
      sizeLimitMB: Option[Double],
      imageExtensions: Set[String],
      audioExtensions: Set[String],
      textExtensions: Set[String],
      textWrapper: String => Map[String, String]
  ): Map[String, String] = {
    val attachment = prepareDataUrlAttachment(
      dataUrl, sizeLimitMB, imageExtensions, audioExtensions, textExtensions)
    attachment.fileType match {
      case "text" => textWrapper(new String(decodeDataUrlContent(attachment), StandardCharsets.UTF_8))
      case "image" => Map("type" -> "input_image", "image_url" -> attachment.originalUrl)
      case "audio" =>
        throw new IllegalArgumentException("Audio input is not supported in the current API version.")
      case "unsupported" => throw new IllegalArgumentException(s"Unsupported file type: ${attachment.mimeType}.")
      case "file" => Map(
        "type" -> "input_file",
        "filename" -> attachment.fileName,
        "file_data" -> attachment.originalUrl
      )
    }
  }

  def chatCompletionsDataUrlMessage(
      dataUrl: String,
      sizeLimitMB: Option[Double],
      imageExtensions: Set[String],
      audioExtensions: Set[String],
      textExtensions: Set[String],
      textWrapper: String => Map[String, String]
  ): Map[String, String] = {
    val attachment = prepareDataUrlAttachment(
      dataUrl, sizeLimitMB, imageExtensions, audioExtensions, textExtensions)
    attachment.fileType match {
      case "text" =>
        textWrapper(s"Content: ${new String(decodeDataUrlContent(attachment), StandardCharsets.UTF_8)}")
      case "image" => Map("type" -> "image_url", "image_url" -> attachment.originalUrl)
      case _ =>
        throw new IllegalArgumentException(
          s"File type '${attachment.fileType}' with MIME type '${attachment.mimeType}' " +
            "is not supported for Chat Completions. Only text and image attachments are supported; " +
            "use apiType='responses' for other file inputs.")
    }
  }

  def responsesMessage(
      fileName: String,
      fileBytes: Array[Byte],
      fileType: String,
      mimeType: String,
      textWrapper: String => Map[String, String]
  ): Map[String, String] = fileType match {
    case "text" => textWrapper(new String(fileBytes, StandardCharsets.UTF_8))
    case "image" => Map(
      "type" -> "input_image",
      "image_url" -> s"data:${mimeType};base64,${Base64.getEncoder.encodeToString(fileBytes)}"
    )
    case "audio" => throw new IllegalArgumentException("Audio input is not supported in the current API version.")
    case "unsupported" => throw new IllegalArgumentException(s"Unsupported file type: $mimeType.")
    case "file" => Map(
      "type" -> "input_file",
      "filename" -> fileName,
      "file_data" -> s"data:${mimeType};base64,${Base64.getEncoder.encodeToString(fileBytes)}"
    )
  }

  def chatCompletionsMessage(
      fileBytes: Array[Byte],
      fileType: String,
      mimeType: String,
      textWrapper: String => Map[String, String]
  ): Map[String, String] = fileType match {
    case "text" => textWrapper(s"Content: ${new String(fileBytes, StandardCharsets.UTF_8)}")
    case "image" => Map(
      "type" -> "image_url",
      "image_url" -> s"data:${mimeType};base64,${Base64.getEncoder.encodeToString(fileBytes)}"
    )
    case _ =>
      throw new IllegalArgumentException(
        s"File type '$fileType' with MIME type '$mimeType' is not supported for Chat Completions. " +
          "Only text and image attachments are supported; use apiType='responses' for other file inputs.")
  }
}
