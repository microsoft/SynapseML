// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.openai

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.io.File
import java.net.{FileNameMap, InetSocketAddress, URLConnection}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.ConcurrentLinkedQueue

class OpenAIPromptOfficePathSuite extends TestBase {

  test("Office HTTP attachments use URL path extensions rather than response Content-Type") {
    val fixture = new File(getClass.getResource("/office/invoice.docx").toURI).toPath
    val bytes = Files.readAllBytes(fixture)
    val mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    val requests = new ConcurrentLinkedQueue[String]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    try {
      Seq("/files/invoice.docx" -> "application/zip", "/download" -> mimeType).foreach { case (path, contentType) =>
        server.createContext(path, new HttpHandler {
          override def handle(exchange: HttpExchange): Unit = {
            try {
              requests.add(exchange.getRequestURI.toString)
              exchange.getResponseHeaders.add("Content-Type", contentType)
              exchange.sendResponseHeaders(200, bytes.length)
              exchange.getResponseBody.write(bytes)
            } finally {
              exchange.close()
            }
          }
        })
      }
      server.start()
      val baseUrl = s"http://127.0.0.1:${server.getAddress.getPort}"
      val prompt = new OpenAIPrompt().setApiType("responses")
      val messages = prompt.createMessagesForRow(
        "Read.", Map("file" -> s"$baseUrl/files/invoice.docx"), Seq("file"))
      assert(messages(1).content(1) == Map(
        "type" -> "input_file",
        "filename" -> "invoice.docx",
        "file_data" -> s"data:$mimeType;base64,${Base64.getEncoder.encodeToString(bytes)}"
      ))
      val error = intercept[IllegalArgumentException] {
        prompt.createMessagesForRow("Read.", Map("file" -> s"$baseUrl/download"), Seq("file"))
      }
      assert(error.getMessage == "Unsupported file type: application/octet-stream.")
      assert(requests.toArray.toSeq == Seq("/files/invoice.docx", "/download"))
    } finally {
      server.stop(0)
    }
  }

  Seq(
    ("docm", "docx", "application/msword"),
    ("xlsm", "xlsx", "application/vnd.ms-excel"),
    ("pptm", "pptx", "application/vnd.ms-powerpoint"),
    ("dot", "doc", "application/msword"),
    ("zip", "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ("bin", "doc", "application/msword")
  ).foreach { case (extension, fixtureExtension, mimeType) =>
    test(s"Office MIME aliases do not admit .$extension paths") {
      val fixture = new File(getClass.getResource(s"/office/invoice.$fixtureExtension").toURI).toPath
      val tempFile = Files.createTempFile("synapseml-unsupported-office", s".$extension")
      val originalFileNameMap = URLConnection.getFileNameMap
      try {
        Files.write(tempFile, Files.readAllBytes(fixture))
        URLConnection.setFileNameMap(new FileNameMap {
          override def getContentTypeFor(fileName: String): String =
            if (fileName.endsWith(s".$extension")) mimeType else originalFileNameMap.getContentTypeFor(fileName)
        })
        val error = intercept[IllegalArgumentException] {
          new OpenAIPrompt().setApiType("responses")
            .createMessagesForRow("Read.", Map("file" -> tempFile.toString), Seq("file"))
        }
        assert(error.getMessage == s"Unsupported file type: $mimeType for extension '.$extension'.")
      } finally {
        URLConnection.setFileNameMap(originalFileNameMap)
        Files.deleteIfExists(tempFile)
      }
    }
  }

  Seq(
    ("xls", "application/vnd.ms-excel", "Invoice,Total\nINV-1042,246.90\n"),
    ("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "Invoice INV-1042")
  ).foreach { case (extension, mimeType, content) =>
    test(s"Office .$extension paths defer document-content validation to the provider") {
      val bytes = content.getBytes(StandardCharsets.UTF_8)
      val tempFile = Files.createTempFile("synapseml-office-content", s".$extension")
      try {
        Files.write(tempFile, bytes)
        val messages = new OpenAIPrompt().setApiType("responses")
          .createMessagesForRow("Read.", Map("file" -> tempFile.toString), Seq("file"))
        assert(messages(1).content(1) == Map(
          "type" -> "input_file",
          "filename" -> tempFile.getFileName.toString,
          "file_data" -> s"data:$mimeType;base64,${Base64.getEncoder.encodeToString(bytes)}"
        ))
      } finally {
        Files.deleteIfExists(tempFile)
      }
    }
  }

  Seq(
    "doc" -> "application/msword",
    "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls" -> "application/vnd.ms-excel",
    "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "ppt" -> "application/vnd.ms-powerpoint",
    "pptx" -> "application/vnd.openxmlformats-officedocument.presentationml.presentation"
  ).foreach { case (extension, mimeType) =>
    val containerMime = if (extension.endsWith("x")) "application/zip" else "application/x-ole-storage"
    Seq(containerMime, "application/octet-stream").foreach { genericMime =>
      test(s"Office .$extension paths override generic $genericMime name mappings") {
        val fixture = new File(getClass.getResource(s"/office/invoice.$extension").toURI).toPath
        val bytes = Files.readAllBytes(fixture)
        val originalFileNameMap = URLConnection.getFileNameMap
        try {
          URLConnection.setFileNameMap(new FileNameMap {
            override def getContentTypeFor(fileName: String): String =
              if (fileName.endsWith(s".$extension")) genericMime else originalFileNameMap.getContentTypeFor(fileName)
          })
          val messages = new OpenAIPrompt().setApiType("responses")
            .createMessagesForRow("Read.", Map("file" -> fixture.toString), Seq("file"))
          assert(messages(1).content(1) == Map(
            "type" -> "input_file",
            "filename" -> fixture.getFileName.toString,
            "file_data" -> s"data:$mimeType;base64,${Base64.getEncoder.encodeToString(bytes)}"
          ))
        } finally {
          URLConnection.setFileNameMap(originalFileNameMap)
        }
      }
    }
  }

  Seq(
    ("xls", "<table><tr><td>246.90</td></tr></table>", "application/xml"),
    ("doc", "{\\rtf1 invoice}", "application/json")
  ).foreach { case (extension, content, mimeType) =>
    test(s"Office-named .$extension text detected as $mimeType remains unsupported") {
      val tempFile = Files.createTempFile("synapseml-office-markup", s".$extension")
      try {
        Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8))
        val error = intercept[IllegalArgumentException] {
          new OpenAIPrompt().setApiType("responses")
            .createMessagesForRow("Read.", Map("file" -> tempFile.toString), Seq("file"))
        }
        assert(error.getMessage == s"Unsupported file type: $mimeType.")
      } finally {
        Files.deleteIfExists(tempFile)
      }
    }
  }
}
