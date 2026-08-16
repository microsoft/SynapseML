// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

private[codegen] object PythonInitMerger {

  private val ByteOrderMark = 0xFEFF.toChar

  def preserve(conf: CodegenConfig): Unit = {
    val manualRoot = new File(new File(conf.pySrcOverrideDir, "synapse"), "ml")
    val generatedRoot = new File(new File(conf.pySrcDir, "synapse"), "ml")
    initFiles(manualRoot).foreach { manualFile =>
      val relativePath = manualRoot.toPath.relativize(manualFile.toPath)
      val generatedFile = generatedRoot.toPath.resolve(relativePath).toFile
      preserveFile(manualFile, generatedFile)
    }
  }

  private def initFiles(dir: File): Seq[File] = {
    if (!dir.isDirectory) {
      Seq.empty
    } else {
      Option(dir.listFiles()).getOrElse(Array.empty[File]).sortBy(_.getName).flatMap {
        case file if file.isDirectory => initFiles(file)
        case file if file.getName == "__init__.py" => Seq(file)
        case _ => Seq.empty
      }
    }
  }

  private def preserveFile(manualFile: File, generatedFile: File): Unit = {
    val manualContent = readUtf8(manualFile)
    val generatedContent = if (generatedFile.isFile) readUtf8(generatedFile) else ""
    val mergedContent =
      if (manualContent.isEmpty || generatedContent.isEmpty || manualContent == generatedContent) {
        if (generatedContent.isEmpty) manualContent else generatedContent
      } else {
        val (prologue, body) = splitPrologue(manualContent)
        join(prologue, generatedContent, body)
      }

    if (mergedContent.nonEmpty || generatedFile.isFile) {
      generatedFile.getParentFile.mkdirs()
      Files.write(generatedFile.toPath, mergedContent.getBytes(StandardCharsets.UTF_8))
      ()
    }
  }

  private def readUtf8(file: File): String =
    new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)

  private def join(parts: String*): String =
    parts.filter(_.nonEmpty).foldLeft("") { (result, part) =>
      if (result.isEmpty || result.endsWith("\n") || result.endsWith("\r") ||
        part.startsWith("\n") || part.startsWith("\r")) {
        result + part
      } else {
        result + "\n" + part
      }
    }

  // Explicit indices keep this small scanner deterministic without regex backtracking or a parser dependency.
  //scalastyle:off
  private[codegen] def splitPrologue(content: String): (String, String) = {
    val start = if (content.headOption.contains(ByteOrderMark)) 1 else 0
    var prologueEnd = skipTrivia(content, start)

    consumeModuleDocstring(content, prologueEnd).foreach { end =>
      prologueEnd = end
    }

    var continue = true
    while (continue) {
      val statementStart = skipTrivia(content, prologueEnd)
      val statementEnd = logicalStatementEnd(content, statementStart)
      if (statementStart < content.length && isFutureImport(content, statementStart, statementEnd)) {
        prologueEnd = statementEnd
      } else {
        continue = false
      }
    }

    (content.substring(0, prologueEnd), content.substring(prologueEnd))
  }

  private def skipTrivia(content: String, start: Int): Int = {
    var index = start
    while (index < content.length) {
      content.charAt(index) match {
        case char if char.isWhitespace => index += 1
        case '#' =>
          index = lineEnd(content, index)
        case _ => return index
      }
    }
    index
  }

  private def lineEnd(content: String, start: Int): Int = {
    var index = start
    while (index < content.length && content.charAt(index) != '\n' && content.charAt(index) != '\r') {
      index += 1
    }
    if (index < content.length && content.charAt(index) == '\r') index += 1
    if (index < content.length && content.charAt(index) == '\n') index += 1
    index
  }

  private def consumeModuleDocstring(content: String, start: Int): Option[Int] = {
    val quoteStart = stringQuoteStart(content, start)
    quoteStart.flatMap { index =>
      val quote = content.charAt(index)
      val triple = index + 2 < content.length &&
        content.charAt(index + 1) == quote && content.charAt(index + 2) == quote
      val literalEnd = stringLiteralEnd(content, index, quote, triple)
      literalEnd.flatMap { end =>
        var suffix = end
        while (suffix < content.length && " \t\f".contains(content.charAt(suffix))) suffix += 1
        if (suffix < content.length && content.charAt(suffix) == '#') {
          Some(lineEnd(content, suffix))
        } else if (suffix == content.length) Some(suffix)
        else if (content.charAt(suffix) == '\n' || content.charAt(suffix) == '\r') {
          Some(lineEnd(content, suffix))
        } else {
          None
        }
      }
    }
  }

  private def stringQuoteStart(content: String, start: Int): Option[Int] = {
    var index = start
    while (index < content.length && "rRuU".contains(content.charAt(index)) && index - start < 2) {
      index += 1
    }
    val prefix = content.substring(start, index).toLowerCase
    val validPrefix = Set("", "r", "u", "ru", "ur").contains(prefix)
    if (validPrefix && index < content.length && (content.charAt(index) == '\'' || content.charAt(index) == '"')) {
      Some(index)
    } else {
      None
    }
  }

  private def stringLiteralEnd(content: String,
                               quoteStart: Int,
                               quote: Char,
                               triple: Boolean): Option[Int] = {
    var index = quoteStart + (if (triple) 3 else 1)
    while (index < content.length) {
      if (content.charAt(index) == '\\') {
        index += 2
      } else if (triple && index + 2 < content.length &&
        content.charAt(index) == quote &&
        content.charAt(index + 1) == quote &&
        content.charAt(index + 2) == quote) {
        return Some(index + 3)
      } else if (!triple && content.charAt(index) == quote) {
        return Some(index + 1)
      } else if (!triple && (content.charAt(index) == '\n' || content.charAt(index) == '\r')) {
        return None
      } else {
        index += 1
      }
    }
    None
  }

  private def logicalStatementEnd(content: String, start: Int): Int = {
    var index = start
    var depth = 0
    while (index < content.length) {
      content.charAt(index) match {
        case '\\' if index + 1 < content.length &&
          (content.charAt(index + 1) == '\n' || content.charAt(index + 1) == '\r') =>
          index = lineEnd(content, index + 1)
        case '#' =>
          val end = lineEnd(content, index)
          if (depth == 0) return end
          index = end
        case quote if quote == '\'' || quote == '"' =>
          val triple = index + 2 < content.length &&
            content.charAt(index + 1) == quote && content.charAt(index + 2) == quote
          index = stringLiteralEnd(content, index, quote, triple).getOrElse(content.length)
        case '(' | '[' | '{' =>
          depth += 1
          index += 1
        case ')' | ']' | '}' =>
          depth = math.max(0, depth - 1)
          index += 1
        case '\n' | '\r' if depth == 0 =>
          return lineEnd(content, index)
        case _ =>
          index += 1
      }
    }
    index
  }

  private def isFutureImport(content: String, start: Int, end: Int): Boolean = {
    val (from, afterFrom) = nextIdentifier(content, start, end)
    val (future, afterFuture) = nextIdentifier(content, afterFrom, end)
    val (importKeyword, _) = nextIdentifier(content, afterFuture, end)
    from == "from" && future == "__future__" && importKeyword == "import"
  }

  private def nextIdentifier(content: String, start: Int, end: Int): (String, Int) = {
    var index = start
    var searching = true
    while (index < end && searching) {
      content.charAt(index) match {
        case char if char.isWhitespace => index += 1
        case '\\' if index + 1 < end &&
          (content.charAt(index + 1) == '\n' || content.charAt(index + 1) == '\r') =>
          index = lineEnd(content, index + 1)
        case '#' => index = lineEnd(content, index)
        case _ => searching = false
      }
    }
    val identifierStart = index
    while (index < end && (content.charAt(index).isLetterOrDigit || content.charAt(index) == '_')) {
      index += 1
    }
    (content.substring(identifierStart, index), index)
  }
  //scalastyle:on
}
