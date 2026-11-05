// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import scala.annotation.tailrec

private[codegen] object PythonMethodExtractor {
  private val MethodPrefix = """^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("""
  private val MethodStart = MethodPrefix.r
  val MethodPattern = (MethodPrefix + """([^)]*)\)(?:\s*->\s*([^:]+))?:\s*$""").r
  private val ScopePrefix = """^(?:(?:async\s+)?def|class)\s+""".r

  case class Method(name: String, signature: String, decorators: Seq[String])

  // Headers, decorators, and skipped scope bodies can contain multiline expressions and strings.
  private class ExpressionState {
    private var quote = ""
    private var delimiterDepth = 0
    private var explicitContinuation = false

    def continues: Boolean = quote.nonEmpty || delimiterDepth > 0 || explicitContinuation

    private def afterStringCharacter(line: String, index: Int): Int = {
      if (line.charAt(index) == '\\') {
        index + 2
      } else if (line.startsWith(quote, index)) {
        val nextIndex = index + quote.length
        quote = ""
        nextIndex
      } else {
        index + 1
      }
    }

    @tailrec
    private def appendCode(line: String, index: Int, code: StringBuilder): Unit = {
      if (index < line.length) {
        val char = line.charAt(index)
        val nextIndex = if (quote.nonEmpty) {
          afterStringCharacter(line, index)
        } else if (char == '#') {
          line.length
        } else if (char == '\'' || char == '"') {
          val tripleQuote = char.toString * 3
          quote = if (line.startsWith(tripleQuote, index)) tripleQuote else char.toString
          index + quote.length
        } else {
          if ("([{".contains(char)) delimiterDepth += 1
          else if (")]}".contains(char)) delimiterDepth -= 1
          code.append(char)
          index + 1
        }
        appendCode(line, nextIndex, code)
      }
    }

    def scan(line: String): String = {
      val code = new StringBuilder
      appendCode(line, 0, code)
      val statement = code.toString.trim
      explicitContinuation = statement.endsWith("\\")
      statement
    }
  }

  def extract(source: String): Seq[Method] = {
    var nestedScopeIndent: Option[Int] = None
    val expression = new ExpressionState
    var decoratorIndent: Option[Int] = None
    var decorators = Vector.empty[String]
    source.split("\n").toSeq.flatMap { line =>
      val statement = line.trim
      val indent = line.prefixLength(_.isWhitespace)
      if (expression.continues) {
        // A continuation can close at the declaration's indentation without ending its scope.
        expression.scan(line)
        None
      } else if (statement.isEmpty || statement.startsWith("#")) {
        None
      } else {
        nestedScopeIndent = nestedScopeIndent.filter(indent > _)
        if (nestedScopeIndent.isDefined) {
          expression.scan(line)
          None
        } else if (statement.startsWith("@")) {
          if (!decoratorIndent.contains(indent)) decorators = Vector.empty
          decoratorIndent = Some(indent)
          decorators :+= expression.scan(line)
          None
        } else {
          val method = MethodStart.findFirstMatchIn(statement).map { matched =>
            val bindingDecorators = if (decoratorIndent.contains(indent)) {
              decorators.filter(Set("@staticmethod", "@classmethod"))
            } else {
              Vector.empty
            }
            Method(matched.group(1), statement, bindingDecorators)
          }
          decorators = Vector.empty
          decoratorIndent = None
          // Control blocks do not introduce a Python scope; functions and classes do.
          if (ScopePrefix.findFirstIn(statement).isDefined) {
            nestedScopeIndent = Some(indent)
            expression.scan(line)
          }
          method
        }
      }
    }
  }
}
