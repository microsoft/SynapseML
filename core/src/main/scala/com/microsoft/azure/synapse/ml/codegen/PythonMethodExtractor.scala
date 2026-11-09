// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.text.Normalizer
import scala.annotation.tailrec

private[codegen] object PythonMethodExtractor {
  private val MethodPrefix = """^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("""
  // Alias validation retains full Unicode names; supported stub signatures remain ASCII-only.
  private val MethodStart = """^(?:async\s+)?def\s+([^\s(]+)\s*\(""".r
  val MethodPattern = (MethodPrefix + """([^)]*)\)(?:\s*->\s*([^:]+))?:\s*$""").r
  private val ScopePrefix = """^(?:(?:async\s+)?def|class)\s+""".r

  case class Method(name: String, signature: String, decorators: Seq[String], unconditional: Boolean)

  def pythonIdentifier(name: String): String = Normalizer.normalize(name, Normalizer.Form.NFKC)

  def effectiveMethods(source: String): Seq[Method] = {
    val methods = extract(source).zipWithIndex
    val lastUnconditional = methods.collect {
      case (method, index) if method.unconditional => pythonIdentifier(method.name) -> index
    }.toMap
    methods.collect {
      case (method, index) if lastUnconditional.get(pythonIdentifier(method.name)).forall(_ <= index) => method
    }
  }

  @tailrec
  private def bindingDecorator(expression: String): Option[String] = {
    if (Set("staticmethod", "classmethod").contains(expression)) {
      Some(s"@$expression")
    } else if (expression.startsWith("(") && expression.endsWith(")")) {
      bindingDecorator(expression.substring(1, expression.length - 1).trim)
    } else {
      None
    }
  }

  // Class statements and nested scope bodies can contain multiline expressions and strings.
  private class ExpressionState {
    private var quote = ""
    private var delimiterDepth = 0
    private var explicitContinuation = false

    def continues: Boolean = quote.nonEmpty || delimiterDepth > 0 || explicitContinuation

    def explicitlyContinues: Boolean = quote.isEmpty && explicitContinuation

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

  private def indentation(line: String): Int = {
    // Measure the generated class body: Python expands tabs and resets indentation on form feeds.
    GenerationUtils.indent(line, 1).takeWhile(" \t\f".contains(_)).foldLeft(0) {
      case (column, '\t') => (column / 8 + 1) * 8
      case (_, '\f') => 0
      case (column, _) => column + 1
    }
  }

  private def logicalLines(source: String): Seq[(String, Int)] = {
    val expression = new ExpressionState
    val prefix = new StringBuilder
    var initialIndent: Option[Int] = None
    source.split("\n").toSeq.flatMap { raw =>
      val line = raw.stripSuffix("\r")
      val indent = initialIndent.getOrElse(indentation(line))
      expression.scan(line)
      if (expression.explicitlyContinues) {
        // Generated wrappers indent every physical line, separating tokens across continuations.
        prefix.append(line.stripSuffix("\\")).append(" ")
        initialIndent = Some(indent)
        None
      } else {
        val statement = prefix.append(line).toString
        prefix.clear()
        initialIndent = None
        Some(statement -> indent)
      }
    }
  }

  def extract(source: String): Seq[Method] = {
    var nestedScopeIndent: Option[Int] = None
    val expression = new ExpressionState
    var decoratorIndent: Option[Int] = None
    var decorators = Vector.empty[String]
    // Explicit continuations keep the logical line's initial indentation, not the next physical line's.
    logicalLines(source).flatMap { logicalLine =>
      val (line, indent) = logicalLine
      val statement = line.trim
      if (expression.continues) {
        // A continuation can close at the declaration's indentation without ending its scope.
        val continuation = expression.scan(line)
        if (decoratorIndent.isDefined) {
          decorators = decorators.init :+ (decorators.last + "\n" + continuation)
        }
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
              decorators.flatMap { decorator =>
                bindingDecorator(decorator.stripPrefix("@").replace("\\\n", "").trim)
              }
            } else {
              Vector.empty
            }
            Method(matched.group(1), statement, bindingDecorators, indent == indentation(""))
          }
          decorators = Vector.empty
          decoratorIndent = None
          // Control blocks do not introduce a Python scope; functions and classes do.
          if (ScopePrefix.findFirstIn(statement).isDefined) {
            nestedScopeIndent = Some(indent)
          }
          expression.scan(line)
          method
        }
      }
    }
  }
}
