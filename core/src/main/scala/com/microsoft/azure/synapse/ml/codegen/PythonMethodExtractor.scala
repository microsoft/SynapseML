// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

private[codegen] object PythonMethodExtractor {
  private val MethodPrefix = """^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("""
  private val MethodStart = MethodPrefix.r
  val MethodPattern = (MethodPrefix + """([^)]*)\)(?:\s*->\s*([^:]+))?:\s*$""").r
  private val ScopePrefix = """^(?:(?:async\s+)?def|class)\s+""".r
  private val HeaderStringOrComment = """'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|#.*""".r

  case class Method(name: String, signature: String, decorators: Seq[String])

  private def headerDelimiterDelta(line: String): Int = {
    val code = HeaderStringOrComment.replaceAllIn(line, "")
    code.count(c => c == '(' || c == '[' || c == '{') -
      code.count(c => c == ')' || c == ']' || c == '}')
  }

  def extract(source: String): Seq[Method] = {
    var nestedScopeIndent: Option[Int] = None
    var headerDepth = 0
    var decoratorIndent: Option[Int] = None
    var decorators = Vector.empty[String]
    source.split("\n").toSeq.flatMap { line =>
      val statement = line.trim
      val indent = line.prefixLength(_.isWhitespace)
      if (statement.isEmpty || statement.startsWith("#")) {
        None
      } else if (headerDepth > 0) {
        // A multiline header can close at the declaration's indentation without ending its scope.
        headerDepth += headerDelimiterDelta(statement)
        None
      } else {
        nestedScopeIndent = nestedScopeIndent.filter(indent > _)
        if (nestedScopeIndent.isDefined) {
          None
        } else if (statement.startsWith("@")) {
          if (!decoratorIndent.contains(indent)) decorators = Vector.empty
          decoratorIndent = Some(indent)
          decorators :+= statement
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
            headerDepth = headerDelimiterDelta(statement)
          }
          method
        }
      }
    }
  }
}
