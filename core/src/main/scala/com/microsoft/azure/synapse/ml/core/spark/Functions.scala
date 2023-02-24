// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.spark

import org.apache.spark.sql.{Column, functions => F}

import scala.collection.mutable.ArrayBuffer

//scalastyle:off
class PEP3101Parser(val expr: String) {
  private var idx = 0

  private val tokens: ArrayBuffer[Column] = ArrayBuffer.empty

  private def consumeLiteral(): Unit = {
    var searchIdx = idx
    while(true) {
      val nextBracket = expr.indexOf('{', searchIdx)

      // hit the end?
      if (nextBracket == -1) {
        strToLit(expr.substring(idx, expr.length))

        idx = expr.length
        return
      }

      if (nextBracket + 1 == expr.length)
        throw new IllegalArgumentException(s"Unable to parse expression: $expr. Partial {")

      // found non-escaped variable?
      if (expr(nextBracket + 1) != '{') {
        strToLit(expr.substring(idx, nextBracket))

        idx = nextBracket
        return
      }

      // escaped bracket
      searchIdx = nextBracket + 2
    }
  }

  private def consumeVariable(): Unit = {
    val nextBracket = expr.indexOf('}', idx)

    // hit the end?
    if (nextBracket == -1)
      throw new IllegalArgumentException(s"Unable to parse expression: $expr. Missing closing }")

    // include closing bracket
    val token = expr.substring(idx, nextBracket + 1)

    if (token.length > 2)
      tokens.append(F.expr(token.substring(1, token.length - 1)))

    idx = nextBracket + 1
  }

  private def strToLit(s: String) = {
    if (s.length > 0) {
      tokens.append(F.lit(s
        .replaceAll("}}", "}")
        .replaceAll("\\{\\{", "{")))
    }
  }

  def parse: Seq[Column] = {
    idx = 0
    tokens.clear()

    var findLiteral = true
    while (idx < expr.length) {
      if (findLiteral)
        consumeLiteral()
      else
        consumeVariable()

      findLiteral = !findLiteral
    }

    tokens
  }
}

object Functions {
  def template(expr: String): Column =
    F.concat(new PEP3101Parser(expr).parse: _*)
}
