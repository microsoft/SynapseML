// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.annotation.tailrec
import scala.sys.process.Process
import scala.util.control.NonFatal
import spray.json.{JsString, JsonParser}

private[codegen] object PythonMethodExtractor {
  private val MethodPrefix = """^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("""
  // Alias validation retains full Unicode names; supported stub signatures remain ASCII-only.
  private val MethodStart = """^(?:async\s+)?def\s+([^\s(]+)\s*\(""".r
  val MethodPattern = (MethodPrefix + """([^)]*)\)(?:\s*->\s*([^:]+))?:\s*$""").r
  private val ScopePrefix = """^(?:(?:async\s+)?def|class)\s+""".r

  private val StringToken = Seq("'''", "\"\"\"", "'", "\"").map { quote =>
    val delimiter = java.util.regex.Pattern.quote(quote)
    s"$delimiter(?:\\\\.|(?!$delimiter)[^\\\\])*$delimiter"
  }.mkString("|")
  private val NumberToken =
    """(?:0[xX][0-9a-fA-F_]+|0[bB][01_]+|0[oO][0-7_]+|""" +
      """(?:[0-9][0-9_]*(?:\.[0-9_]*)?|\.[0-9][0-9_]*)(?:[eE][+-]?[0-9][0-9_]*)?[jJ]?)"""
  // Keep non-ASCII identifier runs intact even when the JDK's Unicode tables differ from Python's.
  private val IdentifierToken = """(?:[A-Za-z_]|[^\x00-\x7f])(?:[A-Za-z_0-9]|[^\x00-\x7f])*"""
  private val DeclarationToken = (
    s"(?s)$StringToken|$NumberToken|$IdentifierToken|" +
      """\*\*=|//=|<<=|>>=|\.\.\.|->|:=|==|!=|<=|>=|<<|>>|\*\*|//|[-+*/%&|^]=|\S"""
    ).r

  def declarationTokens(declaration: String): Seq[String] =
    DeclarationToken.findAllIn(declaration).toVector

  def splitArgumentDefault(argument: String): Seq[String] = {
    var delimiterDepth = 0
    val separator = DeclarationToken.findAllMatchIn(argument).find { token =>
      token.matched match {
        case "(" | "[" | "{" =>
          delimiterDepth += 1
          false
        case ")" | "]" | "}" =>
          delimiterDepth -= 1
          false
        case "=" => delimiterDepth == 0 && !hasUnfinishedLambda(argument.substring(0, token.start))
        case _ => false
      }
    }
    separator.map(token => Seq(argument.substring(0, token.start).trim, argument.substring(token.end).trim))
      .getOrElse(Seq(argument.trim))
  }

  case class Method(name: String, signature: String, decorators: Seq[String], unconditional: Boolean)

  // Python's Unicode tables can be newer than the JDK's. ASCII generation needs no interpreter.
  def pythonIdentifier(name: String): String = {
    if (name.forall(_ <= '\u007f')) {
      name
    } else {
      val executable = if (System.getProperty("os.name").startsWith("Windows")) "python" else "python3"
      // Process arguments use the platform charset; transport Unicode as ASCII instead.
      val encoded = Base64.getEncoder.encodeToString(name.getBytes(StandardCharsets.UTF_8))
      val script = "import base64, json, sys, unicodedata; " +
        "name = base64.b64decode(sys.argv[1]).decode('utf-8')\n" +
        "if not name.isidentifier():\n" +
        "    raise ValueError('Identifier is invalid in Unicode ' + unicodedata.unidata_version)\n" +
        "print(json.dumps(unicodedata.normalize('NFKC', name), ensure_ascii=True))"
      try {
        JsonParser(Process(Seq(executable, "-I", "-c", script, encoded)).!!) match {
          case JsString(identifier) => identifier
          case _ => throw new IllegalStateException("Python identifier normalization did not return a JSON string")
        }
      } catch {
        case NonFatal(error) =>
          throw new IllegalStateException(
            s"Python identifier normalization failed for '$name' using $executable", error)
      }
    }
  }

  // Track def bindings only, not arbitrary assignment, import, class or del rebinding.
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

  private def hasUnfinishedLambda(argument: String): Boolean = {
    var delimiterDepth = 0
    var lambdaHeaders = 0
    declarationTokens(new ExpressionState().scan(argument)).foreach {
      case "(" | "[" | "{" => delimiterDepth += 1
      case ")" | "]" | "}" => delimiterDepth -= 1
      case "lambda" if delimiterDepth == 0 => lambdaHeaders += 1
      case ":" if delimiterDepth == 0 && lambdaHeaders > 0 => lambdaHeaders -= 1
      case _ =>
    }
    lambdaHeaders > 0
  }

  def splitArguments(args: String): Option[Seq[String]] = {
    val expression = new ExpressionState
    val current = new StringBuilder
    val arguments = Vector.newBuilder[String]
    args.split(",", -1).foreach { part =>
      if (current.nonEmpty) current.append(",")
      current.append(part)
      expression.scan(part)
      if (!expression.continues) {
        val argument = current.toString.trim
        if (argument.nonEmpty) arguments += argument
        current.clear()
      }
    }
    val result = arguments.result()
    // A top-level lambda parameter comma must not be mistaken for a signature argument separator.
    if (expression.continues || result.exists(hasUnfinishedLambda)) None else Some(result)
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
