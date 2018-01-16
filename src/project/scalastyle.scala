// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import sbt._

final object ScalaStyleExtras {

  // First string determines options:
  //  * [st]?: optional src- or test-only rule (default: both)
  //  * [FS]:  whether this is a .scalariform or a .file rule
  //  * [EWI]: Error / Warn / Ignore
  val rules = List(
    r("FE", "FileLengthChecker", ("maxFileLength", 800)),
    r("FE", "FileTabChecker"),
    r("FE", "FileLineLengthChecker", ("maxLineLength", 120)),
    r("FE", "NewLineAtEofChecker"),
    r("FE", "RegexChecker", ("regex", "\n\n\n")),
    r("SW", "TokenChecker", ("regex", ".{33}")),
    r("FE", "HeaderMatchesChecker",
      ("header",
       Seq("^// Copyright \\(C\\) Microsoft Corporation\\. All rights reserved\\.",
           "// Licensed under the MIT License\\. See LICENSE in project root for information\\.",
           "",
           "package (?:com\\.microsoft\\.ml\\.spark|org\\.apache\\.spark" +
             "|com\\.microsoft\\.CNTK|com\\.microsoft\\.ml\\.lightgbm)[.\n]")
         .mkString("\n")),
      ("regex", true)),
    r("FI", "IndentationChecker", ("tabSize", 2), ("methodParamIndentSize", 2)),
    r("FE", "WhitespaceEndOfLineChecker"),
    r("SE", "SpacesAfterPlusChecker"),
    r("SE", "SpacesBeforePlusChecker"),
    r("SE", "NoWhitespaceBeforeLeftBracketChecker"),
    r("SE", "NoWhitespaceAfterLeftBracketChecker"),
    r("SE", "EmptyClassChecker"),
    r("SW", "EnsureSingleSpaceAfterTokenChecker", ("tokens", "COLON")), // IF, FOR, WHILE, ELSE -- one or more
    r("SE", "EnsureSingleSpaceBeforeTokenChecker"), // what is this doing?
    r("SW", "DisallowSpaceAfterTokenChecker"),
    r("SE", "DisallowSpaceBeforeTokenChecker"),
    r("SE", "ClassNamesChecker", ("regex", "^[A-Z][A-Za-z0-9]*$")),
    r("SE", "ObjectNamesChecker", ("regex", "^[A-Za-z][A-Za-z0-9]*$")), // allow function-like names
    r("SE", "PackageObjectNamesChecker", ("regex", "^[a-z][A-Za-z]*$")),
    // this matches the first token after a `val`, which might be `(` in case of val (x, y) = ...
    r("SW", "FieldNamesChecker", ("regex", "^([a-z][A-Za-z0-9]*| *\\( *)$")),
    r("SW", "MethodNamesChecker", ("regex", "^[a-z][A-Za-z0-9]*(_=)?$")),
    r("SW", "ClassTypeParameterChecker", ("regex", "^[A-Z_]$")),
    r("SE", "EqualsHashCodeChecker"),
    r("SE", "IllegalImportsChecker", ("illegalImports", "sun._")),
    r("SE", "DeprecatedJavaChecker"),
    r("SE", "ParameterNumberChecker", ("maxParameters", 9)),
    r("SW", "MethodLengthChecker", ("maxLength", 50)),
    r("SE", "NumberOfTypesChecker", ("maxTypes", 30)),
    r("SE", "NumberOfMethodsInTypeChecker", ("maxMethods", 30)),
    r("SE", "NumberOfTypesChecker"),
    r("SW", "CyclomaticComplexityChecker", ("maximum", 10)),
    r("SE", "PublicMethodsHaveTypeChecker"),
    r("sSW", "MagicNumberChecker", ("ignore", "-1,0,1,2,3")),
    r("SE", "UppercaseLChecker"),
    r("SE", "ProcedureDeclarationChecker"),
    r("SE", "RedundantIfChecker"),
    r("SW", "WhileChecker"),
    r("SW", "ReturnChecker"),
    r("SW", "NullChecker"),
    r("SE", "NoCloneChecker"),
    r("SE", "NoFinalizeChecker"),
    r("SE", "StructuralTypeChecker"),
    r("SE", "CovariantEqualsChecker"),
    r("SE", "NonASCIICharacterChecker"),
    // looks like this doesn't work
    r("SE", "ImportOrderChecker", ("groups" , "our,scala,java,other"),
      ("our", "com.microsoft.ml.spark[.].+"), ("scala", "scala[.].+"), ("java", "java[.].+"), ("other", ".+")),
    r("SE", "SimplifyBooleanExpressionChecker"),
    r("SW", "NotImplementedErrorUsage")
    // r("SE", "ScalaDocChecker") <-- use when we add scaladoc
    // Rules that are not used:
    //   VarLocalChecker:         needed in some places
    //   VarFieldChecker:         -"-
    //   BlockImportChecker:      we want to be able to name specific imports...
    //   ImportGroupingChecker:   ... and be able to import in the middle of code
    //   UnderscoreImportChecker: ... and use _ wildcards
    //   NoNewLineAtEofChecker:   want a newline there
    //   ForBraceChecker:         "for {...} yield ..." looks fine, but "for { ... } { ... }" looks messy
    //   XmlLiteralChecker:       maybe it'll be useful
    //   LowercasePatternMatchChecker:  Lots of places where it's fine
    //   MultipleStringLiteralsChecker: applies even in interpolation parts
    //   PatternMatchAlignChecker:      Looks like it's wrong anyway
    //   SpaceAfterCommentStartChecker: rejects the popular "//TODO:"
    //   TodoCommentChecker:            at least for now we need them
    )

  val conf = file(".") / "scalastyle-config.xml"

  def modes    = Map(' ' -> null, 's' -> "src", 't' -> "test")
  def prefixes = Map('F' -> "file", 'S' -> "scalariform")
  def levels   = Map('E' -> "error", 'W' -> "warning", 'I' -> null)
  def r(flags: String, name: String, params: (String, Any)*) = {
    val f3 = if (flags.length < 3) " "+flags else flags
    (modes(f3(0)), s"org.scalastyle.${prefixes(f3(1))}.${name}", levels(f3(2)), params)
  }

  def mkRule(curmode: String)(rule: (String, String, String, Seq[(String,Any)])): String = {
    val (mode, name, level, params) = rule
    if (level == null || (mode != null && curmode != mode)) return null
    val paramStr =
      if (params.isEmpty) ""
      else ("<parameters>"
            + params.map(p => "\n    <parameter name=\"" + p._1 + "\"><![CDATA["
                              + p._2.toString + "]]></parameter>")
              .mkString("")
            + "</parameters>")
    s"""<check level="$level" class="$name" enabled="true">$paramStr</check>"""
  }
  def mkConfig(mode: String): String =
    s"""<scalastyle>
       |  <name>Scalastyle Module Configuration ($mode)</name>
       |  ${rules.map(mkRule(mode)).filter(_ != null).mkString("\n  ")}
       |</scalastyle>
       |""".stripMargin

  def commands = Seq(
    Command.command("run-scalastyle") { st =>
      Extras.addCommands(st, "run-scalastyle-on src", "run-scalastyle-on test")
    },
    Command.single("scalastyle-make-config") { (st, mode) =>
      scala.tools.nsc.io.File(conf).writeAll(mkConfig(mode))
      st
    },
    Command.single("run-scalastyle-on") { (st, mode) =>
      val cmd = (if (mode == "src") "" else mode + ":") + "scalastyle"
      Extras.addCommands(st, s"scalastyle-make-config $mode",
                             s"noisy-command on-all-subs $cmd",
                             "scalastyle-delete-config")
    },
    Command.command("scalastyle-delete-config") { st => conf.delete; st }
  )

}
