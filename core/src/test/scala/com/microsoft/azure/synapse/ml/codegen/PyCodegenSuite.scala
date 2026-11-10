// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.zip.ZipFile

class PyCodegenSuite extends AnyFunSuite {
  import PyCodegenFixtures._

  private def pythonExecutable: String =
    if (System.getProperty("os.name").toLowerCase.contains("windows")) "python" else "python3"

  private def withTempDir(testCode: File => Unit): Unit = {
    val dir = Files.createTempDirectory("py-codegen-suite").toFile
    try testCode(dir) finally FileUtils.deleteDirectory(dir)
  }

  private def codegenConfig(root: File): CodegenConfig = CodegenConfig(
    "test-module", None, root.getAbsolutePath, new File(root, "target").getAbsolutePath,
    "1.0.0", "1.0.0", "1.0.0", "synapseml-test")

  private def packageDir(base: File, packageFolder: String): File = {
    val namespaceRoot = new File(new File(base, "synapse"), "ml")
    packageFolder.split("/").filter(_.nonEmpty).foldLeft(namespaceRoot)(new File(_, _))
  }

  private def initFile(base: File, packageFolder: String): File =
    new File(packageDir(base, packageFolder), "__init__.py")

  private def initStubFile(base: File, packageFolder: String): File =
    new File(packageDir(base, packageFolder), "__init__.pyi")

  private def writeUtf8(file: File, content: String): Unit = {
    file.getParentFile.mkdirs()
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    ()
  }

  private def addManualInit(conf: CodegenConfig, packageFolder: String, content: String): Unit = {
    writeUtf8(initFile(conf.pySrcOverrideDir, packageFolder), content)
    writeUtf8(initFile(conf.pySrcDir, packageFolder), content)
  }

  private def addModule(conf: CodegenConfig, packageFolder: String, name: String): Unit =
    writeUtf8(new File(packageDir(conf.pySrcDir, packageFolder), name), "")

  private def ensurePackage(conf: CodegenConfig, packageFolder: String): Unit = {
    packageDir(conf.pySrcDir, packageFolder).mkdirs()
    ()
  }

  private def readUtf8(file: File): String =
    new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)

  private def occurrences(text: String, value: String): Int =
    text.sliding(value.length).count(_ == value)

  private def buildWheel(sourceDir: File, wheelDir: File): File = {
    wheelDir.mkdirs()
    val process = new ProcessBuilder(
      pythonExecutable, "setup.py", "bdist_wheel", "--universal", "-d", wheelDir.getAbsolutePath)
      .directory(sourceDir)
      .redirectErrorStream(true)
      .start()
    val outputStream = process.getInputStream
    val output = try {
      new String(IOUtils.toByteArray(outputStream), StandardCharsets.UTF_8)
    } finally {
      outputStream.close()
    }
    assert(process.waitFor() === 0, output)
    val wheels = Option(wheelDir.listFiles()).getOrElse(Array.empty)
      .filter(file => file.isFile && file.getName.endsWith(".whl"))
    assert(wheels.length === 1, s"Expected one wheel, found: ${wheels.mkString(", ")}")
    wheels.head
  }

  private def assertPythonCompiles(file: File): Unit = {
    val script = "from pathlib import Path; import sys; " +
      "path = Path(sys.argv[1]); compile(path.read_bytes(), str(path), 'exec')"
    val process = new ProcessBuilder(pythonExecutable, "-c", script, file.getAbsolutePath)
      .redirectErrorStream(true)
      .start()
    val stream = process.getInputStream
    val output = try {
      new String(IOUtils.toByteArray(stream), StandardCharsets.UTF_8)
    } finally {
      stream.close()
    }
    assert(process.waitFor() === 0, output)
  }

  private def assertAliasConflict(root: File, stage: TypedPythonStage, methodName: String = "getModelCol"): Unit = {
    val sourceFile = new File(root, "fixture.py")
    writeUtf8(sourceFile, "class Fixture:\n" +
      stage.pyAdditionalMethods.split("\n").map("    " + _).mkString("\n") + "\n")
    assertPythonCompiles(sourceFile)
    val error = intercept[IllegalArgumentException] {
      stage.makePyFile(codegenConfig(root))
    }
    assert(error.getMessage.contains(
      s"Generated Python method $methodName conflicts with a hand-written method"))
  }

  private def wheelEntryContent(wheel: File, path: String): Option[String] = {
    val archive = new ZipFile(wheel)
    try {
      Option(archive.getEntry(path)).map { entry =>
        val stream = archive.getInputStream(entry)
        try new String(IOUtils.toByteArray(stream), StandardCharsets.UTF_8) finally stream.close()
      }
    } finally {
      archive.close()
    }
  }

  private def repositoryRoot(candidate: File): File = {
    if (new File(candidate, "build.sbt").isFile && new File(candidate, "core").isDirectory) candidate
    else Option(candidate.getParentFile).map(repositoryRoot)
      .getOrElse(fail(s"Could not find repository root from ${System.getProperty("user.dir")}"))
  }

  private def aggregateBuildFile: File =
    new File(repositoryRoot(new File(System.getProperty("user.dir"))), "build.sbt")

  private def aggregatePackageDiscovery(): String = {
    val lines = readUtf8(aggregateBuildFile).split("\n")
      .filter(_.contains("|    packages=find_namespace_packages("))
    assert(lines.length === 1)
    lines.head.trim.stripPrefix("|    packages=").stripSuffix(",")
  }

  private def aggregatePackageData(): String = {
    val content = readUtf8(aggregateBuildFile)
    val pattern = """(?s)\|\s+package_data=(\{.*?\}),\n\s+\|\)""".r
    pattern.findFirstMatchIn(content).map(_.group(1).replaceAllLiterally("|", ""))
      .getOrElse(fail("Could not find aggregate package_data in build.sbt"))
  }

  test("stub helpers do not add abstract state to the public PythonWrappable trait") {
    val abstractMethods = classOf[PythonWrappable].getDeclaredMethods
      .filter(method => Modifier.isAbstract(method.getModifiers))
    assert(abstractMethods.isEmpty, abstractMethods.map(_.getName).mkString(", "))
  }

  test("generated wrappers include typed constructor, param, and method stubs") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val stage = new TypedPythonStage

      stage.makePyFile(conf)

      val folder = "/codegen"
      val runtimeFile = new File(packageDir(conf.pySrcDir, folder), "TypedPythonStage.py")
      val stubFile = new File(packageDir(conf.pySrcDir, folder), "TypedPythonStage.pyi")
      val runtime = readUtf8(runtimeFile)
      val stub = readUtf8(stubFile)

      assert(runtimeFile.isFile)
      assertPythonCompiles(stubFile)
      assert(runtime.contains("""_service_param_names = frozenset(["model"])"""))
      assert(stub.contains("class TypedPythonStage("))
      assert(stub.contains("count: Param"))
      assert(stub.contains("count: Optional[float] = ..."))
      assert(stub.contains("labels: Optional[List[str]] = ..."))
      assert(stub.contains("model: Optional[str] = ..."))
      assert(stub.contains("modelCol: Optional[str] = ..."))
      assert(stub.contains("def getModel(self) -> str: ..."))
      assert(stub.contains("def getModelCol(self) -> str: ..."))
      assert(stub.contains("""_T = TypeVar("_T", bound="TypedPythonStage")"""))
      assert(stub.contains("def setCount(self: _T, value: float) -> _T: ..."))
      assert(stub.contains("def getText(self) -> str: ..."))
      assert(stub.contains("def setAlias(self: _T, *, value: str) -> _T: ..."))
      assert(stub.contains("def setPositionalAlias(self: _T, value: str, /) -> _T: ..."))
      assert(stub.contains("def clear(self, param: Param) -> None: ..."))
      assert(stub.contains(
        "def copy(self: _T, extra: Optional[ParamMap] = ...) -> _T: ..."))
      assert(runtime.contains("self._java_obj.setScalarParam(\"model\", value)"))
      assert(runtime.contains("self._java_obj.setVectorParam(\"model\", value)"))
      assert(runtime.contains("value = self._service_param_value_to_java(value)"))
      assert(runtime.contains(
        """self._java_obj.setScalarParam("model", value)
          |        self._paramMap.pop(self.model, None)""".stripMargin))
      assert(runtime.contains(
        """self._java_obj.setVectorParam("model", value)
          |        self._paramMap.pop(self.model, None)""".stripMargin))
      assert(runtime.contains(
        "self._service_param_scalar_to_python(\"model\", self._java_obj.getModel())"))
      assert(runtime.contains("return self._java_obj.getVectorParam(\"model\")"))
      assert(runtime.contains("return self._set_params_via_setters(kwargs)"))
      assertPythonCompiles(runtimeFile)
    }
  }

  test("generated runtime wrappers and stubs tolerate foreign-owned parameters") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val stage = new ForeignParamPythonStage
      intercept[IllegalArgumentException](stage.getDefault(stage.text))

      stage.makePyFile(conf)

      val folder = packageDir(conf.pySrcDir, "/codegen")
      val runtimeFile = new File(folder, "ForeignParamPythonStage.py")
      val stubFile = new File(folder, "ForeignParamPythonStage.pyi")
      assert(readUtf8(runtimeFile).contains("text=None"))
      assert(readUtf8(runtimeFile).contains("""_service_param_names = frozenset(["model"])"""))
      assert(readUtf8(stubFile).contains("text: Optional[str] = ..."))
      assertPythonCompiles(runtimeFile)
      assertPythonCompiles(stubFile)
    }
  }

  test("generated additional method stubs retain final bindings across decorators and overrides") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      new TypedPythonStage().makePyFile(conf)
      val stubFile = new File(packageDir(conf.pySrcDir, "/codegen"), "TypedPythonStage.pyi")
      val stub = readUtf8(stubFile)
      assert(stub.contains("@staticmethod\n    def loadOutput(spark: Any, outputPath: Any) -> Any: ..."))
      assert(stub.contains("@classmethod\n    def fromPath(cls, path: Any) -> Any: ..."))
      assert(!stub.contains("@classmethod\n    def describe") && !stub.contains("@staticmethod\n    def describe"))
      assert(stub.contains("def describe(self) -> Any: ..."))
      Seq("loadOutput", "fromPath", "describe").foreach(name => assert(occurrences(stub, s"def $name(") == 1))
      Seq("check_keys", "_identity").foreach(name => assert(!stub.contains(s"def $name(")))
      assert(occurrences(stub, "def getModelCol(") == 1)
      assertPythonCompiles(stubFile)
    }
  }
  test("class-body control blocks expose methods but not nested function or class members") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      new ConditionalPythonStage().makePyFile(conf)
      val stubFile = new File(packageDir(conf.pySrcDir, "/codegen"), "ConditionalPythonStage.pyi")
      val stub = readUtf8(stubFile)
      assert(stub.contains("@staticmethod\n    def conditionalValue(value: Any) -> Any: ..."))
      Seq("tryValue(self)", "fallbackValue(self)", "certain(self)", "formatted(self, value: Any)")
        .foreach(signature => assert(stub.contains(s"def $signature -> Any: ...")))
      assert(stub.contains("""def annotated(self, value: list[str], quoted: "left right") -> dict[str,int]: ..."""))
      Seq("hiddenConditional", "nestedOnly", "relabel", "refresh", "maybe",
        "conditionalAsync", "bindingChange", "literalChange", "equalsChange", "keywordChange", "identifierBoundary")
        .foreach(n => assert(!stub.contains(s"def $n(")))
      Seq("getModelCol", "certain", "formatted", "annotated").foreach(n => assert(occurrences(stub, s"def $n(") == 1))
      assert(PythonMethodExtractor.declarationTokens("1.e1") != PythonMethodExtractor.declarationTokens("1 .e1"))
      assertPythonCompiles(new File(stubFile.getParentFile, "ConditionalPythonStage.py"))
      assertPythonCompiles(stubFile)
    }
  }
  Seq(new MultilineFunctionPythonStage, new MultilineClassPythonStage,
    new ContinuedScopePythonStage, new ContinuedScopePythonStage(tight = true), new ContinuedDefaultPythonStage
  ).zipWithIndex.foreach { case (stage, i) =>
    val className = stage.getClass.getSimpleName
    test(s"multiline scope headers exclude nested members from $className ($i)") {
      withTempDir { root =>
        val conf = codegenConfig(root)
        stage.makePyFile(conf)
        val folder = packageDir(conf.pySrcDir, "/codegen")
        val stubFile = new File(folder, s"$className.pyi")
        val stub = readUtf8(stubFile)
        assert(stub.contains("def afterScope(self) -> Any: ..."))
        assert(!stub.contains("def nestedOnly(") && !stub.contains("def unsupportedLambda("))
        if (stage.isInstanceOf[MultilineFunctionPythonStage]) assert(!stub.contains("def describe("))
        if (stage.isInstanceOf[ContinuedDefaultPythonStage])
          ContinuedDefaultStubs.foreach(signature => assert(occurrences(stub, signature) == 1))
        Seq("get", "set").foreach(accessor => assert(occurrences(stub, s"def ${accessor}ModelCol(") == 1))
        assertPythonCompiles(new File(folder, s"$className.py"))
        assertPythonCompiles(stubFile)
      }
    }
  }
  Seq("'''", "\"\"\"").foreach { quote =>
    test(s"multiline string headers retain subsequent class methods with $quote") {
      withTempDir { root =>
        val conf = codegenConfig(root)
        new MultilineStringPythonStage(quote).makePyFile(conf)
        val folder = packageDir(conf.pySrcDir, "/codegen")
        val stubFile = new File(folder, "MultilineStringPythonStage.pyi")
        assertPythonCompiles(new File(folder, "MultilineStringPythonStage.py"))
        assertPythonCompiles(stubFile)
        val stub = readUtf8(stubFile)
        assert(stub.contains("def afterScope(self) -> Any: ...") && !stub.contains("def describe("))
        assert(occurrences(stub, "def getModelCol(") == 1)
      }
    }

    test(s"multiline string headers detect real accessor collisions with $quote") {
      withTempDir { root =>
        assertAliasConflict(root, new ConflictingMultilineStringAliasMethodStage(quote))
      }
    }
  }

  Seq(
    "@%s # binding" -> true,
    "@ %s" -> true,
    "@((%s))" -> true,
    "@(\n    (%s) # )\n)" -> true,
    "@\\\n%s" -> true,
    """@(_with_label("%s"))""" -> false
  ).foreach { case (binding, expectedBinding) =>
    test(s"binding decorator syntax ${binding.replace("\n", "\\n")} preserves supported descriptor binding") {
      withTempDir { root =>
        val conf = codegenConfig(root)
        new DecoratedPythonStage(binding).makePyFile(conf)
        val folder = packageDir(conf.pySrcDir, "/codegen")
        val stubFile = new File(folder, "DecoratedPythonStage.pyi")
        assertPythonCompiles(new File(folder, "DecoratedPythonStage.py"))
        assertPythonCompiles(stubFile)
        val stub = readUtf8(stubFile)
        assert(stub.contains("@staticmethod\n    def commentedStatic(value: Any) -> Any: ...") == expectedBinding)
        assert(stub.contains("@classmethod\n    def commentedClass(cls, value: Any) -> Any: ...") == expectedBinding)
        assert(stub.contains("@staticmethod\n    def decoratedStatic(value: Any) -> Any: ..."))
        assert(stub.contains("@classmethod\n    def decoratedClass(cls, value: Any) -> Any: ..."))
        assert(stub.contains("@staticmethod\n    def continuedStatic(value: Any) -> Any: ..."))
        assert(stub.contains("def plainValue(self) -> Any: ..."))
        assert(!stub.contains("@staticmethod\n    def plainValue"))
        assert(!stub.contains("@classmethod\n    def plainValue"))
      }
    }
  }

  for (scopeHeader <- Seq("def bodyString(self):", "class BodyString:");
       methodIndent <- Seq("", "\f    ", "\t\f    ")) {
    val whitespace = methodIndent.replace("\f", "\\f").replace("\t", "\\t")
    test(s"body strings do not expose nested accessors in $scopeHeader with $whitespace") {
      withTempDir { root =>
        val conf = codegenConfig(root)
        val stage = new BodyStringPythonStage(scopeHeader, methodIndent)
        val sourceFile = new File(root, "fixture.py")
        writeUtf8(sourceFile, "class Fixture:\n" +
          stage.pyAdditionalMethods.split("\n").map("    " + _).mkString("\n") + "\n")
        assertPythonCompiles(sourceFile)
        stage.makePyFile(conf)
        val folder = packageDir(conf.pySrcDir, "/codegen")
        val stubFile = new File(folder, "BodyStringPythonStage.pyi")
        val stub = readUtf8(stubFile)
        assert(stub.contains("def afterScope(self) -> Any: ..."))
        assert(!stub.contains("def nestedOnly("))
        assert(occurrences(stub, "def getModelCol(") == 1)
        assertPythonCompiles(new File(folder, "BodyStringPythonStage.py"))
        assertPythonCompiles(stubFile)
      }
    }

    test(s"body strings do not hide real accessor collisions after $scopeHeader with $whitespace") {
      withTempDir { root =>
        assertAliasConflict(root, new ConflictingBodyStringAliasMethodStage(scopeHeader, methodIndent))
      }
    }
  }

  for {
    controlBlock <- Seq(false, true)
    (declaration, literal) <- Seq("def ", "\\\ndef ", "\\\n\\\ndef ", "def \\\n ", "def\\\n")
      .map(_ -> "'''\ndef notAHeader(\n'''") ++ Seq("def " -> "''\\\n''", "def " -> "\"\"\\\n\"\"")
  } {
    val syntax = (declaration + literal).replace("\n", "\\n")
    test(s"class statements retain methods with control block $controlBlock and declaration $syntax") {
      withTempDir { root =>
        val conf = codegenConfig(root)
        new ClassStatementStringPythonStage(controlBlock, "afterString", declaration, literal).makePyFile(conf)
        val folder = packageDir(conf.pySrcDir, "/codegen")
        val stubFile = new File(folder, "ClassStatementStringPythonStage.pyi")
        assertPythonCompiles(new File(folder, "ClassStatementStringPythonStage.py"))
        assertPythonCompiles(stubFile)
        val stub = readUtf8(stubFile)
        assert(stub.contains("def afterString(self) -> Any: ..."))
        assert(!stub.contains("def notAHeader("))
      }
    }

    test(s"class statements preserve aliases with control block $controlBlock and declaration $syntax") {
      withTempDir { root =>
        assertAliasConflict(root,
          new ClassStatementStringPythonStage(controlBlock, "getModelCol", declaration, literal))
      }
    }
  }

  test("generated wrappers without service parameters have empty service metadata") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      new TypedPythonModel().makePyFile(conf)
      val runtimeFile = new File(packageDir(conf.pySrcDir, "/codegen"), "TypedPythonModel.py")
      assert(readUtf8(runtimeFile).contains("_service_param_names = frozenset([])"))
      assertPythonCompiles(runtimeFile)
    }
  }

  conflictingAliasNames.foreach {
    case (paramName, aliasName) => test(s"service column alias $aliasName cannot shadow real parameters") {
      withTempDir { root =>
        val stage = new ConflictingServiceAliasStage(paramName = paramName, aliasName = aliasName)
        val error = intercept[IllegalArgumentException](stage.makePyFile(codegenConfig(root)))
        assert(error.getMessage.contains(
          s"Service parameter $paramName cannot use Python alias ${paramName}Col because that Param already exists"))
      }
    }
  }

  conflictingAliasStages.foreach { case (declaration, stage, methodName) =>
    test(s"service column accessors cannot shadow methods declared with $declaration") {
      withTempDir(root => assertAliasConflict(root, stage, methodName))
    }
  }

  test("Unicode method names unsupported by the codegen interpreter fail explicitly") {
    withTempDir { root =>
      val stage = new TypedPythonStage() {
        override def pyAdditionalMethods: String = "def unknown\u0378(self):\n    return None\n"
      }
      val error = intercept[IllegalStateException](stage.makePyFile(codegenConfig(root)))
      assert(error.getMessage.contains("unknown\u0378") && error.getMessage.contains("python"))
      assert(error.getCause != null)
    }
  }

  test("generated estimator stubs preserve companion model return types") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      new TypedPythonModel().makePyFile(conf)
      new TypedPythonEstimator().makePyFile(conf)
      val stubFile = new File(packageDir(conf.pySrcDir, "/codegen"), "TypedPythonEstimator.pyi")
      val stub = readUtf8(stubFile)
      assert(stub.contains(
        "from synapse.ml.codegen.TypedPythonModel import TypedPythonModel"))
      assert(stub.contains(
        "def _create_model(self, java_model: Any) -> TypedPythonModel: ..."))
      assert(stub.contains(
        "def _fit(self, dataset: DataFrame) -> TypedPythonModel: ..."))
      assertPythonCompiles(stubFile)
    }
  }

  test("generated probabilistic classifier wrappers implement the raw prediction contract") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      new TypedPythonClassificationModel().makePyFile(conf)
      new TypedPythonClassifier().makePyFile(conf)
      new TypedPythonEstimator().makePyFile(conf)
      val folder = packageDir(conf.pySrcDir, "/codegen")
      val estimator = readUtf8(new File(folder, "TypedPythonClassifier.py"))
      val estimatorStub = readUtf8(new File(folder, "TypedPythonClassifier.pyi"))
      val model = readUtf8(new File(folder, "TypedPythonClassificationModel.py"))
      val modelStub = readUtf8(new File(folder, "TypedPythonClassificationModel.pyi"))
      val unrelatedEstimator = readUtf8(new File(folder, "TypedPythonEstimator.py"))
      val unrelatedEstimatorStub = readUtf8(new File(folder, "TypedPythonEstimator.pyi"))
      val expectedMixinImport = "from pyspark.ml.param.shared import HasRawPredictionCol"
      val expectedEstimator =
        "class TypedPythonClassifier(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, " +
          "HasRawPredictionCol, JavaEstimator):"
      val expectedModel =
        "class TypedPythonClassificationModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, " +
          "HasRawPredictionCol, JavaModel):"

      assert(estimator.contains(expectedEstimator))
      assert(estimatorStub.contains(expectedEstimator))
      assert(model.contains(expectedModel))
      assert(modelStub.contains(expectedModel))
      assert(estimatorStub.contains(expectedMixinImport))
      assert(modelStub.contains(expectedMixinImport))
      assert(!unrelatedEstimator.contains("HasRawPredictionCol"))
      assert(!unrelatedEstimatorStub.contains("HasRawPredictionCol"))
      assertPythonCompiles(new File(folder, "TypedPythonClassifier.py"))
      assertPythonCompiles(new File(folder, "TypedPythonClassifier.pyi"))
      assertPythonCompiles(new File(folder, "TypedPythonClassificationModel.py"))
      assertPythonCompiles(new File(folder, "TypedPythonClassificationModel.pyi"))
    }
  }

  test("nested init keeps UTF-8 manual content after deterministic generated imports") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/custom/nested"
      val prefix = "# hand written\n"
      val body = "message = \"Grüße 雪\"\n"
      addManualInit(conf, folder, prefix + body)
      addModule(conf, folder, "Zulu.py")
      addModule(conf, folder, "Alpha.py")

      PyCodegen.generateInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      val first = Files.readAllBytes(output.toPath)
      val generated = new String(first, StandardCharsets.UTF_8)
      val alphaImport = "from synapse.ml.custom.nested.Alpha import *"
      val zuluImport = "from synapse.ml.custom.nested.Zulu import *"
      assert(generated.indexOf(alphaImport) >= 0)
      assert(generated.indexOf(alphaImport) < generated.indexOf(zuluImport))
      assert(generated.startsWith(prefix))
      assert(generated.indexOf(zuluImport) < generated.indexOf(body))
      assert(occurrences(generated, body) === 1)

      PyCodegen.generateInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
    }
  }

  test("namespace roots stay absent unless a non-empty manual init is required") {
    withTempDir { root =>
      val absentConf = codegenConfig(new File(root, "absent"))
      ensurePackage(absentConf, "")
      PyCodegen.generateInitFiles(absentConf)
      assert(!initFile(absentConf.pySrcDir, "").exists())

      val emptyConf = codegenConfig(new File(root, "empty"))
      addManualInit(emptyConf, "", "")
      PyCodegen.generateInitFiles(emptyConf)
      assert(!initFile(emptyConf.pySrcDir, "").exists())

      val manualConf = codegenConfig(new File(root, "manual"))
      val manual = "root_value = \"namespace 雪\"\n"
      addManualInit(manualConf, "", manual)
      PyCodegen.generateInitFiles(manualConf)
      val output = initFile(manualConf.pySrcDir, "")
      assert(output.exists())
      assert(readUtf8(output) === manual)

      PyCodegen.generateInitFiles(manualConf)
      assert(readUtf8(output) === manual)
    }
  }

  test("OpenAI init keeps generated hook, manual content, and idempotent ordering") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/services/openai"
      val manual = "manual_value = \"café 雪\"\n"
      addManualInit(conf, folder, manual)
      addModule(conf, folder, "Zulu.py")
      addModule(conf, folder, "Alpha.py")
      addModule(conf, folder, "OpenAICompletion.py")

      PyCodegen.generateInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      val first = Files.readAllBytes(output.toPath)
      val generated = new String(first, StandardCharsets.UTF_8)
      val alphaImport = "from synapse.ml.services.openai.Alpha import *"
      val zuluImport = "from synapse.ml.services.openai.Zulu import *"
      val skippedImport = "from synapse.ml.services.openai.OpenAICompletion import *"
      val hook = "def __getattr__(name):"
      assert(generated.indexOf(alphaImport) < generated.indexOf(zuluImport))
      assert(!generated.contains(skippedImport))
      assert(generated.indexOf(zuluImport) < generated.indexOf(hook))
      assert(generated.indexOf(hook) < generated.indexOf(manual))
      assert(occurrences(generated, hook) === 1)
      assert(occurrences(generated, manual) === 1)
      assert(!initStubFile(conf.pySrcDir, folder).exists())

      PyCodegen.generateInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
    }
  }

  test("OpenAI init stub exports generated classes and the deprecated compatibility class") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/services/openai"
      addModule(conf, folder, "OpenAIPrompt.py")
      addModule(conf, folder, "OpenAICompletion.py")
      writeUtf8(
        new File(packageDir(conf.pySrcDir, folder), "OpenAIPrompt.pyi"),
        "class OpenAIPrompt: ...\n"
      )

      PyCodegen.generateInitFiles(conf)

      val stub = readUtf8(initStubFile(conf.pySrcDir, folder))
      assert(stub.contains(
        "from synapse.ml.services.openai.OpenAIPrompt import OpenAIPrompt as OpenAIPrompt"))
      assert(stub.contains(
        "from synapse.ml.services.openai.OpenAICompletion import OpenAICompletion as OpenAICompletion"))
      assert(!stub.contains("OpenAICompletion import *"))
    }
  }

  test("nested package without manual init gets stable generated imports") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/plain"
      ensurePackage(conf, folder)
      addModule(conf, folder, "Zulu.py")
      addModule(conf, folder, "Alpha.py")
      writeUtf8(new File(packageDir(conf.pySrcDir, folder), "Zulu.pyi"), "class Zulu: ...\n")
      writeUtf8(new File(packageDir(conf.pySrcDir, folder), "Alpha.pyi"), "class Alpha: ...\n")

      PyCodegen.generateInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      val first = Files.readAllBytes(output.toPath)
      val generated = new String(first, StandardCharsets.UTF_8)
      val alphaImport = "from synapse.ml.plain.Alpha import *"
      val zuluImport = "from synapse.ml.plain.Zulu import *"
      assert(generated.indexOf(alphaImport) >= 0)
      assert(generated.indexOf(alphaImport) < generated.indexOf(zuluImport))
      assert(occurrences(generated, alphaImport) === 1)
      assert(occurrences(generated, zuluImport) === 1)
      val stubOutput = initStubFile(conf.pySrcDir, folder)
      val firstStub = Files.readAllBytes(stubOutput.toPath)
      val stub = new String(firstStub, StandardCharsets.UTF_8)
      val alphaStubImport = "from synapse.ml.plain.Alpha import Alpha as Alpha"
      val zuluStubImport = "from synapse.ml.plain.Zulu import Zulu as Zulu"
      assert(stub.indexOf(alphaStubImport) >= 0)
      assert(stub.indexOf(alphaStubImport) < stub.indexOf(zuluStubImport))

      PyCodegen.generateInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
      assert(Files.readAllBytes(stubOutput.toPath).sameElements(firstStub))
    }
  }

  test("services init stub mirrors the runtime langchain exclusion") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      ensurePackage(conf, "/services/openai")
      ensurePackage(conf, "/services/langchain")

      PyCodegen.generateInitFiles(conf)

      val stub = readUtf8(initStubFile(conf.pySrcDir, "/services"))
      assert(stub.contains("from synapse.ml.services.openai import *"))
      assert(!stub.contains("langchain"))
    }
  }

  test("manual Python prologue stays ahead of generated executable statements") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/prologue"
      val prologue =
        "\uFEFF# -*- coding: utf-8 -*-\r\n" +
          "# leading comment\r\n" +
          "\r\n" +
          "\"\"\"Hand-written\r\nmodule documentation.\r\n\"\"\"\r\n" +
          "from __future__ import absolute_import\r\n" +
          "from __future__ import (\r\n    division,\r\n)\r\n"
      val body = "# manual exports\r\nmanual_value = \"Grüße 雪\"\r\n"
      addManualInit(conf, folder, prologue + body)
      addModule(conf, folder, "Generated.py")

      PyCodegen.generateInitFiles(conf)

      val outputFile = initFile(conf.pySrcDir, folder)
      val output = readUtf8(outputFile)
      val generatedImport = "from synapse.ml.prologue.Generated import *"
      assert(output.startsWith(prologue))
      assert(output.indexOf("\uFEFF") === 0)
      assert(output.indexOf("from __future__ import absolute_import") <
        output.indexOf("__version__ ="))
      assert(output.indexOf("division,") < output.indexOf("__version__ ="))
      assert(output.indexOf(generatedImport) < output.indexOf(body))
      assert(occurrences(output, "Hand-written\r\nmodule documentation.") === 1)

      val first = Files.readAllBytes(outputFile.toPath)
      PyCodegen.generateInitFiles(conf)
      assert(Files.readAllBytes(outputFile.toPath).sameElements(first))
    }
  }

  test("blank and comment prefix stays before generated code while manual body stays after it") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/commented"
      val prefix = "# package policy\n\n# generated exports follow\n"
      val body = "manual_value = 1\n"
      addManualInit(conf, folder, prefix + body)
      addModule(conf, folder, "Generated.py")

      PyCodegen.generateInitFiles(conf)

      val output = readUtf8(initFile(conf.pySrcDir, folder))
      assert(output.startsWith(prefix))
      assert(output.indexOf("from synapse.ml.commented.Generated import *") < output.indexOf(body))
    }
  }

  test("inline-commented module docstring keeps following future import legal") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/inlinecomment"
      val docstring = "\"\"\"Package documentation.\"\"\"  # retained explanation\n"
      val futureImport = "from __future__ import absolute_import\n"
      val body = "manual_value = 1\n"
      addManualInit(conf, folder, docstring + futureImport + body)
      addModule(conf, folder, "Generated.py")

      PyCodegen.generateInitFiles(conf)

      val outputFile = initFile(conf.pySrcDir, folder)
      val output = readUtf8(outputFile)
      assert(output.startsWith(docstring + futureImport))
      assert(output.indexOf(futureImport) < output.indexOf("__version__ ="))
      assert(output.indexOf("from synapse.ml.inlinecomment.Generated import *") < output.indexOf(body))
      assertPythonCompiles(outputFile)
    }
  }

  test("deleted and renamed modules do not leave stale generated initializer content") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/transitions"
      val manual = "manual_value = \"preserved\"\n"
      addManualInit(conf, folder, manual)
      val oldModule = new File(packageDir(conf.pySrcDir, folder), "OldName.py")
      writeUtf8(oldModule, "")

      PyCodegen.generateInitFiles(conf)
      val output = initFile(conf.pySrcDir, folder)
      assert(readUtf8(output).contains("from synapse.ml.transitions.OldName import *"))

      assert(oldModule.delete())
      addModule(conf, folder, "NewName.py")
      PyCodegen.generateInitFiles(conf)
      val renamed = readUtf8(output)
      assert(!renamed.contains("OldName"))
      assert(renamed.contains("from synapse.ml.transitions.NewName import *"))
      assert(occurrences(renamed, manual) === 1)

      assert(initFile(conf.pySrcOverrideDir, folder).delete())
      PyCodegen.generateInitFiles(conf)
      val withoutManual = readUtf8(output)
      assert(!withoutManual.contains(manual))
      assert(occurrences(withoutManual, "NewName") === 1)
    }
  }

  test("cognitive compatibility init remains entirely hand written") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/cognitive"
      val manual = "compatibility_value = \"manual 雪\"\n"
      addManualInit(conf, folder, manual)
      addModule(conf, folder, "Generated.py")

      PyCodegen.generateInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      assert(readUtf8(output) === manual)
      PyCodegen.generateInitFiles(conf)
      assert(readUtf8(output) === manual)
    }
  }

  test("component wheel includes a preserved non-empty namespace root init") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val manual = "wheel_marker = \"Grüße 雪\"\n"
      addManualInit(conf, "", manual)
      addModule(conf, "/nested", "Widget.py")
      writeUtf8(new File(packageDir(conf.pySrcDir, "/nested"), "Widget.pyi"), "class Widget: ...\n")
      PyCodegen.generatePyPackageData(conf)
      PyCodegen.generateInitFiles(conf)

      val wheel = buildWheel(conf.pySrcDir, new File(conf.targetDir, "wheel-test"))
      assert(wheelEntryContent(wheel, "synapse/ml/__init__.py").contains(manual))
      assert(wheelEntryContent(wheel, "synapse/ml/py.typed").isEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.py").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.pyi").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/py.typed").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/Widget.pyi").nonEmpty)
    }
  }

  test("aggregate wheel includes a preserved non-empty namespace root init") {
    withTempDir { root =>
      val sourceDir = new File(root, "aggregate-source")
      val wheelDir = new File(root, "aggregate-wheel")
      val manual = "aggregate_marker = \"café 雪\"\n"
      writeUtf8(new File(sourceDir, "synapse/ml/__init__.py"), manual)
      writeUtf8(new File(sourceDir, "synapse/ml/nested/__init__.py"), "")
      writeUtf8(
        new File(sourceDir, "synapse/ml/nested/__init__.pyi"),
        "from .Widget import Widget as Widget\n"
      )
      writeUtf8(new File(sourceDir, "synapse/ml/nested/py.typed"), "")
      writeUtf8(new File(sourceDir, "synapse/ml/nested/Widget.pyi"), "class Widget: ...\n")
      val setup = "from setuptools import setup, find_namespace_packages\n" +
        "setup(name=\"aggregate-wheel-test\", version=\"1.0.0\", packages=" +
        aggregatePackageDiscovery() + ", package_data=" + aggregatePackageData() + ")\n"
      writeUtf8(new File(sourceDir, "setup.py"), setup)

      val wheel = buildWheel(sourceDir, wheelDir)
      assert(wheelEntryContent(wheel, "synapse/ml/__init__.py").contains(manual))
      assert(wheelEntryContent(wheel, "synapse/ml/py.typed").isEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.py").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.pyi").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/py.typed").nonEmpty)
      assert(wheelEntryContent(wheel, "synapse/ml/nested/Widget.pyi").nonEmpty)
    }
  }
}
