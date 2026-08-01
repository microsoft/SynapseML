// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import org.apache.commons.io.{FileUtils, IOUtils}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.zip.ZipFile

class PyCodegenSuite extends AnyFunSuite {

  private def withTempDir(testCode: File => Unit): Unit = {
    val dir = Files.createTempDirectory("py-codegen-suite").toFile
    try testCode(dir) finally FileUtils.deleteDirectory(dir)
  }

  private def codegenConfig(root: File): CodegenConfig = CodegenConfig(
    "test-module",
    None,
    root.getAbsolutePath,
    new File(root, "target").getAbsolutePath,
    "1.0.0",
    "1.0.0",
    "1.0.0",
    "synapseml-test")

  private def packageDir(base: File, packageFolder: String): File = {
    val namespaceRoot = new File(new File(base, "synapse"), "ml")
    packageFolder.split("/").filter(_.nonEmpty).foldLeft(namespaceRoot)(new File(_, _))
  }

  private def initFile(base: File, packageFolder: String): File =
    new File(packageDir(base, packageFolder), "__init__.py")

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
    val python = if (System.getProperty("os.name").toLowerCase.contains("windows")) "python" else "python3"
    val process = new ProcessBuilder(
      python, "setup.py", "bdist_wheel", "--universal", "-d", wheelDir.getAbsolutePath)
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

  private def aggregatePackageDiscovery(): String = {
    def repositoryRoot(candidate: File): File = {
      if (new File(candidate, "build.sbt").isFile && new File(candidate, "core").isDirectory) candidate
      else Option(candidate.getParentFile).map(repositoryRoot)
        .getOrElse(fail(s"Could not find repository root from ${System.getProperty("user.dir")}"))
    }
    val buildFile = new File(repositoryRoot(new File(System.getProperty("user.dir"))), "build.sbt")
    val lines = readUtf8(buildFile).split("\n")
      .filter(_.contains("|    packages=find_namespace_packages("))
    assert(lines.length === 1)
    lines.head.trim.stripPrefix("|    packages=").stripSuffix(",")
  }

  test("nested init keeps UTF-8 manual content after deterministic generated imports") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/custom/nested"
      val manual = "# hand written\nmessage = \"Grüße 雪\"\n"
      addManualInit(conf, folder, manual)
      addModule(conf, folder, "Zulu.py")
      addModule(conf, folder, "Alpha.py")

      PyCodegen.makeInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      val first = Files.readAllBytes(output.toPath)
      val generated = new String(first, StandardCharsets.UTF_8)
      val alphaImport = "from synapse.ml.custom.nested.Alpha import *"
      val zuluImport = "from synapse.ml.custom.nested.Zulu import *"
      assert(generated.indexOf(alphaImport) >= 0)
      assert(generated.indexOf(alphaImport) < generated.indexOf(zuluImport))
      assert(generated.indexOf(zuluImport) < generated.indexOf(manual))
      assert(occurrences(generated, manual) === 1)

      PyCodegen.makeInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
    }
  }

  test("namespace roots stay absent unless a non-empty manual init is required") {
    withTempDir { root =>
      val absentConf = codegenConfig(new File(root, "absent"))
      ensurePackage(absentConf, "")
      PyCodegen.makeInitFiles(absentConf)
      assert(!initFile(absentConf.pySrcDir, "").exists())

      val emptyConf = codegenConfig(new File(root, "empty"))
      addManualInit(emptyConf, "", "")
      PyCodegen.makeInitFiles(emptyConf)
      assert(!initFile(emptyConf.pySrcDir, "").exists())

      val manualConf = codegenConfig(new File(root, "manual"))
      val manual = "root_value = \"namespace 雪\"\n"
      addManualInit(manualConf, "", manual)
      PyCodegen.makeInitFiles(manualConf)
      val output = initFile(manualConf.pySrcDir, "")
      assert(output.exists())
      assert(readUtf8(output) === manual)

      PyCodegen.makeInitFiles(manualConf)
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

      PyCodegen.makeInitFiles(conf)

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

      PyCodegen.makeInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
    }
  }

  test("nested package without manual init gets stable generated imports") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/plain"
      ensurePackage(conf, folder)
      addModule(conf, folder, "Zulu.py")
      addModule(conf, folder, "Alpha.py")

      PyCodegen.makeInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      val first = Files.readAllBytes(output.toPath)
      val generated = new String(first, StandardCharsets.UTF_8)
      val alphaImport = "from synapse.ml.plain.Alpha import *"
      val zuluImport = "from synapse.ml.plain.Zulu import *"
      assert(generated.indexOf(alphaImport) >= 0)
      assert(generated.indexOf(alphaImport) < generated.indexOf(zuluImport))
      assert(occurrences(generated, alphaImport) === 1)
      assert(occurrences(generated, zuluImport) === 1)

      PyCodegen.makeInitFiles(conf)
      assert(Files.readAllBytes(output.toPath).sameElements(first))
    }
  }

  test("cognitive compatibility init remains entirely hand written") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val folder = "/cognitive"
      val manual = "compatibility_value = \"manual 雪\"\n"
      addManualInit(conf, folder, manual)
      addModule(conf, folder, "Generated.py")

      PyCodegen.makeInitFiles(conf)

      val output = initFile(conf.pySrcDir, folder)
      assert(readUtf8(output) === manual)
      PyCodegen.makeInitFiles(conf)
      assert(readUtf8(output) === manual)
    }
  }

  test("component wheel includes a preserved non-empty namespace root init") {
    withTempDir { root =>
      val conf = codegenConfig(root)
      val manual = "wheel_marker = \"Grüße 雪\"\n"
      addManualInit(conf, "", manual)
      addModule(conf, "/nested", "Widget.py")
      PyCodegen.generatePyPackageData(conf)
      PyCodegen.makeInitFiles(conf)

      val wheel = buildWheel(conf.pySrcDir, new File(conf.targetDir, "wheel-test"))
      assert(wheelEntryContent(wheel, "synapse/ml/__init__.py").contains(manual))
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.py").nonEmpty)
    }
  }

  test("aggregate wheel includes a preserved non-empty namespace root init") {
    withTempDir { root =>
      val sourceDir = new File(root, "aggregate-source")
      val wheelDir = new File(root, "aggregate-wheel")
      val manual = "aggregate_marker = \"café 雪\"\n"
      writeUtf8(new File(sourceDir, "synapse/ml/__init__.py"), manual)
      writeUtf8(new File(sourceDir, "synapse/ml/nested/__init__.py"), "")
      val setup = "from setuptools import setup, find_namespace_packages\n" +
        "setup(name=\"aggregate-wheel-test\", version=\"1.0.0\", packages=" +
        aggregatePackageDiscovery() + ")\n"
      writeUtf8(new File(sourceDir, "setup.py"), setup)

      val wheel = buildWheel(sourceDir, wheelDir)
      assert(wheelEntryContent(wheel, "synapse/ml/__init__.py").contains(manual))
      assert(wheelEntryContent(wheel, "synapse/ml/nested/__init__.py").nonEmpty)
    }
  }
}
