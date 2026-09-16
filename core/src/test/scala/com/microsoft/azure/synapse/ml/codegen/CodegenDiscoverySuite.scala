// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils
import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileOutputStream}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.jar.{JarEntry, JarOutputStream}
import scala.collection.JavaConverters._

object CodegenDiscoveryProbe {
  def main(args: Array[String]): Unit = {
    val conf = CodegenConfig("synapseml-core", Some(args(0)), args(2), args(2),
      "1.0.0", "1.0.0", "1.0.0", "synapseml")
    val fixtureName = classOf[PyCodegenFixtures.TypedPythonEstimator].getName
    val fixtureResource = getClass.getResource("/" + fixtureName.replace('.', '/') + ".class")
    require(fixtureResource.getProtocol == "jar" && fixtureResource.toString.contains(args(1)),
      s"Fixture must load from the test JAR, not an exploded directory: $fixtureResource")

    PyCodegen.generatePythonClasses(conf)
    RCodegen.generateRClasses(conf)
    require(new File(conf.pySrcDir, "synapse/ml/stages/SelectColumns.py").isFile)
    require(new File(conf.pySrcDir, "synapse/ml/stages/SelectColumns.pyi").isFile)
    require(new File(conf.rSrcDir, "ml_select_columns.R").isFile)
    require(!new File(conf.pySrcDir, "synapse/ml/codegen/TypedPythonEstimator.py").exists())
    require(!new File(conf.pySrcDir, "synapse/ml/codegen/ForeignParamPythonStage.py").exists())

    val testClasses = JarLoadingUtils.instantiateServices[AnyRef]((c: Class[_]) => c, Some(args(1)))
    require(testClasses.contains(classOf[PyCodegenFixtures.TypedPythonEstimator]))
    require(!testClasses.contains(classOf[com.microsoft.azure.synapse.ml.stages.SelectColumns]))
    val allClasses = JarLoadingUtils.instantiateServices[AnyRef]((c: Class[_]) => c, None)
    require(allClasses.contains(classOf[PyCodegenFixtures.TypedPythonEstimator]))
    require(allClasses.contains(classOf[com.microsoft.azure.synapse.ml.stages.SelectColumns]))
    try {
      JarLoadingUtils.instantiateServices[PythonWrappable](Some(args(1)))
      throw new AssertionError("Explicit test-JAR discovery must retain invalid-constructor failures")
    } catch {
      case _: NoSuchMethodException => ()
    }
    println("PUBLISHED_TEST_JAR_CODEGEN_OK")
  }
}

class CodegenDiscoverySuite extends AnyFunSuite {
  test("artifact matching separates main and test jars, including jars under target") {
    Seq("1.0.0", "1.0.0-SNAPSHOT").foreach { version =>
      val main = s"synapseml-core_2.13-$version.jar"
      val tests = s"synapseml-core_2.13-$version-tests.jar"
      Seq("/repo%20dir/", "/workspace/core/target/scala-2.13/").foreach { directory =>
        def resource(name: String): URL = new URL(s"jar:file:$directory$name!/example/Stage.class")
        assert(JarLoadingUtils.matchesJar(resource(main), main))
        assert(JarLoadingUtils.matchesJar(resource(tests), tests))
        assert(!JarLoadingUtils.matchesJar(resource(tests), main))
        assert(!JarLoadingUtils.matchesJar(resource(main), tests))
      }
    }
  }

  test("snapshot aliases preserve exact artifact and classifier boundaries") {
    val main = "synapseml-core_2.13-1.0.0.jar"
    val tests = "synapseml-core_2.13-1.0.0-tests.jar"
    def resource(name: String): URL = new URL(s"jar:file:/repo/$main/$name!/example/Stage.class")
    assert(JarLoadingUtils.matchesJar(resource(main), "synapseml-core_2.13-1.0.0-SNAPSHOT.jar"))
    assert(JarLoadingUtils.matchesJar(resource("synapseml-core_2.13-1.0.0-SNAPSHOT.jar"), main))
    assert(JarLoadingUtils.matchesJar(resource("synapseml-core_2.13-1.0.0-SNAPSHOT-tests.jar"), tests))
    assert(JarLoadingUtils.matchesJar(resource(tests), "synapseml-core_2.13-1.0.0-tests-SNAPSHOT.jar"))
    Seq(tests, "synapseml-core_2.13-1.0.01.jar", "synapseml-core_2.13-1.0.0-sources.jar",
      "synapseml-opencv_2.13-1.0.0.jar").foreach { name =>
      assert(!JarLoadingUtils.matchesJar(resource(name), main))
    }
  }

  test("exploded discovery preserves the module and main or test output scope") {
    Seq("", "scala-2.13/").foreach { scalaDirectory =>
      val root = s"file:/workspace/core/target/$scalaDirectory"
      val main = "synapseml-core_2.13-1.0.0-SNAPSHOT.jar"
      val tests = "synapseml-core_2.13-1.0.0-SNAPSHOT-tests.jar"
      val mainResource = new URL(root + "classes/example/Stage.class")
      val testResource = new URL(root + "test-classes/example/Fixture.class")
      assert(JarLoadingUtils.matchesJar(mainResource, main))
      assert(JarLoadingUtils.matchesJar(testResource, tests))
      assert(!JarLoadingUtils.matchesJar(testResource, main))
      assert(!JarLoadingUtils.matchesJar(mainResource, tests))
      assert(!JarLoadingUtils.matchesJar(mainResource, "synapseml-opencv_2.13-1.0.0.jar"))
    }
  }

  test("file URLs containing exclamation marks preserve main and test output scope") {
    val main = "synapseml-core_2.13-1.0.0.jar"
    val tests = "synapseml-core_2.13-1.0.0-tests.jar"
    Seq("build!", "build%21", s"$main!", s"$main%21").foreach { directory =>
      Seq("", "scala-2.13/").foreach { scalaDirectory =>
        val root = s"file:/workspace/$directory/core/target/$scalaDirectory"
        val mainResource = new URL(root + "classes/example/Stage.class")
        val testResource = new URL(root + "test-classes/example/Fixture.class")
        assert(JarLoadingUtils.matchesJar(mainResource, main))
        assert(JarLoadingUtils.matchesJar(testResource, tests))
        assert(!JarLoadingUtils.matchesJar(testResource, main))
        assert(!JarLoadingUtils.matchesJar(mainResource, tests))
      }
    }
  }

  test("jar URLs decode exclamation marks only after locating the archive") {
    val main = "synapseml-core_2.13-1.0.0.jar"
    val tests = "synapseml-core_2.13-1.0.0-tests.jar"
    def resource(name: String): URL =
      new URL(s"jar:file:/workspace/build%21/core/target/$name!/example/Stage.class")
    assert(JarLoadingUtils.matchesJar(resource(main), main))
    assert(JarLoadingUtils.matchesJar(resource(tests), tests))
    assert(!JarLoadingUtils.matchesJar(resource(tests), main))
    assert(!JarLoadingUtils.matchesJar(resource(main), tests))
  }

  private def codeSource(clazz: Class[_]): File =
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getCanonicalFile

  private def packageClasses(source: File, jar: File, include: String => Boolean): Unit = {
    require(source.isDirectory, s"Expected compiled class directory: $source")
    val paths = Files.walk(source.toPath)
    try {
      val archive = new JarOutputStream(new FileOutputStream(jar))
      try {
        paths.iterator().asScala.filter(path => Files.isRegularFile(path)).foreach { path =>
          val name = source.toPath.relativize(path).toString.replace(File.separatorChar, '/')
          if (name.endsWith(".class") && include(name)) {
            archive.putNextEntry(new JarEntry(name))
            Files.copy(path, archive)
            archive.closeEntry()
          }
        }
      } finally {
        archive.close()
      }
    } finally {
      paths.close()
    }
  }

  test("codegen isolates production classes when published test jars share the classpath") {
    val root = Files.createTempDirectory("codegen-discovery space-").toFile
    try {
      val output = new File(root, "core/target/scala-2.13")
      assert(output.mkdirs())
      val mainJar = new File(output, "synapseml-core_2.13-1.0.0.jar")
      val testJar = new File(output, "synapseml-core_2.13-1.0.0-tests.jar")
      val mainClasses = codeSource(classOf[Wrappable])
      val testClasses = codeSource(classOf[CodegenDiscoverySuite])
      packageClasses(mainClasses, mainJar, _ => true)
      packageClasses(testClasses, testJar, name =>
        name.startsWith("com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures") ||
          name.startsWith("com/microsoft/azure/synapse/ml/codegen/CodegenDiscoveryProbe"))
      val dependencies = System.getProperty("java.class.path").split(File.pathSeparator)
        .map(path => new File(path).getCanonicalFile)
        .filterNot(path => path == mainClasses || path == testClasses)
      val classpath = (Seq(mainJar, testJar) ++ dependencies).map(_.getAbsolutePath).mkString(File.pathSeparator)
      val javaExecutable = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
      val log = new File(root, "probe.log")
      val process = new ProcessBuilder(javaExecutable, "-cp", classpath,
        CodegenDiscoveryProbe.getClass.getName.stripSuffix("$"),
        mainJar.getName, testJar.getName, new File(root, "generated").getAbsolutePath)
        .redirectErrorStream(true).redirectOutput(log).start()
      val completed = process.waitFor(5, TimeUnit.MINUTES)
      if (!completed) {
        process.destroyForcibly()
        process.waitFor()
      }
      val text = FileUtils.readFileToString(log, StandardCharsets.UTF_8)
      assert(completed, s"Published-JAR probe timed out:\n$text")
      assert(process.exitValue() == 0, text)
      assert(text.contains("PUBLISHED_TEST_JAR_CODEGEN_OK"), text)
    } finally {
      FileUtils.deleteDirectory(root)
    }
  }
}
