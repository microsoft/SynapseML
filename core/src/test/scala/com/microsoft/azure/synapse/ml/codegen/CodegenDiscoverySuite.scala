// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils
import org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

import java.io.{File, FileOutputStream, IOException}
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.jar.{JarEntry, JarOutputStream}
import java.util.zip.Deflater
import scala.collection.JavaConverters._

object CodegenDiscoveryLauncher {
  def main(args: Array[String]): Unit = {
    val urls = System.getProperty("java.class.path").split(File.pathSeparator).map(path => new File(path).toURI.toURL)
    // Match SBT's URL-based loader; Spark 3's shaded Guava cannot scan the JDK application loader.
    // The application loader's parent is the extension loader on Java 8 and platform loader on newer JDKs.
    val loader = new URLClassLoader(urls, ClassLoader.getSystemClassLoader.getParent)
    val thread = Thread.currentThread()
    val original = thread.getContextClassLoader
    try {
      thread.setContextClassLoader(loader)
      val probe = loader.loadClass("com.microsoft.azure.synapse.ml.codegen.CodegenDiscoveryProbe")
      require(probe.getClassLoader eq loader, "Probe must load through the isolated URL classloader")
      probe.getMethod("main", classOf[Array[String]]).invoke(probe, args)
    } finally {
      thread.setContextClassLoader(original)
      loader.close()
    }
  }
}

object CodegenDiscoveryProbe {
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Expected main JAR name, test JAR name, and output directory")
    val conf = CodegenConfig("synapseml-core", Some(args(0)), args(2), args(2),
      "1.0.0", "1.0.0", "1.0.0", "synapseml")
    val fixtureName = classOf[PyCodegenFixtures.TypedPythonEstimator].getName
    val fixtureResource = getClass.getResource("/" + fixtureName.replace('.', '/') + ".class")
    require(fixtureResource != null && fixtureResource.getProtocol == "jar" &&
      fixtureResource.toString.contains(args(1)),
      s"Fixture must load from the test JAR, not an exploded directory: $fixtureResource")

    PyCodegen.generatePythonClasses(conf)
    RCodegen.generateRClasses(conf)
    require(new File(conf.pySrcDir, "synapse/ml/stages/SelectColumns.py").isFile, "Missing production Python wrapper")
    require(new File(conf.pySrcDir, "synapse/ml/stages/SelectColumns.pyi").isFile, "Missing production Python stub")
    require(new File(conf.rSrcDir, "ml_select_columns.R").isFile, "Missing production R wrapper")
    require(!new File(conf.pySrcDir, "synapse/ml/codegen/TypedPythonEstimator.py").exists(),
      "Production discovery included the test estimator")
    require(!new File(conf.pySrcDir, "synapse/ml/codegen/ForeignParamPythonStage.py").exists(),
      "Production discovery included the foreign-parameter test stage")

    val testClasses = JarLoadingUtils.instantiateServices[AnyRef]((c: Class[_]) => c, Some(args(1)))
    require(testClasses.contains(classOf[PyCodegenFixtures.TypedPythonEstimator]),
      "Explicit test discovery missed fixture")
    require(!testClasses.contains(classOf[com.microsoft.azure.synapse.ml.stages.SelectColumns]),
      "Explicit test discovery included production classes")
    val allClasses = JarLoadingUtils.instantiateServices[AnyRef]((c: Class[_]) => c, None)
    require(allClasses.contains(classOf[PyCodegenFixtures.TypedPythonEstimator]),
      "Unscoped discovery missed test classes")
    require(allClasses.contains(classOf[com.microsoft.azure.synapse.ml.stages.SelectColumns]),
      "Unscoped discovery missed production classes")
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
  private val scalaBinaryVersion = scala.util.Properties.versionNumberString.split('.').take(2).mkString(".")

  test("artifact matching separates main and test jars, including jars under target") {
    Seq("1.0.0", "1.0.0-SNAPSHOT").foreach { version =>
      val main = s"synapseml-core_$scalaBinaryVersion-$version.jar"
      val tests = s"synapseml-core_$scalaBinaryVersion-$version-tests.jar"
      Seq("/repo%20dir/", s"/workspace/core/target/scala-$scalaBinaryVersion/").foreach { directory =>
        def resource(name: String): URL = new URL(s"jar:file:$directory$name!/example/Stage.class")
        assert(JarLoadingUtils.matchesJar(resource(main), main))
        assert(JarLoadingUtils.matchesJar(resource(tests), tests))
        assert(!JarLoadingUtils.matchesJar(resource(tests), main))
        assert(!JarLoadingUtils.matchesJar(resource(main), tests))
      }
    }
  }

  test("snapshot aliases preserve exact artifact and classifier boundaries") {
    val main = s"synapseml-core_$scalaBinaryVersion-1.0.0.jar"
    val tests = s"synapseml-core_$scalaBinaryVersion-1.0.0-tests.jar"
    def resource(name: String): URL = new URL(s"jar:file:/repo/$main/$name!/example/Stage.class")
    assert(JarLoadingUtils.matchesJar(resource(main), s"synapseml-core_$scalaBinaryVersion-1.0.0-SNAPSHOT.jar"))
    assert(JarLoadingUtils.matchesJar(resource(s"synapseml-core_$scalaBinaryVersion-1.0.0-SNAPSHOT.jar"), main))
    assert(JarLoadingUtils.matchesJar(resource(s"synapseml-core_$scalaBinaryVersion-1.0.0-SNAPSHOT-tests.jar"), tests))
    assert(JarLoadingUtils.matchesJar(resource(tests), s"synapseml-core_$scalaBinaryVersion-1.0.0-tests-SNAPSHOT.jar"))
    Seq(tests, s"synapseml-core_$scalaBinaryVersion-1.0.01.jar",
      s"synapseml-core_$scalaBinaryVersion-1.0.0-sources.jar",
      s"synapseml-opencv_$scalaBinaryVersion-1.0.0.jar").foreach { name =>
      assert(!JarLoadingUtils.matchesJar(resource(name), main))
    }
  }

  test("exploded discovery preserves the module and main or test output scope") {
    Seq("", s"scala-$scalaBinaryVersion/").foreach { scalaDirectory =>
      val root = s"file:/workspace/core/target/$scalaDirectory"
      val main = s"synapseml-core_$scalaBinaryVersion-1.0.0-SNAPSHOT.jar"
      val tests = s"synapseml-core_$scalaBinaryVersion-1.0.0-SNAPSHOT-tests.jar"
      val mainResource = new URL(root + "classes/example/Stage.class")
      val testResource = new URL(root + "test-classes/example/Fixture.class")
      assert(JarLoadingUtils.matchesJar(mainResource, main))
      assert(JarLoadingUtils.matchesJar(testResource, tests))
      assert(!JarLoadingUtils.matchesJar(testResource, main))
      assert(!JarLoadingUtils.matchesJar(mainResource, tests))
      assert(!JarLoadingUtils.matchesJar(mainResource, s"synapseml-opencv_$scalaBinaryVersion-1.0.0.jar"))
    }
  }

  test("file URLs containing exclamation marks preserve main and test output scope") {
    val main = s"synapseml-core_$scalaBinaryVersion-1.0.0.jar"
    val tests = s"synapseml-core_$scalaBinaryVersion-1.0.0-tests.jar"
    Seq("build!", "build%21", s"$main!", s"$main%21").foreach { directory =>
      Seq("", s"scala-$scalaBinaryVersion/").foreach { scalaDirectory =>
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
    val main = s"synapseml-core_$scalaBinaryVersion-1.0.0.jar"
    val tests = s"synapseml-core_$scalaBinaryVersion-1.0.0-tests.jar"
    def resource(name: String): URL =
      new URL(s"jar:file:/workspace/build%21/core/target/$name!/example/Stage.class")
    assert(JarLoadingUtils.matchesJar(resource(main), main))
    assert(JarLoadingUtils.matchesJar(resource(tests), tests))
    assert(!JarLoadingUtils.matchesJar(resource(tests), main))
    assert(!JarLoadingUtils.matchesJar(resource(main), tests))
  }

  test("invalid resource URLs fail explicitly with artifact and resource context") {
    val main = s"synapseml-core_$scalaBinaryVersion-1.0.0.jar"
    Seq(
      new URL("file:/workspace/with space/core/target/classes/example/Stage.class"),
      new URL("jar:file:relative.jar!/example/Stage.class")
    ).foreach { resource =>
      val error = intercept[IOException] {
        JarLoadingUtils.matchesJar(resource, main)
      }
      assert(error.getMessage.contains(main))
      assert(error.getMessage.contains(resource.toString))
      assert(error.getCause != null)
    }
  }

  private def codeSource(clazz: Class[_]): File =
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getCanonicalFile

  private def runtimeClasspath: Seq[File] = {
    val loaderPaths = Iterator.iterate(getClass.getClassLoader)(_.getParent)
      .takeWhile(_ != null)
      .collect { case loader: URLClassLoader => loader }
      .flatMap(_.getURLs.iterator)
      .filter(_.getProtocol == "file")
      .map(url => new File(url.toURI))
      .toVector
    val systemPaths = System.getProperty("java.class.path").split(File.pathSeparator).map(new File(_))
    (loaderPaths ++ systemPaths).map(_.getCanonicalFile).distinct
  }

  private def packageClasses(source: File, jar: File, include: String => Boolean): Unit = {
    require(source.isDirectory, s"Expected compiled class directory: $source")
    val paths = Files.walk(source.toPath)
    try {
      val archive = new JarOutputStream(new FileOutputStream(jar))
      archive.setLevel(Deflater.NO_COMPRESSION)
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
      val output = new File(root, s"core/target/scala-$scalaBinaryVersion")
      assert(output.mkdirs())
      val mainJar = new File(output, s"synapseml-core_$scalaBinaryVersion-1.0.0.jar")
      val testJar = new File(output, s"synapseml-core_$scalaBinaryVersion-1.0.0-tests.jar")
      val mainClasses = codeSource(classOf[Wrappable])
      val testClasses = codeSource(classOf[CodegenDiscoverySuite])
      packageClasses(mainClasses, mainJar, _ => true)
      packageClasses(testClasses, testJar, name =>
        name.startsWith("com/microsoft/azure/synapse/ml/codegen/PyCodegenFixtures") ||
          name.startsWith("com/microsoft/azure/synapse/ml/codegen/CodegenDiscoveryLauncher") ||
          name.startsWith("com/microsoft/azure/synapse/ml/codegen/CodegenDiscoveryProbe"))
      val dependencies = runtimeClasspath
        .filterNot(path => path == mainClasses || path == testClasses)
      val classpath = (Seq(mainJar, testJar) ++ dependencies).map(_.getAbsolutePath).mkString(File.pathSeparator)
      val javaExecutable = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
      val log = new File(root, "probe.log")
      val process = new ProcessBuilder(javaExecutable, "-cp", classpath,
        CodegenDiscoveryLauncher.getClass.getName.stripSuffix("$"),
        mainJar.getName, testJar.getName, new File(root, "generated").getAbsolutePath)
        .redirectErrorStream(true).redirectOutput(log).start()
      val completed = try {
        process.waitFor(5, TimeUnit.MINUTES)
      } finally {
        if (process.isAlive) {
          process.destroyForcibly()
          process.waitFor()
        }
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
