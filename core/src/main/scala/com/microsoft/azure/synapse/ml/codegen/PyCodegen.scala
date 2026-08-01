// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.codegen.GenerationUtils.indent
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils._
import org.apache.spark.ml.{Estimator, Model}
import spray.json._

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

object PyCodegen {

  import CodeGenUtils._

  private val DeprecatedOpenAICompletionFile = "OpenAICompletion.py"

  private val OpenAICompletionImportHook: String =
    """
      |def __getattr__(name):
      |    if name == "OpenAICompletion":
      |        import warnings
      |
      |        with warnings.catch_warnings():
      |            warnings.simplefilter("ignore", FutureWarning)
      |            from synapse.ml.services.openai.OpenAICompletion import (
      |                OpenAICompletion,
      |                warn_openai_completion_deprecated,
      |            )
      |        warn_openai_completion_deprecated(stacklevel=2)
      |        globals()["OpenAICompletion"] = OpenAICompletion
      |        return OpenAICompletion
      |    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
      |""".stripMargin

  private def isOpenAICompletionStub(packageFolder: String, fileName: String): Boolean =
    packageFolder == "/services/openai" && fileName == DeprecatedOpenAICompletionFile

  private def initFileExtra(packageFolder: String): String =
    if (packageFolder == "/services/openai") OpenAICompletionImportHook else ""

  def generatePythonClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[PythonWrappable](conf.jarName)
    instantiatedClasses.foreach { w =>
      println(w.getClass.getName)
      w.makePyFile(conf)
    }
  }

  private def packageDirectory(base: File, packageFolder: String): File =
    packageFolder.split("/").filter(_.nonEmpty)
      .foldLeft(join(base, "synapse", "ml"))((parent, child) => new File(parent, child))

  private def directoryEntries(dir: File): Array[File] =
    Option(dir.listFiles()).getOrElse(Array.empty[File])

  private def generatedImports(dir: File, packageFolder: String): String = {
    val packageString = packageFolder.replace("/", ".")
    if (packageFolder == "/services") {
      directoryEntries(dir).filter(_.isDirectory).map(_.getName)
        .filter(_ != "langchain").sorted
        .map(name => s"from synapse.ml$packageString.$name import *\n").mkString("")
    } else {
      directoryEntries(dir).filter(_.isFile).map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .filterNot(name => isOpenAICompletionStub(packageFolder, name)).sorted
        .map(name => s"from synapse.ml$packageString.${getBaseName(name)} import *\n").mkString("")
    }
  }

  private def readUtf8(file: File): String =
    new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8)

  private def writeUtf8(file: File, content: String): Unit = {
    Files.write(file.toPath, content.getBytes(StandardCharsets.UTF_8))
    ()
  }

  private def manualInitContent(conf: CodegenConfig, packageFolder: String): Option[String] = {
    val sourceInit = new File(packageDirectory(conf.pySrcOverrideDir, packageFolder), "__init__.py")
    if (sourceInit.isFile) Some(readUtf8(sourceInit)) else None
  }

  private def writeManualOnlyInit(initFile: File,
                                  manualContent: Option[String],
                                  preserveEmpty: Boolean): Unit = {
    manualContent.filter(content => preserveEmpty || content.nonEmpty) match {
      case Some(content) => writeUtf8(initFile, content)
      case None =>
        Files.deleteIfExists(initFile.toPath)
        ()
    }
  }

  private def appendManualContent(generatedContent: String, manualContent: Option[String]): String =
    manualContent.filter(_.nonEmpty) match {
      case Some(content) if generatedContent.endsWith("\n") || content.startsWith("\n") =>
        generatedContent + content
      case Some(content) => generatedContent + "\n" + content
      case None => generatedContent
    }

  private def writeInitFile(conf: CodegenConfig, packageFolder: String, dir: File): Unit = {
    val initFile = new File(dir, "__init__.py")
    val manualContent = manualInitContent(conf, packageFolder)
    packageFolder match {
      case "" => writeManualOnlyInit(initFile, manualContent, preserveEmpty = false)
      case "/cognitive" => writeManualOnlyInit(initFile, manualContent, preserveEmpty = true)
      case _ =>
        val generatedContent = conf.packageHelp(generatedImports(dir, packageFolder)) + initFileExtra(packageFolder)
        writeUtf8(initFile, appendManualContent(generatedContent, manualContent))
    }
  }

  private[codegen] def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = packageDirectory(conf.pySrcDir, packageFolder)
    val childDirectories = directoryEntries(dir).filter(_.isDirectory).sortBy(_.getName)
    writeInitFile(conf, packageFolder, dir)
    childDirectories.foreach(child => makeInitFiles(conf, packageFolder + "/" + child.getName))
  }

  //noinspection ScalaStyle
  //scalastyle:off
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    if (!conf.pySrcDir.exists()) {
      conf.pySrcDir.mkdir()
    }
    val extraPackage = if (conf.name.endsWith("core")) {
      " + [\"mmlspark\"]"
    } else {
      ""
    }
    val requireList = if (conf.name.contains("deep-learning")) {
      s"""MINIMUM_SUPPORTED_PYTHON_VERSION = "3.8"""".stripMargin
    } else ""
    val extraRequirements = if (conf.name.contains("deep-learning")) {
      // There's `Already borrowed` error found in transformers 4.16.2 when using tokenizers
      s"""extras_require={"extras": [
         |    "cmake",
         |    "horovod==0.28.1",
         |    "pytorch_lightning>=1.5.0,<1.5.10",
         |    "torch==1.13.1",
         |    "torchvision>=0.14.1",
         |    "transformers==4.32.1",
         |    "petastorm>=0.12.0",
         |    "huggingface-hub>=0.8.1",
         |]},
         |python_requires=f">={MINIMUM_SUPPORTED_PYTHON_VERSION}",""".stripMargin
    } else ""
    writeFile(join(conf.pySrcDir, "setup.py"),
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |import os
         |from setuptools import setup, find_namespace_packages
         |import codecs
         |import os.path
         |
         |$requireList
         |
         |setup(
         |    name="${conf.name}",
         |    version="${conf.pythonizedVersion}",
         |    description="Synapse Machine Learning",
         |    long_description="SynapseML contains Microsoft's open source "
         |                     + "contributions to the Apache Spark ecosystem",
         |    license="MIT",
         |    packages=find_namespace_packages(include=['synapse.ml.*']) ${extraPackage},
         |    url="https://github.com/Microsoft/SynapseML",
         |    author="Microsoft",
         |    author_email="synapseml-support@microsoft.com",
         |    classifiers=[
         |        "Development Status :: 4 - Beta",
         |        "Intended Audience :: Developers",
         |        "Intended Audience :: Science/Research",
         |        "Topic :: Software Development :: Libraries",
         |        "License :: OSI Approved :: MIT License",
         |        "Programming Language :: Python :: 2",
         |        "Programming Language :: Python :: 3",
         |    ],
         |    zip_safe=True,
         |    package_data={"synapseml": ["../LICENSE.txt", "../README.txt"]},
         |    project_urls={
         |        "Website": "https://microsoft.github.io/SynapseML/",
         |        "Documentation": "https://mmlspark.blob.core.windows.net/docs/${conf.pythonizedVersion}/pyspark/index.html",
         |        "Source Code": "https://github.com/Microsoft/SynapseML",
         |    },
         |${indent(extraRequirements, 1)}
         |)
         |
         |""".stripMargin)
  }
  //scalastyle:on

  def pyGen(conf: CodegenConfig): Unit = {
    println(s"Generating python for ${conf.jarName}")
    clean(conf.pySrcDir)
    generatePyPackageData(conf)
    generatePythonClasses(conf)
    if (conf.pySrcOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.pySrcOverrideDir), toDir(conf.pySrcDir))
    makeInitFiles(conf)
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.pyPackageDir)
    pyGen(conf)
  }

}
