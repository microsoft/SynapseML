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

  private val OpenAICompletionStubImport: String =
    """from synapse.ml.services.openai.OpenAICompletion import OpenAICompletion as OpenAICompletion
      |""".stripMargin

  private val StubSymbolPattern = """(?m)^(?:class|def)\s+([A-Za-z_][A-Za-z0-9_]*)\b""".r

  private def isOpenAICompletionStub(packageFolder: String, fileName: String): Boolean =
    packageFolder == "/services/openai" && fileName == DeprecatedOpenAICompletionFile

  private def initFileExtra(packageFolder: String): String =
    if (packageFolder == "/services/openai") OpenAICompletionImportHook else ""

  private def initStubExtra(packageFolder: String): String =
    if (packageFolder == "/services/openai") OpenAICompletionStubImport else ""

  private def hasManualInit(conf: CodegenConfig, packageFolder: String): Boolean = {
    val manualInit = join(conf.pySrcOverrideDir, "synapse", "ml", packageFolder, "__init__.py")
    manualInit.isFile && readFile(manualInit).trim.nonEmpty
  }

  private def stubModuleImports(
      dir: File,
      packageFolder: String,
      packageString: String): String = {
    val stubFiles = dir.listFiles.filter(file =>
      file.isFile &&
        file.getName.endsWith(".pyi") &&
        file.getName != "__init__.pyi" &&
        !file.getName.startsWith("_")
    ).sorted
    val stubModules = stubFiles.map(file => getBaseName(file.getName)).toSet
    val explicitImports = stubFiles.flatMap { file =>
      val moduleName = getBaseName(file.getName)
      val modulePath = s"synapse.ml$packageString.$moduleName"
      StubSymbolPattern.findAllMatchIn(readFile(file)).map(_.group(1))
        .filterNot(_.startsWith("_"))
        .map(symbol => s"from $modulePath import $symbol as $symbol\n")
    }
    val fallbackImports = dir.listFiles.filter(file =>
      file.isFile &&
        file.getName.endsWith(".py") &&
        file.getName != "__init__.py" &&
        !file.getName.startsWith("_") &&
        !file.getName.startsWith("test") &&
        !stubModules.contains(getBaseName(file.getName))
    ).sorted
      .filterNot(file => isOpenAICompletionStub(packageFolder, file.getName))
      .map(file => s"from synapse.ml$packageString.${getBaseName(file.getName)} import *\n")
    (explicitImports ++ fallbackImports).mkString("")
  }

  private def makeInitStub(
      conf: CodegenConfig,
      dir: File,
      packageFolder: String,
      packageString: String): Unit = {
    if (packageFolder.nonEmpty && !hasManualInit(conf, packageFolder)) {
      val importStrings = if (packageFolder == "/services") {
        dir.listFiles.filter(_.isDirectory)
          .filter(folder => folder.getName != "langchain").sorted
          .map(folder => s"from synapse.ml$packageString.${folder.getName} import *\n").mkString("")
      } else {
        stubModuleImports(dir, packageFolder, packageString)
      }
      writeFile(new File(dir, "__init__.pyi"), importStrings + initStubExtra(packageFolder))
    }
  }

  def generatePythonClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[PythonWrappable](conf.jarName)
    instantiatedClasses.foreach { w =>
      println(w.getClass.getName)
      w.makePyFile(conf)
    }
  }

  private def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = join(conf.pySrcDir, "synapse", "ml", packageFolder)
    val packageString = if (packageFolder != "") packageFolder.replace("/", ".") else ""
    val importStrings = if (packageFolder == "/services") {
      dir.listFiles.filter(_.isDirectory)
        .filter(folder => folder.getName != "langchain").sorted
        .map(folder => s"from synapse.ml$packageString.${folder.getName} import *\n").mkString("")
    } else {
      dir.listFiles.filter(_.isFile).sorted
        .map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .filterNot(name => isOpenAICompletionStub(packageFolder, name))
        .map(name => s"from synapse.ml$packageString.${getBaseName(name)} import *\n").mkString("")
    }
    val initFile = new File(dir, "__init__.py")
    if (packageFolder != "/cognitive"){
      if (packageFolder != "") {
        writeFile(initFile, conf.packageHelp(importStrings) + initFileExtra(packageFolder))
      } else if (initFile.exists()) {
        initFile.delete()
      }
    }
    makeInitStub(conf, dir, packageFolder, packageString)
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }

  private def containsTypeStub(dir: File): Boolean =
    Option(dir.listFiles()).exists(_.exists { file =>
      (file.isFile && file.getName.endsWith(".pyi")) ||
        (file.isDirectory && containsTypeStub(file))
    })

  private def generateTypingMarkers(conf: CodegenConfig): Unit = {
    val namespaceRoot = join(conf.pySrcDir, "synapse", "ml")
    Option(namespaceRoot.listFiles()).getOrElse(Array.empty)
      .filter(_.isDirectory)
      .filter(containsTypeStub)
      .foreach(dir => writeFile(join(dir, "py.typed"), ""))
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
         |    license_expression="MIT",
         |    packages=find_namespace_packages(include=['synapse.ml', 'synapse.ml.*']) ${extraPackage},
         |    url="https://github.com/Microsoft/SynapseML",
         |    author="Microsoft",
         |    author_email="synapseml-support@microsoft.com",
         |    classifiers=[
         |        "Development Status :: 4 - Beta",
         |        "Intended Audience :: Developers",
         |        "Intended Audience :: Science/Research",
         |        "Topic :: Software Development :: Libraries",
         |        "Programming Language :: Python :: 3",
         |    ],
         |    zip_safe=True,
         |    package_data={
         |        "": ["*.pyi", "py.typed"],
         |        "synapseml": ["../LICENSE.txt", "../README.txt"],
         |    },
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

  private[codegen] def generateInitFiles(conf: CodegenConfig): Unit = {
    generateTypingMarkers(conf)
    makeInitFiles(conf)
    PythonInitMerger.preserve(conf)
  }

  def pyGen(conf: CodegenConfig): Unit = {
    println(s"Generating python for ${conf.jarName}")
    clean(conf.pySrcDir)
    generatePyPackageData(conf)
    generatePythonClasses(conf)
    if (conf.pySrcOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.pySrcOverrideDir), toDir(conf.pySrcDir))
    generateInitFiles(conf)
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.pyPackageDir)
    pyGen(conf)
  }

}
