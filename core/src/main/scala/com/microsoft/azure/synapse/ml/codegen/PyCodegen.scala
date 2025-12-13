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
    val importStrings = buildImportStrings(dir, packageFolder, packageString)
    val initFile = new File(dir, "__init__.py")
    if (packageFolder != "/cognitive"){
      val patchedImportStrings = patchImportStrings(packageFolder, importStrings)
      persistInitFile(conf, packageFolder, initFile, patchedImportStrings)
    }
    safeListFiles(dir).filter(_.isDirectory).foreach { f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    }
  }

  private def buildImportStrings(dir: File, packageFolder: String, packageString: String): String = {
    if (packageFolder == "/services") {
      serviceImports(dir, packageString)
    } else {
      standardImports(dir, packageFolder, packageString)
    }
  }

  private def serviceImports(dir: File, packageString: String): String = {
    safeListFiles(dir)
      .filter(_.isDirectory)
      .filter(_.getName != "langchain")
      .sortBy(_.getName)
      .map(folder => s"from synapse.ml$packageString.${folder.getName} import *\n")
      .mkString("")
  }

  private def standardImports(dir: File, packageFolder: String, packageString: String): String = {
    val files = safeListFiles(dir).filter(_.isFile).map(_.getName)
    val baseImports = files.sorted
      .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
      .map(name => s"from synapse.ml$packageString.${getBaseName(name)} import *\n")
    val iceImport =
      if (needsIceImport(packageFolder, files)) Seq(s"from synapse.ml$packageString.ICETransformer import *\n")
      else Seq.empty
    (iceImport ++ baseImports).mkString("")
  }

  private def needsIceImport(packageFolder: String, files: Seq[String]): Boolean = {
    // In 1.1.0 the explainers package surface always includes ICETransformer
    // alongside the LIME/SHAP variants. When generating per-module wheels,
    // some modules (e.g., deep-learning) only see the LIME/SHAP wrappers
    // locally but rely on the core wheel to supply ICETransformer.py.
    // Ensure the generated __init__ for /explainers keeps exporting ICETransformer
    // so imports like `from synapse.ml.explainers import ICETransformer` continue
    // to resolve to the class rather than the module.
    packageFolder == "/explainers" && !files.contains("ICETransformer.py")
  }

  private def patchImportStrings(packageFolder: String, importStrings: String): String = {
    if (packageFolder == "/core") {
      s"from synapse.ml.core.serialize import java_params_patch as _java_params_patch\n$importStrings"
    } else {
      importStrings
    }
  }

  private def persistInitFile(conf: CodegenConfig,
                              packageFolder: String,
                              initFile: File,
                              contents: String): Unit = {
    if (packageFolder.nonEmpty) {
      writeFile(initFile, conf.packageHelp(contents))
    } else if (initFile.exists()) {
      initFile.delete()
    }
  }

  private def safeListFiles(dir: File): Seq[File] = {
    Option(dir.listFiles).map(_.toSeq).getOrElse(Seq.empty)
  }

  //noinspection ScalaStyle
  //scalastyle:off
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    if (!conf.pySrcDir.exists()) {
      conf.pySrcDir.mkdirs()
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
      s"""extras_require={"extras": [
         |    "cmake",
         |    "horovod @ git+https://github.com/horovod/horovod.git@3a31d933a13c7c885b8a673f4172b17914ad334d",
         |    "pytorch_lightning==1.5.0",
         |    "torch==2.2.0",
         |    "torchvision==0.17.0",
         |    "transformers==4.49.0",
         |    "petastorm>=0.12.0",
         |    "huggingface-hub==0.26.0",
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
    val json = if (args.head.startsWith("@")) {
      val source = scala.io.Source.fromFile(args.head.substring(1))
      try source.mkString finally source.close()
    } else {
      args.head
    }
    val conf = json.parseJson.convertTo[CodegenConfig]
    clean(conf.pyPackageDir)
    pyGen(conf)
  }

}
