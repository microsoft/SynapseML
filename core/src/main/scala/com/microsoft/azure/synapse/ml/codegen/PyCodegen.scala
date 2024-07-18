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
    val importStrings = if (packageFolder == "/services") {
      dir.listFiles.filter(_.isDirectory)
        .filter(folder => folder.getName != "langchain").sorted
        .map(folder => s"from synapse.ml$packageString.${folder.getName} import *\n").mkString("")
    } else {
      dir.listFiles.filter(_.isFile).sorted
        .map(_.getName)
        .filter(name => name.endsWith(".py") && !name.startsWith("_") && !name.startsWith("test"))
        .map(name => s"from synapse.ml$packageString.${getBaseName(name)} import *\n").mkString("")
    }
    val initFile = new File(dir, "__init__.py")
    if (packageFolder != "/cognitive"){
      if (packageFolder != "") {
        writeFile(initFile, conf.packageHelp(importStrings))
      } else if (initFile.exists()) {
        initFile.delete()
      }
    }
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
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
