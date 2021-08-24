// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File
import com.microsoft.ml.spark.codegen.CodegenConfigProtocol._
import com.microsoft.ml.spark.core.env.FileUtilities._
import com.microsoft.ml.spark.core.test.base.TestBase
import com.microsoft.ml.spark.core.test.fuzzing.{DotnetTestFuzzing, PyTestFuzzing}
import com.microsoft.ml.spark.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._


object TestGen {

  import CodeGenUtils._

  def generatePythonTests(conf: CodegenConfig): Unit = {
    instantiateServices[PyTestFuzzing[_]](conf.jarName).foreach { ltc =>
      try {
        ltc.makePyTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  def generateDotnetTests(conf: CodegenConfig): Unit = {
    instantiateServices[DotnetTestFuzzing[_]](conf.jarName).foreach { ltc =>
      try {
        ltc.makeDotnetTestFile(conf)
      } catch {
        case _: NotImplementedError =>
          println(s"ERROR: Could not generate test for ${ltc.testClassName} because of Complex Parameters")
      }
    }
  }

  def generateDotnetHelperFile(conf: CodegenConfig): Unit = {
    if (!conf.dotnetTestDir.exists()) {
      conf.dotnetTestDir.mkdir()
    }
    writeFile(join(conf.dotnetTestDir, "mmlsparktest", "SparkFixtureHelper.cs"),
      s"""
         |// Copyright (C) Microsoft Corporation. All rights reserved.
         |// Licensed under the MIT License. See LICENSE in project root for information.
         |
         |using MMLSparktest.Utils;
         |using Xunit;
         |
         |namespace MMLSparktest.Helper
         |{
         |    [CollectionDefinition("MMLSpark Tests")]
         |    public class MMLSparkCollection: ICollectionFixture<SparkFixture>
         |    {
         |        // This class has no code, and is never created. Its purpose is simply
         |        // to be the place to apply [CollectionDefinition] and all the
         |        // ICollectionFixture<> interfaces.
         |    }
         |}
         |""".stripMargin)
  }

  def generateDotnetTestProjFile(conf: CodegenConfig): Unit = {
    if (!conf.dotnetTestDir.exists()) {
      conf.dotnetTestDir.mkdir()
    }
    writeFile(join(conf.dotnetTestDir, "mmlsparktest", "TestProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>net5.0</TargetFramework>
         |  </PropertyGroup>
         |
         |  <ItemGroup>
         |    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="16.7.1" />
         |    <PackageReference Include="MSTest.TestAdapter" Version="2.1.1" />
         |    <PackageReference Include="MSTest.TestFramework" Version="2.1.1" />
         |    <PackageReference Include="coverlet.collector" Version="1.3.0" />
         |    <PackageReference Include="xunit" Version="2.4.1" />
         |    <PackageReference Include="xunit.runner.visualstudio" Version="2.4.3">
         |      <PrivateAssets>all</PrivateAssets>
         |      <IncludeAssets>runtime; build; native; contentfiles; analyzers</IncludeAssets>
         |    </PackageReference>
         |    <PackageReference Include="Microsoft.Spark" Version="2.0.0" />
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |  </ItemGroup>
         |
         |  <ItemGroup>
         |    <ProjectReference Include="..\\..\\..\\..\\..\\..\\..\\..\\core\\src\\main\\dotnet\\dotnetBase.csproj" />
         |    <ProjectReference Include="${conf.dotnetSrcDir.getAbsolutePath}\\mmlspark\\ProjectSetup.csproj" />
         |  </ItemGroup>
         |
         |  <PropertyGroup>
         |    <InternalsAssemblyNames>Microsoft.Spark</InternalsAssemblyNames>
         |  </PropertyGroup>
         |
         |  <PropertyGroup>
         |    <InternalsAssemblyUseEmptyMethodBodies>false</InternalsAssemblyUseEmptyMethodBodies>
         |  </PropertyGroup>
         |
         |</Project>
         |""".stripMargin)
  }

  private def makeInitFiles(conf: CodegenConfig, packageFolder: String = ""): Unit = {
    val dir = new File(new File(conf.pyTestDir,  "mmlsparktest"), packageFolder)
    if (!dir.exists()){
      dir.mkdirs()
    }
    writeFile(new File(dir, "__init__.py"), "")
    dir.listFiles().filter(_.isDirectory).foreach(f =>
      makeInitFiles(conf, packageFolder + "/" + f.getName)
    )
  }


  //noinspection ScalaStyle
  def generatePyPackageData(conf: CodegenConfig): Unit = {
    if (!conf.pySrcDir.exists()) {
      conf.pySrcDir.mkdir()
    }
    writeFile(join(conf.pyTestDir,"mmlsparktest", "spark.py"),
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |from pyspark.sql import SparkSession, SQLContext
         |import os
         |import mmlspark
         |from mmlspark.core import __spark_package_version__
         |
         |spark = (SparkSession.builder
         |    .master("local[*]")
         |    .appName("PysparkTests")
         |    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:" + __spark_package_version__)
         |    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
         |    .config("spark.executor.heartbeatInterval", "60s")
         |    .config("spark.sql.shuffle.partitions", 10)
         |    .config("spark.sql.crossJoin.enabled", "true")
         |    .getOrCreate())
         |
         |sc = SQLContext(spark.sparkContext)
         |
         |""".stripMargin)
  }

  def dotnetTestMain(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.testDataDir)
    clean(conf.dotnetTestDir)
    generateDotnetTests(conf)
    TestBase.stopSparkSession()
    if (toDir(conf.dotnetTestOverrideDir).exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.dotnetTestOverrideDir), toDir(conf.dotnetTestDir))
  }


  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.testDataDir)
    clean(conf.pyTestDir)
    generatePythonTests(conf)
    TestBase.stopSparkSession()
    generatePyPackageData(conf)
    if (toDir(conf.pyTestOverrideDir).exists()){
      FileUtils.copyDirectoryToDirectory(toDir(conf.pyTestOverrideDir), toDir(conf.pyTestDir))
    }
    makeInitFiles(conf)
  }
}
