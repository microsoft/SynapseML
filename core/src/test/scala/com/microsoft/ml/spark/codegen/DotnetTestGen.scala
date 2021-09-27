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


object DotnetTestGen {

  import CodeGenUtils._

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
    val dir = new File(conf.dotnetTestDir,  "MMLSparktest")
    if (!dir.exists()){
      dir.mkdirs()
    }
    writeFile(new File(dir, "SparkFixtureHelper.cs"),
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

  // noinspection ScalaStyle
  def generateDotnetTestProjFile(conf: CodegenConfig): Unit = {
    val dir = new File(conf.dotnetTestDir,  "MMLSparktest")
    if (!dir.exists()){
      dir.mkdirs()
    }
    val curName = conf.name.split("-".toCharArray).drop(1).mkString("-")
    val curProject = curName match {
      case "deep-learning" => "deepLearning"
      case s => s
    }
    val dotnetBasePath = join(conf.dotnetSrcDir, "helper", "dotnetBase.csproj").toString
      .replaceAllLiterally(curName, "core")
    val curPath = conf.dotnetSrcDir.getAbsolutePath
    val corePath = curPath.replace(curProject, "core")
    val referenceCore = conf.name match {
      case "mmlspark-opencv" =>
        s"""<ProjectReference Include="$corePath\\mmlspark\\CoreProjectSetup.csproj" />"""
      case _ => ""
    }
    writeFile(new File(dir, "TestProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>net5.0</TargetFramework>
         |    <LangVersion>9.0</LangVersion>
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
         |    <PackageReference Include="XunitXml.TestLogger" Version="3.0.66" />
         |    <PackageReference Include="Microsoft.Spark" Version="2.0.0" />
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |  </ItemGroup>
         |
         |  <ItemGroup>
         |    <ProjectReference Include="$dotnetBasePath" />
         |    <ProjectReference Include="$curPath\\mmlspark\\${curProject.capitalize}ProjectSetup.csproj" />
         |    $referenceCore
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

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.dotnetTestDataDir)
    clean(conf.dotnetTestDir)
    generateDotnetTests(conf)
    TestBase.stopSparkSession()
    if (toDir(conf.dotnetTestOverrideDir).exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.dotnetTestOverrideDir), toDir(conf.dotnetTestDir))
    generateDotnetTestProjFile(conf)
    generateDotnetHelperFile(conf)
  }
}
