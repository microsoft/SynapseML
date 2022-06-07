// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import java.io.File
import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{DotnetTestFuzzing, PyTestFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
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
    val dir = new File(conf.dotnetTestDir,  "SynapseMLtest")
    if (!dir.exists()){
      dir.mkdirs()
    }
    writeFile(new File(dir, "SparkFixtureHelper.cs"),
      s"""
         |// Copyright (C) Microsoft Corporation. All rights reserved.
         |// Licensed under the MIT License. See LICENSE in project root for information.
         |
         |using SynapseMLtest.Utils;
         |using Xunit;
         |
         |namespace SynapseMLtest.Helper
         |{
         |    [CollectionDefinition("SynapseML Tests")]
         |    public class SynapseMLCollection: ICollectionFixture<SparkFixture>
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
    val dir = new File(conf.dotnetTestDir,  "SynapseMLtest")
    if (!dir.exists()){
      dir.mkdirs()
    }
    val curName = conf.name.split("-".toCharArray).drop(1).mkString("-")
    val curProject = curName match {
      case "deep-learning" => "deepLearning"
      case s => s
    }
    // TODO: upload dotnetTestBase to blob and reference it
    val dotnetBasePath = join(conf.dotnetSrcDir, "helper", "src", "dotnetBase.csproj").toString
      .replaceAllLiterally(curName, "core")
    val dotnetTestBasePath = join(conf.dotnetSrcDir, "helper", "test", "dotnetTestBase.csproj").toString
      .replaceAllLiterally(curName, "core")
    val curPath = conf.dotnetSrcDir.getAbsolutePath
    val corePath = curPath.replace(curProject, "core")
    val referenceCore = conf.name match {
      case "synapseml-opencv" =>
        s"""<ProjectReference Include="$corePath\\synapse\\ml\\CoreProjectSetup.csproj" />"""
      case _ => ""
    }
    writeFile(new File(dir, "TestProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>net5.0</TargetFramework>
         |    <LangVersion>9.0</LangVersion>
         |    <AssemblyName>SynapseML.$curProject.Test</AssemblyName>
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
         |    <PackageReference Include="Microsoft.Spark" Version="2.1.1" />
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |  </ItemGroup>
         |
         |  <ItemGroup>
         |    <ProjectReference Include="$curPath\\synapse\\ml\\${curProject.capitalize}ProjectSetup.csproj" />
         |    <ProjectReference Include="$dotnetBasePath" PrivateAssets="All" />
         |    <ProjectReference Include="$dotnetTestBasePath" PrivateAssets="All" />
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
