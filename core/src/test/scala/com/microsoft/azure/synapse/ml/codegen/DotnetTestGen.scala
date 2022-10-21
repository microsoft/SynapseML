// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.DotnetTestFuzzing
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.File
import java.nio.file.Path


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
    val dir = new File(conf.dotnetTestDir, "SynapseMLtest")
    if (!dir.exists()) {
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
    val dir = new File(conf.dotnetTestDir, "SynapseMLtest")
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val curProject = conf.name.split("-").drop(1).map(s => s.capitalize).mkString("")
    // TODO: update SynapseML.DotnetBase version whenever we upload a new one
    val referenceCore = conf.name match {
      case "synapseml-opencv" | "synapseml-deep-learning" =>
        s"""<PackageReference Include="SynapseML.Core" Version="${conf.dotnetVersion}" />"""
      case _ => ""
    }
    // scalastyle:off line.size.limit
    writeFile(new File(dir, "TestProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>netcoreapp3.1</TargetFramework>
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
         |    <PackageReference Include="SynapseML.DotnetBase" Version="0.9.1" />
         |    <PackageReference Include="SynapseML.DotnetE2ETest" Version="${conf.dotnetVersion}" />
         |    <PackageReference Include="SynapseML.$curProject" Version="${conf.dotnetVersion}" />
         |    $referenceCore
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |  </ItemGroup>
         |
         |  <PropertyGroup>
         |    <InternalsAssemblyNames>Microsoft.Spark;SynapseML.DotnetBase;SynapseML.DotnetE2ETest;SynapseML.$curProject</InternalsAssemblyNames>
         |  </PropertyGroup>
         |
         |  <PropertyGroup>
         |    <InternalsAssemblyUseEmptyMethodBodies>false</InternalsAssemblyUseEmptyMethodBodies>
         |  </PropertyGroup>
         |
         |  <ItemGroup>
         |    <None Include="Resources/log4j.properties" CopyToOutputDirectory="PreserveNewest" />
         |  </ItemGroup>
         |</Project>
         |""".stripMargin, StandardOpenOption.CREATE)
    // scalastyle:on line.size.limit
  }

  def generateLog4jPropertiesFile(conf: CodegenConfig): Unit = {
    val dir = join(conf.dotnetTestDir, "SynapseMLtest", "Resources")
    if (!dir.exists()) {
      dir.mkdirs()
    }
    writeFile(new File(dir, "log4j.properties"),
      s"""log4j.appender.stdout=org.apache.log4j.ConsoleAppender
         |log4j.appender.stdout.Target=System.out
         |log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
         |log4j.appender.stdout.layout.ConversionPattern=%d{HH:mm:ss} %-5p %c{1}:%L - %m%n
         |
         |log4j.rootLogger=WARN, stdout
         |log4j.logger.org.apache.spark=WARN, stdout
         |log4j.logger.com.microsoft=INFO, stdout
         |""".stripMargin, StandardOpenOption.CREATE)
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
      generateLog4jPropertiesFile(conf)
    }
}
