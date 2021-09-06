// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.codegen

import java.io.File
import com.microsoft.ml.spark.codegen.CodegenConfigProtocol._
import com.microsoft.ml.spark.core.env.FileUtilities._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils._
import com.microsoft.ml.spark.core.utils.JarLoadingUtils.instantiateServices
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import spray.json._


object DotnetCodegen {

  import CodeGenUtils._

  def generateDotnetClasses(conf: CodegenConfig): Unit = {
    val instantiatedClasses = instantiateServices[DotnetWrappable](conf.jarName)
    instantiatedClasses.foreach { w =>
      println(w.getClass.getName)
      w.makeDotnetFile(conf)
    }
  }

  //noinspection ScalaStyle
  def generateDotnetProjFile(conf: CodegenConfig): Unit = {
    if (!conf.dotnetSrcDir.exists()) {
      conf.dotnetSrcDir.mkdir()
    }
    val curName = conf.name.split("-".toCharArray).drop(1).mkString("-")
    val packageName = curName match {
      case "deep-learning" => "deepLearning"
      case s => s.capitalize
    }
    val dotnetBasePath = join(conf.dotnetSrcDir, "helper", "dotnetBase.csproj").toString
      .replaceAllLiterally(curName, "core")
    writeFile(new File(join(conf.dotnetSrcDir, "mmlspark"), s"${packageName}ProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>net5.0</TargetFramework>
         |    <LangVersion>9.0</LangVersion>
         |    <RootNamespace>MMLSpark.$packageName</RootNamespace>
         |  </PropertyGroup>
         |
         |  <ItemGroup>
         |    <PackageReference Include="Microsoft.Spark" Version="2.0.0" />
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |  </ItemGroup>
         |
         |  <ItemGroup>
         |    <ProjectReference Include="$dotnetBasePath" />
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
         |
         |""".stripMargin)
  }

  def dotnetGen(conf: CodegenConfig): Unit = {
    println(s"Generating dotnet for ${conf.jarName}")
    clean(conf.dotnetSrcDir)
    generateDotnetClasses(conf)
    if (conf.dotnetSrcOverrideDir.exists())
      FileUtils.copyDirectoryToDirectory(toDir(conf.dotnetSrcOverrideDir), toDir(conf.dotnetSrcHelperDir))
    generateDotnetProjFile(conf)
  }

  def main(args: Array[String]): Unit = {
    val conf = args.head.parseJson.convertTo[CodegenConfig]
    clean(conf.dotnetPackageDir)
    dotnetGen(conf)
  }

}

