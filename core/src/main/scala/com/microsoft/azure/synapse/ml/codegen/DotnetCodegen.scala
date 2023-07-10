// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.codegen.CodegenConfigProtocol._
import com.microsoft.azure.synapse.ml.core.env.FileUtilities._
import com.microsoft.azure.synapse.ml.core.utils.JarLoadingUtils.instantiateServices
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.File


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
    val curProject = conf.name.split("-").drop(1).map(s => s.capitalize).mkString("")
    val projectDir = join(conf.dotnetSrcDir, "synapse", "ml")
    if (!projectDir.exists()){
      projectDir.mkdirs()
    }
    val newtonsoftDep = if(curProject == "DeepLearning") {
      s"""<PackageReference Include="Newtonsoft.Json" Version="13.0.1" />""".stripMargin
    } else ""
    // TODO: update SynapseML.DotnetBase version whenever we upload a new one
    writeFile(new File(projectDir, s"${curProject}ProjectSetup.csproj"),
      s"""<Project Sdk="Microsoft.NET.Sdk">
         |
         |  <PropertyGroup>
         |    <TargetFramework>netstandard2.1</TargetFramework>
         |    <LangVersion>9.0</LangVersion>
         |    <AssemblyName>SynapseML.$curProject</AssemblyName>
         |    <IsPackable>true</IsPackable>
         |    <GenerateDocumentationFile>true</GenerateDocumentationFile>
         |    <Description>.NET for SynapseML.$curProject</Description>
         |    <Version>${conf.dotnetVersion}</Version>
         |  </PropertyGroup>
         |
         |  <ItemGroup>
         |    <PackageReference Include="Microsoft.Spark" Version="2.1.1" />
         |    <PackageReference Include="SynapseML.DotnetBase" Version="0.11.2" />
         |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
         |    $newtonsoftDep
         |  </ItemGroup>
         |
         |  <ItemGroup>
         |    <InternalsVisibleTo Include="SynapseML.$curProject.Test" />
         |  </ItemGroup>
         |
         |  <PropertyGroup>
         |    <InternalsAssemblyNames>Microsoft.Spark;SynapseML.DotnetBase</InternalsAssemblyNames>
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
