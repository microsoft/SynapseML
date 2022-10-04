import BuildUtils._
import org.apache.commons.io.FileUtils
import sbt.ExclusionRule

import java.io.File
import java.net.URL
import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq, _}

val condaEnvName = "synapseml"
val sparkVersion = "3.3.1"
name := "synapseml"
ThisBuild / organization := "com.microsoft.azure"
ThisBuild / scalaVersion := "2.12.15"

val scalaMajorVersion = 2.12

val excludes = Seq(
  ExclusionRule("org.apache.spark", s"spark-tags_$scalaMajorVersion"),
  ExclusionRule("org.scalatest")
)

val coreDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-tags" % sparkVersion % "test",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test")
val extraDependencies = Seq(
  "org.scalactic" %% "scalactic" % "3.2.14",
  "io.spray" %% "spray-json" % "1.3.5",
  "com.jcraft" % "jsch" % "0.1.54",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.1.3",
  "org.apache.httpcomponents" % "httpmime" % "4.5.13",
  "com.linkedin.isolation-forest" %% "isolation-forest_3.2.0" % "2.0.8",
  // Although breeze 1.2 is already provided by Spark, this is needed for Azure Synapse Spark 3.2 pools.
  // Otherwise a NoSuchMethodError will be thrown by interpretability code. This problem only happens
  // to Azure Synapse Spark 3.2 pools.
  "org.scalanlp" %% "breeze" % "1.2"
).map(d => d excludeAll (excludes: _*))
val dependencies = coreDependencies ++ extraDependencies

def txt(e: Elem, label: String): String = "\"" + e.child.filter(_.label == label).flatMap(_.text).mkString + "\""

val omittedDeps = Set(s"spark-core_$scalaMajorVersion", s"spark-mllib_$scalaMajorVersion", "org.scala-lang")
// skip dependency elements with a scope

def pomPostFunc(node: XmlNode): scala.xml.Node = {
  new RuleTransformer(new RewriteRule {
    override def transform(node: XmlNode): XmlNodeSeq = node match {
      case e: Elem if e.label == "extraDependencyAttributes" =>
        Comment("Removed Dependency Attributes")
      case e: Elem if e.label == "dependency"
        && e.child.exists(child => child.label == "scope") =>
        Comment(
          s""" scoped dependency ${txt(e, "groupId")} % ${txt(e, "artifactId")}
             |% ${txt(e, "version")} % ${txt(e, "scope")} has been omitted """.stripMargin)
      case e: Elem if e.label == "dependency"
        && e.child.exists(child => omittedDeps(child.text)) =>
        Comment(
          s""" excluded dependency ${txt(e, "groupId")} % ${txt(e, "artifactId")}
             |% ${txt(e, "version")} has been omitted """.stripMargin)
      case _ => node
    }
  }).transform(node).head
}

pomPostProcess := pomPostFunc

val getDatasetsTask = TaskKey[Unit]("getDatasets", "download datasets used for testing")
val datasetName = "datasets-2022-08-09.tgz"
val datasetUrl = new URL(s"https://mmlspark.blob.core.windows.net/installers/$datasetName")
val datasetDir = settingKey[File]("The directory that holds the dataset")
ThisBuild / datasetDir := {
  join((Compile / packageBin / artifactPath).value.getParentFile,
    "datasets", datasetName.split(".".toCharArray.head).head)
}

getDatasetsTask := {
  val d = datasetDir.value.getParentFile
  val f = new File(d, datasetName)
  if (!d.exists()) d.mkdirs()
  if (!f.exists()) {
    FileUtils.copyURLToFile(datasetUrl, f)
    UnzipUtils.unzip(f, d)
  }
}

val genBuildInfo = TaskKey[Unit]("genBuildInfo", "generate a build info file")
genBuildInfo := {
  val docInfo =
    s"""
       |
       |### Documentation Pages:
       |[Scala Documentation](https://mmlspark.blob.core.windows.net/docs/${version.value}/scala/index.html)
       |[Python Documentation](https://mmlspark.blob.core.windows.net/docs/${version.value}/pyspark/index.html)
       |
    """.stripMargin
  val buildInfo = (root / blobArtifactInfo).value + docInfo
  val infoFile = join("target", "Build.md")
  if (infoFile.exists()) FileUtils.forceDelete(infoFile)
  FileUtils.writeStringToFile(infoFile, buildInfo, "utf-8")
}

val rootGenDir = SettingKey[File]("rootGenDir")
rootGenDir := {
  val targetDir = (root / Compile / packageBin / artifactPath).value.getParentFile
  join(targetDir, "generated")
}

// scalastyle:off line.size.limit
val genSleetConfig = TaskKey[File]("genSleetConfig",
  "generate sleet.json file for sleet configuration so we can push nuget package to the blob")
genSleetConfig := {
  val fileContent =
    s"""{
       |  "username": "",
       |  "useremail": "",
       |  "sources": [
       |    {
       |      "name": "SynapseMLNuget",
       |      "type": "azure",
       |      "container": "synapsemlnuget",
       |      "path": "https://mmlspark.blob.core.windows.net/synapsemlnuget",
       |      "connectionString": "DefaultEndpointsProtocol=https;AccountName=mmlspark;AccountKey=${Secrets.storageKey};EndpointSuffix=core.windows.net"
       |    }
       |  ]
       |}""".stripMargin
  val sleetJsonFile = join(rootGenDir.value, "sleet.json")
  if (sleetJsonFile.exists()) FileUtils.forceDelete(sleetJsonFile)
  FileUtils.writeStringToFile(sleetJsonFile, fileContent, "utf-8")
  sleetJsonFile
}
// scalastyle:on line.size.limit

val publishDotnetTestBase = TaskKey[Unit]("publishDotnetTestBase",
  "generate dotnet test helper file with current library version and publish E2E test base")
publishDotnetTestBase := {
  val fileContent =
    s"""// Licensed to the .NET Foundation under one or more agreements.
       |// The .NET Foundation licenses this file to you under the MIT license.
       |// See the LICENSE file in the project root for more information.
       |
       |namespace SynapseMLtest.Utils
       |{
       |    public class Helper
       |    {
       |        public static string GetSynapseMLPackage()
       |        {
       |            return "com.microsoft.azure:synapseml_2.12:${version.value}";
       |        }
       |    }
       |
       |}
       |""".stripMargin
  val dotnetTestBaseDir = join(baseDirectory.value, "core", "src", "main", "dotnet", "test")
  val dotnetHelperFile = join(dotnetTestBaseDir, "SynapseMLVersion.cs")
  if (dotnetHelperFile.exists()) FileUtils.forceDelete(dotnetHelperFile)
  FileUtils.writeStringToFile(dotnetHelperFile, fileContent, "utf-8")

  val dotnetTestBaseProjContent =
    s"""<Project Sdk="Microsoft.NET.Sdk">
       |
       |  <PropertyGroup>
       |    <TargetFramework>netstandard2.1</TargetFramework>
       |    <LangVersion>9.0</LangVersion>
       |    <AssemblyName>SynapseML.DotnetE2ETest</AssemblyName>
       |    <IsPackable>true</IsPackable>
       |    <Description>SynapseML .NET Test Base</Description>
       |    <Version>${dotnetedVersion(version.value)}</Version>
       |  </PropertyGroup>
       |
       |  <ItemGroup>
       |    <PackageReference Include="xunit" Version="2.4.1" />
       |    <PackageReference Include="Microsoft.Spark" Version="2.1.1" />
       |    <PackageReference Include="IgnoresAccessChecksToGenerator" Version="0.4.0" PrivateAssets="All" />
       |  </ItemGroup>
       |
       |  <ItemGroup>
       |    <InternalsVisibleTo Include="SynapseML.Cognitive" />
       |    <InternalsVisibleTo Include="SynapseML.Core" />
       |    <InternalsVisibleTo Include="SynapseML.DeepLearning" />
       |    <InternalsVisibleTo Include="SynapseML.Lightgbm" />
       |    <InternalsVisibleTo Include="SynapseML.Opencv" />
       |    <InternalsVisibleTo Include="SynapseML.Vw" />
       |    <InternalsVisibleTo Include="SynapseML.Cognitive.Test" />
       |    <InternalsVisibleTo Include="SynapseML.Core.Test" />
       |    <InternalsVisibleTo Include="SynapseML.DeepLearning.Test" />
       |    <InternalsVisibleTo Include="SynapseML.Lightgbm.Test" />
       |    <InternalsVisibleTo Include="SynapseML.Opencv.Test" />
       |    <InternalsVisibleTo Include="SynapseML.Vw.Test" />
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
       |</Project>""".stripMargin
  // update the version of current dotnetTestBase assembly
  val dotnetTestBaseProj = join(dotnetTestBaseDir, "dotnetTestBase.csproj")
  if (dotnetTestBaseProj.exists()) FileUtils.forceDelete(dotnetTestBaseProj)
  FileUtils.writeStringToFile(dotnetTestBaseProj, dotnetTestBaseProjContent, "utf-8")

  packDotnetAssemblyCmd(join(dotnetTestBaseDir, "target").getAbsolutePath, dotnetTestBaseDir)
  val packagePath = join(dotnetTestBaseDir,
    "target", s"SynapseML.DotnetE2ETest.${dotnetedVersion(version.value)}.nupkg").getAbsolutePath
  publishDotnetAssemblyCmd(packagePath, genSleetConfig.value)
}

// This command should be run only when you make an update to DotnetBase proj, and it will override
// existing nuget package with the same version number
val publishDotnetBase = TaskKey[Unit]("publishDotnetBase",
  "publish dotnet base nuget package that contains core elements for SynapseML in C#")
publishDotnetBase := {
  val dotnetBaseDir = join(baseDirectory.value, "core", "src", "main", "dotnet", "src")
  packDotnetAssemblyCmd(join(dotnetBaseDir, "target").getAbsolutePath, dotnetBaseDir)
  val packagePath = join(dotnetBaseDir,
    // Update the version whenever there's a new release
    "target", s"SynapseML.DotnetBase.${dotnetedVersion("0.11.0")}.nupkg").getAbsolutePath
  publishDotnetAssemblyCmd(packagePath, genSleetConfig.value)
}

def runTaskForAllInCompile(task: TaskKey[Unit]): Def.Initialize[Task[Seq[Unit]]] = {
  task.all(ScopeFilter(
    inProjects(core, deepLearning, cognitive, vw, lightgbm, opencv),
    inConfigurations(Compile))
  )
}

val generatePythonDoc = TaskKey[Unit]("generatePythonDoc", "Generate sphinx docs for python")
generatePythonDoc := {
  runTaskForAllInCompile(installPipPackage).value
  runTaskForAllInCompile(mergePyCode).value
  val dir = join(rootGenDir.value, "src", "python", "synapse")
  join(dir, "__init__.py").createNewFile()
  join(dir, "ml", "__init__.py").createNewFile()
  runCmd(activateCondaEnv ++ Seq("sphinx-apidoc", "-f", "-o", "doc", "."), dir)
  runCmd(activateCondaEnv ++ Seq("sphinx-build", "-b", "html", "doc", "../../../doc/pyspark"), dir)
}

val generateDotnetDoc = TaskKey[Unit]("generateDotnetDoc", "Generate documentation for dotnet classes")
generateDotnetDoc := {
  Def.sequential(
    runTaskForAllInCompile(dotnetCodeGen),
    runTaskForAllInCompile(mergeDotnetCode)
  ).value
  val dotnetSrcDir = join(rootGenDir.value, "src", "dotnet")
  runCmd(Seq("doxygen", "-g"), dotnetSrcDir)
  FileUtils.copyFile(join(baseDirectory.value, "README.md"), join(dotnetSrcDir, "README.md"))
  runCmd(Seq("sed", "-i", s"""s/img width=\"800\"/img width=\"300\"/g""", "README.md"), dotnetSrcDir)
  val packageName = name.value.split("-").map(_.capitalize).mkString(" ")
  val fileContent =
    s"""PROJECT_NAME = "$packageName"
       |PROJECT_NUMBER = "${dotnetedVersion(version.value)}"
       |USE_MDFILE_AS_MAINPAGE = "README.md"
       |RECURSIVE = YES
       |""".stripMargin
  val doxygenHelperFile = join(dotnetSrcDir, "DoxygenHelper.txt")
  if (doxygenHelperFile.exists()) FileUtils.forceDelete(doxygenHelperFile)
  FileUtils.writeStringToFile(doxygenHelperFile, fileContent, "utf-8")
  runCmd(Seq("bash", "-c", "cat DoxygenHelper.txt >> Doxyfile", ""), dotnetSrcDir)
  runCmd(Seq("doxygen"), dotnetSrcDir)
}

val packageSynapseML = TaskKey[Unit]("packageSynapseML", "package all projects into SynapseML")
packageSynapseML := {
  def writeSetupFileToTarget(dir: File): Unit = {
    if (!dir.exists()) {
      dir.mkdir()
    }
    val content =
      s"""
         |# Copyright (C) Microsoft Corporation. All rights reserved.
         |# Licensed under the MIT License. See LICENSE in project root for information.
         |
         |import os
         |from setuptools import setup, find_namespace_packages
         |import codecs
         |import os.path
         |
         |setup(
         |    name="synapseml",
         |    version="${pythonizedVersion(version.value)}",
         |    description="Synapse Machine Learning",
         |    long_description="SynapseML contains Microsoft's open source "
         |                     + "contributions to the Apache Spark ecosystem",
         |    license="MIT",
         |    packages=find_namespace_packages(include=['synapse.ml.*']),
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
         |)
         |
         |""".stripMargin
    IO.write(join(dir, "setup.py"), content)
  }

  Def.sequential(
    runTaskForAllInCompile(packagePython),
    runTaskForAllInCompile(mergePyCode)
  ).value
  val targetDir = rootGenDir.value
  val dir = join(targetDir, "src", "python")
  val packageDir = join(targetDir, "package", "python").absolutePath
  writeSetupFileToTarget(dir)
  packagePythonWheelCmd(packageDir, dir)
}

val publishPypi = TaskKey[Unit]("publishPypi", "publish synapseml python wheel to pypi")
publishPypi := {
  packageSynapseML.value
  val fn = s"${name.value}-${pythonizedVersion(version.value)}-py2.py3-none-any.whl"
  runCmd(
    activateCondaEnv ++
      Seq("twine", "upload", "--skip-existing",
        join(rootGenDir.value, "package", "python", fn).toString,
        "--username", "__token__", "--password", Secrets.pypiApiToken, "--verbose")
  )
}

val publishDocs = TaskKey[Unit]("publishDocs", "publish docs for scala, python and dotnet")
publishDocs := {
  Def.sequential(
    generatePythonDoc,
    generateDotnetDoc,
    (root / Compile / unidoc)
  ).value
  val html =
    """
      |<html><body><pre style="font-size: 150%;">
      |<a href="pyspark/index.html">pyspark/</u>
      |<a href="scala/index.html">scala/</u>
      |</pre></body></html>
    """.stripMargin
  val targetDir = (root / Compile / packageBin / artifactPath).value.getParentFile
  val codegenDir = join(targetDir, "generated")
  val unifiedDocDir = join(codegenDir, "doc")
  val scalaDir = join(unifiedDocDir.toString, "scala")
  if (scalaDir.exists()) FileUtils.forceDelete(scalaDir)
  FileUtils.copyDirectory(join(targetDir, "unidoc"), scalaDir)
  FileUtils.writeStringToFile(join(unifiedDocDir.toString, "index.html"), html, "utf-8")
  val dotnetDir = join(unifiedDocDir.toString, "dotnet")
  if (dotnetDir.exists()) FileUtils.forceDelete(dotnetDir)
  FileUtils.copyDirectory(join(codegenDir, "src", "dotnet", "html"), dotnetDir)
  uploadToBlob(unifiedDocDir.toString, version.value, "docs")
}

val publishBadges = TaskKey[Unit]("publishBadges", "publish badges to synapseml blob")
publishBadges := {
  def enc(s: String): String = {
    s.replaceAllLiterally("_", "__").replaceAllLiterally(" ", "_").replaceAllLiterally("-", "--")
  }

  def uploadBadge(left: String, right: String, color: String, filename: String): Unit = {
    val badgeDir = join(baseDirectory.value.toString, "target", "badges")
    if (!badgeDir.exists()) badgeDir.mkdirs()
    runCmd(Seq("curl",
      "-o", join(badgeDir.toString, filename).toString,
      s"https://img.shields.io/badge/${enc(left)}-${enc(right)}-${enc(color)}"))
    singleUploadToBlob(
      join(badgeDir.toString, filename).toString,
      s"badges/$filename", "icons",
      extraArgs = Seq("--content-cache-control", "no-cache", "--content-type", "image/svg+xml"))
  }

  uploadBadge("master version", version.value, "blue", "master_version3.svg")
}

val settings = Seq(
  Test / scalastyleConfig := (ThisBuild / baseDirectory).value / "scalastyle-test-config.xml",
  Test / logBuffered := false,
  Test / parallelExecution := false,
  Test / publishArtifact := true,
  assembly / test := {},
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false),
  autoAPIMappings := true,
  pomPostProcess := pomPostFunc,
  sbtPlugin := false
)
ThisBuild / publishMavenStyle := true

lazy val core = (project in file("core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(settings ++ Seq(
    libraryDependencies ++= dependencies,
    buildInfoKeys ++= Seq[BuildInfoKey](
      datasetDir,
      version,
      scalaVersion,
      sbtVersion,
      baseDirectory
    ),
    name := "synapseml-core",
    buildInfoPackage := "com.microsoft.azure.synapse.ml.build"
  ): _*)

lazy val deepLearning = (project in file("deep-learning"))
  .dependsOn(core % "test->test;compile->compile", opencv % "test->test;compile->compile")
  .settings(settings ++ Seq(
    libraryDependencies ++= Seq(
      "com.microsoft.azure" % "onnx-protobuf_2.12" % "0.9.3",
      "com.microsoft.onnxruntime" % "onnxruntime_gpu" % "1.8.1"
    ),
    name := "synapseml-deep-learning"
  ): _*)

lazy val lightgbm = (project in file("lightgbm"))
  .dependsOn(core % "test->test;compile->compile")
  .settings(settings ++ Seq(
    libraryDependencies += ("com.microsoft.ml.lightgbm" % "lightgbmlib" % "3.3.500"),
    name := "synapseml-lightgbm"
  ): _*)

lazy val vw = (project in file("vw"))
  .dependsOn(core % "test->test;compile->compile")
  .settings(settings ++ Seq(
    libraryDependencies += ("com.github.vowpalwabbit" % "vw-jni" % "9.3.0"),
    name := "synapseml-vw"
  ): _*)

lazy val cognitive = (project in file("cognitive"))
  .dependsOn(core % "test->test;compile->compile")
  .settings(settings ++ Seq(
    libraryDependencies ++= Seq(
      "com.microsoft.cognitiveservices.speech" % "client-jar-sdk" % "1.14.0",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4" % "test",
      "org.apache.hadoop" % "hadoop-azure" % "3.3.4" % "test",
    ),
    name := "synapseml-cognitive"
  ): _*)

lazy val opencv = (project in file("opencv"))
  .dependsOn(core % "test->test;compile->compile")
  .settings(settings ++ Seq(
    libraryDependencies += ("org.openpnp" % "opencv" % "3.2.0-1"),
    name := "synapseml-opencv"
  ): _*)

lazy val root = (project in file("."))
  .aggregate(core, deepLearning, cognitive, vw, lightgbm, opencv)
  .dependsOn(
    core % "test->test;compile->compile",
    deepLearning % "test->test;compile->compile",
    cognitive % "test->test;compile->compile",
    vw % "test->test;compile->compile",
    lightgbm % "test->test;compile->compile",
    opencv % "test->test;compile->compile")
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(CodegenPlugin)
  .settings(settings ++ Seq(
    name := "synapseml",
    ThisBuild / credentials += Credentials(
      "",
      "msdata.pkgs.visualstudio.com",
      "msdata", Secrets.adoFeedToken),
    ThisBuild / useCoursier := false,
  ))

val setupTask = TaskKey[Unit]("setup", "set up library for intellij")
setupTask := {
  compile.all(ScopeFilter(
    inProjects(root, core, deepLearning, cognitive, vw, lightgbm, opencv),
    inConfigurations(Compile, Test))
  ).value
  getDatasetsTask.value
}

val convertNotebooks = TaskKey[Unit]("convertNotebooks",
  "convert notebooks to markdown for website display")
convertNotebooks := {
  runCmd(
    Seq("python", s"${join(baseDirectory.value, "website/notebookconvert.py")}")
  )
}

val testWebsiteDocs = TaskKey[Unit]("testWebsiteDocs",
  "test code blocks inside markdowns under folder website/docs/documentation")
testWebsiteDocs := {
  runCmd(
    Seq("python", s"${join(baseDirectory.value, "website/doctest.py")}", version.value)
  )
}
