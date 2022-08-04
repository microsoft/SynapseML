import java.io.File
import BuildUtils.{join, runCmd, singleUploadToBlob, zipFolder}
import CondaPlugin.autoImport.{condaEnvLocation, createCondaEnvTask}
import org.apache.commons.io.FileUtils
import sbt.Keys._
import sbt.{Def, Global, Tags, _}
import spray.json._
import BuildUtils._

object CodegenConfigProtocol extends DefaultJsonProtocol {
  implicit val CCFormat: RootJsonFormat[CodegenConfig] = jsonFormat9(CodegenConfig.apply)
}

import CodegenConfigProtocol._

case class CodegenConfig(name: String,
                         jarName: Option[String],
                         topDir: String,
                         targetDir: String,
                         version: String,
                         pythonizedVersion: String,
                         rVersion: String,
                         dotnetVersion: String,
                         packageName: String)

//noinspection ScalaStyle
object CodegenPlugin extends AutoPlugin {
  override def trigger = allRequirements

  override def requires: Plugins = CondaPlugin

  def rCmd(activateCondaEnv: Seq[String], cmd: Seq[String], wd: File, libPath: String): Unit = {
    println(s"----- wd: ${wd.getPath}")
    runCmd(activateCondaEnv ++ cmd, wd, Map("R_LIBS" -> libPath, "R_USER_LIBS" -> libPath))
  }

  val RInstallTag = Tags.Tag("rInstall")
  val TestGenTag = Tags.Tag("testGen")
  val DotnetTestGenTag = Tags.Tag("dotnetTestGen")
  val PyCodeGenTag = Tags.Tag("pyCodeGen")
  val PyTestGenTag = Tags.Tag("pyTestGen")
  val RCodeGenTag = Tags.Tag("rCodeGen")
  val RTestGenTag = Tags.Tag("rTestGen")
  val DotnetCodeGenTag = Tags.Tag("dotnetCodeGen")
  val TestDotnetTag = Tags.Tag("testDotnet")

  object autoImport {
    val rVersion = settingKey[String]("R version")
    val genRPackageNamespace = settingKey[String]("genRPackageNamespace")

    val dotnetVersion = settingKey[String]("Dotnet version")

    val genPackageNamespace = settingKey[String]("genPackageNamespace")
    val genTestPackageNamespace = settingKey[String]("genTestPackageNamespace")

    val codegenJarName = settingKey[Option[String]]("codegenJarName")
    val testgenJarName = settingKey[Option[String]]("testgenJarName")
    val codegenArgs = settingKey[String]("codegenArgs")
    val testgenArgs = settingKey[String]("testgenArgs")


    val targetDir = settingKey[File]("targetDir")
    val codegenDir = settingKey[File]("codegenDir")

    val codegen = TaskKey[Unit]("codegen", "Generate Code")
    val testgen = TaskKey[Unit]("testgen", "Generate Tests")

    val packageR = TaskKey[Unit]("packageR", "Generate roxygen docs and zip R package")
    val publishR = TaskKey[Unit]("publishR", "publish R package to blob")
    val testR = TaskKey[Unit]("testR", "Run testthat on R tests")
    val rCodeGen = TaskKey[Unit]("rCodegen", "Generate R code")
    val rTestGen = TaskKey[Unit]("rTestgen", "Generate R tests")

    val packagePython = TaskKey[Unit]("packagePython", "Package python sdk")
    val installPipPackage = TaskKey[Unit]("installPipPackage", "install python sdk")
    val publishPython = TaskKey[Unit]("publishPython", "publish python wheel")
    val testPython = TaskKey[Unit]("testPython", "test python sdk")
    val pyCodegen = TaskKey[Unit]("pyCodegen", "Generate python code")
    val pyTestgen = TaskKey[Unit]("pyTestgen", "Generate python tests")

    val dotnetTestGen = TaskKey[Unit]("dotnetTestgen", "Generate dotnet tests")
    val dotnetCodeGen = TaskKey[Unit]("dotnetCodegen", "Generate dotnet code")
    val packageDotnet = TaskKey[Unit]("packageDotnet", "Generate dotnet nuget package")
    val publishDotnet = TaskKey[Unit]("publishDotnet", "publish dotnet nuget package")
    val testDotnet = TaskKey[Unit]("testDotnet", "test dotnet nuget package")

    val mergeCodeDir = SettingKey[File]("mergeCodeDir")
    val mergePyCode = TaskKey[Unit]("mergePyCode", "copy python code to a destination")
    val mergeDotnetCode = TaskKey[Unit]("mergeDotnetCode", "copy dotnet code to a destination")
  }

  import autoImport._

  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    Global / concurrentRestrictions ++= Seq(
      Tags.limit(RInstallTag, 1), Tags.limit(TestGenTag, 1), Tags.limit(DotnetTestGenTag, 1),
      Tags.limit(DotnetCodeGenTag, 1), Tags.limit(TestDotnetTag, 1))
  )

  def testRImpl: Def.Initialize[Task[Unit]] = Def.task {
    packageR.value
    publishLocal.value
    rTestGen.value
    println(s"condaEnvLocation: ${condaEnvLocation.value}")
    println(s"codegenDir: ${codegenDir.value}")
    val libPath = join(condaEnvLocation.value, "lib", "R", "library").toString
    println(s"libPath: ${libPath}")
    val rSrcDir = join(codegenDir.value, "src", "R", genRPackageNamespace.value)
    println(s"rSrcDir ${rSrcDir}")
    val rTestDir = join(codegenDir.value, "test", "R")
    println(s"rTestDir ${rTestDir}")
    try {
      println("running install")
      rCmd(activateCondaEnv,
        Seq("R", "CMD", "INSTALL", "--no-multiarch", "--with-keep.source", genRPackageNamespace.value),
        rSrcDir.getParentFile, libPath)
      println("ran install")
    }
    catch {
      case e: Exception => {
        println(s"Exception in activateConda: ${e.toString} (${e.getClass}")
        throw e
      }
    }
    val testRunner = join("tools", "tests", "run_r_tests.R")
    println(s"testRunner: ${testRunner}")
    if (rTestDir.exists()) {
      try {
        println("running Rscript")
        rCmd(activateCondaEnv,
          Seq("Rscript", testRunner.getAbsolutePath), rTestDir, libPath)
        println("ran Rscript")
      }
      catch {
        case e: Exception => {
          println(s"Exception in running tests: ${e.toString} (${e.getClass})}")
          throw e
        }
      }
    }
  } tag (RInstallTag)

  def testGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.TestGen $arg").value
    }
  } tag (TestGenTag)

  def pyCodeGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = codegenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.PyCodegen $arg").value
    }
  } tag (RCodeGenTag)

  def pyTestGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.PyTestGen $arg").value
    }
  } tag (PyTestGenTag)

  def rCodeGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = codegenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.RCodegen $arg").value
    }
  } tag (RCodeGenTag)

  def rTestGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.RTestGen $arg").value
    }
  } tag (RTestGenTag)

  def dotnetTestGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.DotnetTestGen $arg").value
    }
  } tag (DotnetTestGenTag)

  def dotnetCodeGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = codegenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.DotnetCodegen $arg").value
    }
  } tag (DotnetCodeGenTag)

  def testDotnetImpl: Def.Initialize[Task[Unit]] = Def.task {
    dotnetTestGen.value
    val mainTargetDir = join(baseDirectory.value.getParent, "target")
    runCmd(
      Seq("dotnet",
        "test",
        s"${join(codegenDir.value, "test", "dotnet", "SynapseMLtest", "TestProjectSetup.csproj")}",
        "--logger",
        s""""trx;LogFileName=${join(mainTargetDir, s"dotnet_test_results_${name.value}.trx")}""""
      ),
      new File(codegenDir.value, "test/dotnet/")
    )
  } tag (TestDotnetTag)

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    publishMavenStyle := true,
    codegenArgs := {
      CodegenConfig(
        name.value,
        codegenJarName.value,
        baseDirectory.value.getAbsolutePath,
        targetDir.value.getAbsolutePath,
        version.value,
        pythonizedVersion(version.value),
        rVersion.value,
        dotnetVersion.value,
        genPackageNamespace.value
      ).toJson.compactPrint
    },
    testgenArgs := {
      CodegenConfig(
        name.value,
        testgenJarName.value,
        baseDirectory.value.getAbsolutePath,
        targetDir.value.getAbsolutePath,
        version.value,
        pythonizedVersion(version.value),
        rVersion.value,
        dotnetVersion.value,
        genPackageNamespace.value
      ).toJson.compactPrint
    },
    codegenJarName := {
      val art: Artifact = (Compile / packageBin / artifact).value
      Some(artifactName.value(
        ScalaVersion(scalaVersion.value, scalaBinaryVersion.value),
        projectID.value,
        art))
    },
    testgenJarName := {
      val art: Artifact = (Test / packageBin / artifact).value
      Some(artifactName.value(
        ScalaVersion(scalaVersion.value, scalaBinaryVersion.value),
        projectID.value,
        art))
    },
    codegen := (Def.taskDyn {
      (Compile / compile).value
      (Test / compile).value
      val arg = codegenArgs.value
      Def.task {
        (Compile / runMain).toTask(s" com.microsoft.azure.synapse.ml.codegen.CodeGen $arg").value
      }
    }.value),
    testgen := testGenImpl.value,
    pyTestgen := pyTestGenImpl.value,
    rVersion := {
      if (version.value.contains("-")) {
        version.value.split("-".head).head
      } else {
        version.value
      }
    },
    dotnetVersion := {
      if (version.value.contains("-")) {
        val versionArray = version.value.split("-".toCharArray)
        versionArray.head + "-rc" + versionArray.drop(1).dropRight(1).mkString("")
      } else {
        version.value
      }
    },
    packageR := {
      createCondaEnvTask.value
      codegen.value
      val rSrcDir = join(codegenDir.value, "src", "R", genRPackageNamespace.value)
      val rPackageDir = join(codegenDir.value, "package", "R")
      val libPath = join(condaEnvLocation.value, "Lib", "R", "library").toString
      rCmd(activateCondaEnv, Seq("R", "-q", "-e", "roxygen2::roxygenise()"), rSrcDir, libPath)
      rPackageDir.mkdirs()
      zipFolder(rSrcDir, new File(rPackageDir, s"${name.value}-${version.value}.zip"))
    },
    testR := testRImpl.value,
    publishR := {
      codegen.value
      packageR.value
      val rPackageDir = join(codegenDir.value, "package", "R")
      val rPackage = rPackageDir.listFiles().head
      singleUploadToBlob(rPackage.toString, rPackage.getName, "rrr")
    },
    packagePython := {
      codegen.value
      createCondaEnvTask.value
      val destPyDir = join(targetDir.value, "classes", genPackageNamespace.value)
      val packageDir = join(codegenDir.value, "package", "python").absolutePath
      val pythonSrcDir = join(codegenDir.value, "src", "python")
      if (destPyDir.exists()) FileUtils.forceDelete(destPyDir)
      val sourcePyDir = join(pythonSrcDir.getAbsolutePath, genPackageNamespace.value)
      FileUtils.copyDirectory(sourcePyDir, destPyDir)
      packagePythonWheelCmd(packageDir, pythonSrcDir)
    },
    installPipPackage := {
      packagePython.value
      publishLocal.value
      runCmd(
        activateCondaEnv ++ Seq("pip", "install", "-I",
          s"${name.value.replace("-", "_")}-${pythonizedVersion(version.value)}-py2.py3-none-any.whl"),
        join(codegenDir.value, "package", "python"))
    },
    publishPython := {
      publishLocal.value
      packagePython.value
      val fn = s"${name.value.replace("-", "_")}-${pythonizedVersion(version.value)}-py2.py3-none-any.whl"
      singleUploadToBlob(
        join(codegenDir.value, "package", "python", fn).toString,
        version.value + "/" + fn, "pip")
    },
    mergePyCode := {
      val srcDir = join(codegenDir.value, "src", "python", genPackageNamespace.value)
      val destDir = join(mergeCodeDir.value, "src", "python", genPackageNamespace.value)
      FileUtils.copyDirectory(srcDir, destDir)
    },
    mergeDotnetCode := {
      val srcDir = join(codegenDir.value, "src", "dotnet", genPackageNamespace.value)
      val destDir = join(mergeCodeDir.value, "src", "dotnet", genPackageNamespace.value)
      FileUtils.copyDirectory(srcDir, destDir)
    },
    pyCodegen := pyCodeGenImpl.value,
    testPython := {
      installPipPackage.value
      pyTestgen.value
      val mainTargetDir = join(baseDirectory.value.getParent, "target")
      runCmd(
        activateCondaEnv ++ Seq("python",
          "-m",
          "pytest",
          s"--cov=${genPackageNamespace.value}",
          s"--junitxml=${join(mainTargetDir, s"python-test-results-${name.value}.xml")}",
          "--cov-report=xml",
          genTestPackageNamespace.value
        ),
        new File(codegenDir.value, "test/python/")
      )
    },
    rCodeGen := rCodeGenImpl.value,
    rTestGen := rTestGenImpl.value,
    testR := testRImpl.value,
    dotnetCodeGen := dotnetCodeGenImpl.value,
    dotnetTestGen := dotnetTestGenImpl.value,
    testDotnet := testDotnetImpl.value,
    packageDotnet := {
      dotnetCodeGen.value
      val destDotnetDir = join(targetDir.value, "classes", genPackageNamespace.value)
      val dotnetSrcDir = join(codegenDir.value, "src", "dotnet")
      if (destDotnetDir.exists()) FileUtils.forceDelete(destDotnetDir)
      val sourceDotnetDir = join(dotnetSrcDir.getAbsolutePath, genPackageNamespace.value)
      FileUtils.copyDirectory(sourceDotnetDir, destDotnetDir)
      val packageDir = join(codegenDir.value, "package", "dotnet").absolutePath
      packDotnetAssemblyCmd(packageDir, join(dotnetSrcDir, "synapse", "ml"))
    },
    publishDotnet := {
      packageDotnet.value
      val dotnetPackageName = name.value.split("-").drop(1).map(s => s.capitalize).mkString("")
      val packagePath = join(codegenDir.value, "package", "dotnet",
        s"SynapseML.$dotnetPackageName.${dotnetVersion.value}.nupkg").absolutePath
      publishDotnetAssemblyCmd(packagePath, mergeCodeDir.value)
    },
    targetDir := {
      (Compile / packageBin / artifactPath).value.getParentFile
    },
    mergeCodeDir := {
      join(baseDirectory.value.getParent, "target", "scala-2.12", "generated")
    },
    codegenDir := {
      join(targetDir.value, "generated")
    },
    genPackageNamespace := {
      "synapse"
    },
    genRPackageNamespace := {
      "synapseml"
    },
    genTestPackageNamespace := {
      "synapsemltest"
    }

  )
}
