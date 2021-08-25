import java.io.File
import BuildUtils.{join, runCmd, singleUploadToBlob, zipFolder}
import CondaPlugin.autoImport.{activateCondaEnv, condaEnvLocation, createCondaEnvTask}
import org.apache.commons.io.FileUtils
import sbt.Keys._
import sbt.{Def, Global, Tags, _}
import spray.json._

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
    runCmd(activateCondaEnv ++ cmd, wd, Map("R_LIBS" -> libPath, "R_USER_LIBS" -> libPath))
  }

  val RInstallTag = Tags.Tag("rInstall")
  val TestGenTag = Tags.Tag("testGen")

  object autoImport {
    val pythonizedVersion = settingKey[String]("Pythonized version")
    val rVersion = settingKey[String]("R version")
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

    val packagePython = TaskKey[Unit]("packagePython", "Package python sdk")
    val installPipPackage = TaskKey[Unit]("installPipPackage", "install python sdk")
    val publishPython = TaskKey[Unit]("publishPython", "publish python wheel")
    val testPython = TaskKey[Unit]("testPython", "test python sdk")

    val packageDotnet = TaskKey[Unit]("packageDotnet", "Generate dotnet nuget package")
    val publishDotnet = TaskKey[Unit]("publishDotnet", "publish dotnet nuget package")
    val testDotnet = TaskKey[Unit]("testDotnet", "test dotnet nuget package")

    val mergePyCodeDir = SettingKey[File]("mergePyCodeDir")
    val mergePyCode = TaskKey[Unit]("mergePyCode", "copy python code to a destination")
  }

  import autoImport._

  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    Global / concurrentRestrictions ++= Seq(
      Tags.limit(RInstallTag, 1), Tags.limit(TestGenTag, 1))
  )

  def testRImpl: Def.Initialize[Task[Unit]] = Def.task {
    packageR.value
    publishLocal.value
    val libPath = join(condaEnvLocation.value, "Lib", "R", "library").toString
    val rSrcDir = join(codegenDir.value, "src", "R", genPackageNamespace.value)
    rCmd(activateCondaEnv.value,
      Seq("R", "CMD", "INSTALL", "--no-multiarch", "--with-keep.source", genPackageNamespace.value),
      rSrcDir.getParentFile, libPath)
    val testRunner = join("tools", "tests", "run_r_tests.R")
    if (join(rSrcDir,"tests").exists()){
      rCmd(activateCondaEnv.value,
        Seq("Rscript", testRunner.getAbsolutePath), rSrcDir, libPath)
    }
  } tag(RInstallTag)

  def testGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      (Test / runMain).toTask(s" com.microsoft.ml.spark.codegen.TestGen $arg").value
    }
  } tag(TestGenTag)


  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    publishMavenStyle := true,
    codegenArgs := {
      CodegenConfig(
        name.value,
        codegenJarName.value,
        baseDirectory.value.getAbsolutePath,
        targetDir.value.getAbsolutePath,
        version.value,
        pythonizedVersion.value,
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
        pythonizedVersion.value,
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
        (Compile / runMain).toTask(s" com.microsoft.ml.spark.codegen.CodeGen $arg").value
      }
    }.value),
    testgen := testGenImpl.value,
    pythonizedVersion := {
      if (version.value.contains("-")) {
        version.value.split("-".head).head + ".dev1"
      } else {
        version.value
      }
    },
    rVersion := {
      if (version.value.contains("-")) {
        version.value.split("-".head).head
      } else {
        version.value
      }
    },
    dotnetVersion := {
      version.value
    },
    packageR := {
      createCondaEnvTask.value
      codegen.value
      val rSrcDir = join(codegenDir.value, "src", "R", genPackageNamespace.value)
      val rPackageDir = join(codegenDir.value, "package", "R")
      val libPath = join(condaEnvLocation.value, "Lib", "R", "library").toString
      rCmd(activateCondaEnv.value, Seq("R", "-q", "-e", "roxygen2::roxygenise()"), rSrcDir, libPath)
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
      runCmd(
        activateCondaEnv.value ++
          Seq(s"python", "setup.py", "bdist_wheel", "--universal", "-d", packageDir),
        pythonSrcDir)
    },
    installPipPackage := {
      packagePython.value
      publishLocal.value
      runCmd(
        activateCondaEnv.value ++ Seq("pip", "install", "-I",
          s"${name.value.replace("-", "_")}-${pythonizedVersion.value}-py2.py3-none-any.whl"),
        join(codegenDir.value, "package", "python"))
    },
    publishPython := {
      publishLocal.value
      packagePython.value
      val fn = s"${name.value.replace("-", "_")}-${pythonizedVersion.value}-py2.py3-none-any.whl"
      singleUploadToBlob(
        join(codegenDir.value, "package", "python", fn).toString,
        version.value + "/" + fn, "pip")
    },
    mergePyCode := {
      val srcDir = join(codegenDir.value, "src", "python", genPackageNamespace.value)
      val destDir = join(mergePyCodeDir.value, "src", "python", genPackageNamespace.value)
      FileUtils.copyDirectory(srcDir, destDir)
    },
    testPython := {
      installPipPackage.value
      testgen.value
      val mainTargetDir = join(baseDirectory.value.getParent, "target")
      runCmd(
        activateCondaEnv.value ++ Seq("python",
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
    targetDir := {
      artifactPath.in(packageBin).in(Compile).value.getParentFile
    },
    mergePyCodeDir := {
      join(baseDirectory.value.getParent, "target", "scala-2.12", "sbt-1.0", "generated")
    },
    codegenDir := {
      join(targetDir.value, "generated")
    },
    genPackageNamespace := {
      "mmlspark"
    },
    genTestPackageNamespace := {
      "mmlsparktest"
    }

  )
}