import java.io.File
import BuildUtils.{join, runCmd, singleUploadToBlob, zipFolder}
import CondaPlugin.autoImport.{condaEnvLocation, createCondaEnvTask}
import org.apache.commons.io.FileUtils
import sbt.Keys._
import sbt.{Def, Global, Tags, _}
import spray.json._
import BuildUtils._

object CodegenConfigProtocol extends DefaultJsonProtocol {
  implicit val CCFormat: RootJsonFormat[CodegenConfig] = jsonFormat8(CodegenConfig.apply)
}

import CodegenConfigProtocol._

case class CodegenConfig(name: String,
                         jarName: Option[String],
                         topDir: String,
                         targetDir: String,
                         version: String,
                         pythonizedVersion: String,
                         rVersion: String,
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
  val PyCodeGenTag = Tags.Tag("pyCodeGen")
  val PyTestGenTag = Tags.Tag("pyTestGen")
  val RCodeGenTag = Tags.Tag("rCodeGen")
  val RTestGenTag = Tags.Tag("rTestGen")

  object autoImport {
    val rVersion = settingKey[String]("R version")
    val genRPackageNamespace = settingKey[String]("genRPackageNamespace")

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
    val removePipPackage = TaskKey[Unit]("removePipPackage",
      "remove the installed synapseml pip package from local env")

    val publishPython = TaskKey[Unit]("publishPython", "publish python wheel")
    val testPython = TaskKey[Unit]("testPython", "test python sdk")

    val pythonIgnoreTestPath = settingKey[Option[String]]("Optional path to ignore in pytest")
    val pythonSubTestPath = settingKey[Option[String]]("Optional subpath of tests to run")

    val pyCodegen = TaskKey[Unit]("pyCodegen", "Generate python code")
    val pyTestgen = TaskKey[Unit]("pyTestgen", "Generate python tests")

    val mergeCodeDir = SettingKey[File]("mergeCodeDir")
    val mergePyCode = TaskKey[Unit]("mergePyCode", "copy python code to a destination")
  }

  import autoImport._

  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    Global / concurrentRestrictions ++= Seq(
      Tags.limit(RInstallTag, 1), Tags.limit(TestGenTag, 1)),
    Global / excludeLintKeys += publishMavenStyle
  )

  def testRImpl: Def.Initialize[Task[Unit]] = Def.task {
    packageR.value
    publishLocal.value
    rTestGen.value
    val libPath = join(condaEnvLocation.value, "lib", "R", "library").toString
    val rSrcDir = join(codegenDir.value, "src", "R", genRPackageNamespace.value)
    val rTestDir = join(codegenDir.value, "test", "R")
    rCmd(activateCondaEnv,
      Seq("R", "CMD", "INSTALL", "--no-multiarch", "--with-keep.source", genRPackageNamespace.value),
      rSrcDir.getParentFile, libPath)
    val testRunner = join("tools", "tests", "run_r_tests.R")
    if (rTestDir.exists()) {
      rCmd(activateCondaEnv,
        Seq("Rscript", testRunner.getAbsolutePath), rTestDir, libPath)
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
      val cp = (Test / fullClasspath).value
      val log = streams.value.log
      // Ensure java.sql is available to Scala reflection when running PyCodegen under Java 17+
      val javaOpts = (Test / javaOptions).value ++ Seq(
        "--add-modules=java.sql",
        "--add-opens=java.sql/java.sql=ALL-UNNAMED"
      )
      val opts = ForkOptions()
        .withRunJVMOptions(javaOpts.toVector)
        .withOutputStrategy(OutputStrategy.LoggedOutput(log))

      // Write arg to temp file to avoid quoting issues
      val tempFile = File.createTempFile("pycodegen_conf", ".json")
      FileUtils.writeStringToFile(tempFile, arg, "UTF-8")

      val exitCode = Fork.java(
        opts,
        Seq(
          "-cp",
          cp.files.map(_.getAbsolutePath).mkString(File.pathSeparator),
          "com.microsoft.azure.synapse.ml.codegen.PyCodegen",
          "@" + tempFile.getAbsolutePath
        )
      )
      if (exitCode != 0) sys.error(s"PyCodegen failed with exit code $exitCode")
    }
  } tag (PyCodeGenTag)

  def pyTestGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      val cp = (Test / fullClasspath).value
      val log = streams.value.log
      // We must explicitly add the java.sql modules here because (Test / runner) might not reliably
      // propagate them to the forked process in all environments or task configurations.
      val javaOpts = (Test / javaOptions).value ++ Seq(
        "--add-modules=java.sql",
        "--add-opens=java.sql/java.sql=ALL-UNNAMED"
      )
      val opts = ForkOptions()
        .withRunJVMOptions(javaOpts.toVector)
        .withOutputStrategy(OutputStrategy.LoggedOutput(log))
      
      // Write arg to temp file to avoid quoting issues
      val tempFile = File.createTempFile("pytestgen_conf", ".json")
      FileUtils.writeStringToFile(tempFile, arg, "UTF-8")
      
      val exitCode = Fork.java(opts, Seq("-cp", cp.files.map(_.getAbsolutePath).mkString(File.pathSeparator), "com.microsoft.azure.synapse.ml.codegen.PyTestGen", "@" + tempFile.getAbsolutePath))
      if (exitCode != 0) sys.error(s"PyTestGen failed with exit code $exitCode")
    }
  } tag (PyTestGenTag)

  def rCodeGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = codegenArgs.value
    Def.task {
      val r = (Test / runner).value
      val cp = (Test / fullClasspath).value
      r.run("com.microsoft.azure.synapse.ml.codegen.RCodegen", cp.files, Seq(arg), streams.value.log).get
    }
  } tag (RCodeGenTag)

  def rTestGenImpl: Def.Initialize[Task[Unit]] = Def.taskDyn {
    (Compile / compile).value
    (Test / compile).value
    val arg = testgenArgs.value
    Def.task {
      val r = (Test / runner).value
      val cp = (Test / fullClasspath).value
      r.run("com.microsoft.azure.synapse.ml.codegen.RTestGen", cp.files, Seq(arg), streams.value.log).get
    }
  } tag (RTestGenTag)

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    Compile / fork := true,
    Compile / javaOptions += "--add-modules=java.sql",
    Test / javaOptions ++= Seq(
      "--add-modules=java.sql",
      "--add-opens=java.sql/java.sql=ALL-UNNAMED"
    ),
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
      if (sys.props.getOrElse("skipCodegen", "false") != "true") {
        (Compile / compile).value
        (Test / compile).value
        val arg = codegenArgs.value
        Def.task {
          val cp = (Compile / fullClasspath).value
          val opts = ForkOptions()
            .withRunJVMOptions((Seq("--add-modules=java.sql") ++ (Compile / javaOptions).value).toVector)
            .withOutputStrategy(OutputStrategy.LoggedOutput(streams.value.log))
          // Write arg to temp file to avoid quoting issues
          val tempFile = File.createTempFile("codegen_conf", ".json")
          FileUtils.writeStringToFile(tempFile, arg, "UTF-8")
          val exitCode = Fork.java(opts, Seq("-cp", cp.files.map(_.getAbsolutePath).mkString(File.pathSeparator), "com.microsoft.azure.synapse.ml.codegen.CodeGen", "@" + tempFile.getAbsolutePath))
          if (exitCode != 0) sys.error(s"Codegen failed with exit code $exitCode")
        }
      } else {
        Def.task {
          streams.value.log.info("Skipping codegen.")
        }
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
      val packageDir = join(codegenDir.value, "package", "python").absolutePath
      val pythonSrcDir = join(codegenDir.value, "src", "python")
      packagePythonWheelCmd(packageDir, pythonSrcDir)
    },
    removePipPackage := {
      runCmd(activateCondaEnv ++ Seq("pip", "uninstall", "-y", name.value))
    },
    installPipPackage := {
      val packagePythonResult: Unit = packagePython.value
      val publishLocalResult: Unit = (publishLocal dependsOn packagePython).value
      val rootPublishLocalResult: Unit = (LocalRootProject / Compile / publishLocal).value
      runCmd(
        activateCondaEnv ++ Seq(
          "pip",
          "install",
          "-I",
          "--no-deps",
          s"${name.value.replace("-", "_")}-${pythonizedVersion(version.value)}-py2.py3-none-any.whl"
        ),
        join(codegenDir.value, "package", "python")
      )
    },
    publishPython := {
      val packagePythonResult: Unit = packagePython.value
      val publishLocalResult: Unit = (publishLocal dependsOn packagePython).value
      val rootPublishLocalResult: Unit = (LocalRootProject / Compile / publishLocal).value
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
    pyCodegen := pyCodeGenImpl.value,
    pythonIgnoreTestPath := sys.props.get("pythonIgnoreTestPath"),
    pythonSubTestPath := sys.props.get("pythonSubTestPath"),
    testPython := {
      installPipPackage.value
      pyTestgen.value
      val mainTargetDir = join(baseDirectory.value.getParent, "target")
      val baseTestPath = genTestPackageNamespace.value
      val ignoreArg = pythonIgnoreTestPath.value.map(path => s"--ignore=${new File(baseTestPath, path)}").toSeq
      val subPathArg = pythonSubTestPath.value.map(identity).toSeq
      val testPath: String = subPathArg.headOption
        .map(sub => join(baseTestPath, sub).toString)
        .getOrElse(baseTestPath)

      val cmd = activateCondaEnv ++ Seq(
        "python",
        "-m", "pytest",
        s"--junitxml=${join(mainTargetDir, s"python-test-results-${name.value}.xml")}",
        // "--cov=${genPackageNamespace.value}",
        // "--cov-report=xml",
      ) ++ ignoreArg ++ Seq(testPath)

      runCmd(cmd, new File(codegenDir.value, "test/python/"))
    },
    rCodeGen := rCodeGenImpl.value,
    rTestGen := rTestGenImpl.value,
    testR := testRImpl.value,
    targetDir := {
      (Compile / packageBin / artifactPath).value.getParentFile
    },
    mergeCodeDir := {
      join(baseDirectory.value.getParent, "target", "scala-2.13", "generated")
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
