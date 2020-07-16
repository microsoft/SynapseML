import java.io.{File, PrintWriter}
import java.net.URL

import org.apache.commons.io.FileUtils
import sbt.internal.util.ManagedLogger

import scala.sys.process.Process

val condaEnvName = "mmlspark"
name := "mmlspark"
organization := "com.microsoft.ml.spark"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "compile",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "io.spray" %% "spray-json" % "1.3.2",
  "com.microsoft.cntk" % "cntk" % "2.4",
  "org.openpnp" % "opencv" % "3.2.0-1",
  "com.jcraft" % "jsch" % "0.1.54",
  "com.microsoft.cognitiveservices.speech" % "client-sdk" % "1.11.0",
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "com.microsoft.ml.lightgbm" % "lightgbmlib" % "2.3.180",
  "com.github.vowpalwabbit" % "vw-jni" % "8.7.0.3",
  "com.linkedin.isolation-forest" %% "isolation-forest_2.4.3" % "0.3.2",
  "org.apache.spark" %% "spark-avro" % sparkVersion
)
resolvers += "Speech" at "https://mmlspark.blob.core.windows.net/maven/"

//noinspection ScalaStyle
lazy val IntegrationTest2 = config("it").extend(Test)

def join(folders: String*): File = {
  folders.tail.foldLeft(new File(folders.head)) { case (f, s) => new File(f, s) }
}

val createCondaEnvTask = TaskKey[Unit]("createCondaEnv", "create conda env")
createCondaEnvTask := {
  val s = streams.value
  val hasEnv = Process("conda env list").lineStream.toList
    .map(_.split("\\s+").head).contains(condaEnvName)
  if (!hasEnv) {
    Process(
      "conda env create -f environment.yaml",
      new File(".")) ! s.log
  } else {
    println("Found conda env " + condaEnvName)
  }
}

val cleanCondaEnvTask = TaskKey[Unit]("cleanCondaEnv", "create conda env")
cleanCondaEnvTask := {
  val s = streams.value
  Process(
    s"conda env remove --name $condaEnvName -y",
    new File(".")) ! s.log
}

def isWindows: Boolean = {
  sys.props("os.name").toLowerCase.contains("windows")
}

def osPrefix: Seq[String] = {
  if (isWindows) {
    Seq("cmd", "/C")
  } else {
    Seq()
  }
}

def activateCondaEnv: Seq[String] = {
  if (sys.props("os.name").toLowerCase.contains("windows")) {
    osPrefix ++ Seq("activate", condaEnvName, "&&")
  } else {
    Seq()
    //TODO figure out why this doesent work
    //Seq("/bin/bash", "-l", "-c", "source activate " + condaEnvName, "&&")
  }
}

val packagePythonTask = TaskKey[Unit]("packagePython", "Package python sdk")
val genDir = join("target", "scala-2.11", "generated")
val unidocDir = join("target", "scala-2.11", "unidoc")
val pythonSrcDir = join(genDir.toString, "src", "python")
val unifiedDocDir = join(genDir.toString, "doc")
val pythonDocDir = join(unifiedDocDir.toString, "pyspark")
val pythonPackageDir = join(genDir.toString, "package", "python")
val pythonTestDir = join(genDir.toString, "test", "python")

val generatePythonDoc = TaskKey[Unit]("generatePythonDoc", "Generate sphinx docs for python")
generatePythonDoc := {
  val s = streams.value
  installPipPackageTask.value
  Process(activateCondaEnv ++ Seq("sphinx-apidoc", "-f", "-o", "doc", "."),
    join(pythonSrcDir.toString, "mmlspark")) ! s.log
  Process(activateCondaEnv ++ Seq("sphinx-build", "-b", "html", "doc", "../../../doc/pyspark"),
    join(pythonSrcDir.toString, "mmlspark")) ! s.log

}

val pythonizedVersion = settingKey[String]("Pythonized version")
pythonizedVersion := {
  if (version.value.contains("-")){
    version.value.split("-".head).head + ".dev1"
  }else{
    version.value
  }
}

def uploadToBlob(source: String, dest: String,
                 container: String, log: ManagedLogger,
                 accountName: String="mmlspark"): Int = {
  val command = Seq("az", "storage", "blob", "upload-batch",
    "--source", source,
    "--destination", container,
    "--destination-path", dest,
    "--account-name", accountName,
    "--account-key", Secrets.storageKey)
  Process(osPrefix ++ command) ! log
}

def downloadFromBlob(source: String, dest: String,
                 container: String, log: ManagedLogger,
                 accountName: String="mmlspark"): Int = {
  val command = Seq("az", "storage", "blob", "download-batch",
    "--destination", dest,
    "--pattern", source,
    "--source", container,
    "--account-name", accountName,
    "--account-key", Secrets.storageKey)
  Process(osPrefix ++ command) ! log
}
def singleUploadToBlob(source: String, dest: String,
                 container: String, log: ManagedLogger,
                 accountName: String="mmlspark", extraArgs: Seq[String] = Seq()): Int = {
  val command = Seq("az", "storage", "blob", "upload",
    "--file", source,
    "--container-name", container,
    "--name", dest,
    "--account-name", accountName,
    "--account-key", Secrets.storageKey) ++ extraArgs
  Process(osPrefix ++ command) ! log
}

val publishDocs = TaskKey[Unit]("publishDocs", "publish docs for scala and python")
publishDocs := {
  val s = streams.value
  generatePythonDoc.value
  (Compile / unidoc).value
  val html =
    """
      |<html><body><pre style="font-size: 150%;">
      |<a href="pyspark/index.html">pyspark/</u>
      |<a href="scala/index.html">scala/</u>
      |</pre></body></html>
    """.stripMargin
  val scalaDir = join(unifiedDocDir.toString, "scala")
  if (scalaDir.exists()) FileUtils.forceDelete(scalaDir)
  FileUtils.copyDirectory(unidocDir, scalaDir)
  FileUtils.writeStringToFile(join(unifiedDocDir.toString, "index.html"), html, "utf-8")
  uploadToBlob(unifiedDocDir.toString, version.value, "docs", s.log)
}

val publishR = TaskKey[Unit]("publishR", "publish R package to blob")
publishR := {
  val s = streams.value
  (run in IntegrationTest2).toTask("").value
  val rPackage = join("target", "scala-2.11", "generated", "package", "R")
    .listFiles().head
  singleUploadToBlob(rPackage.toString,rPackage.getName, "rrr", s.log)
}

packagePythonTask := {
  val s = streams.value
  (run in IntegrationTest2).toTask("").value
  createCondaEnvTask.value
  val destPyDir = join("target", "scala-2.11", "classes", "mmlspark")
  if (destPyDir.exists()) FileUtils.forceDelete(destPyDir)
  FileUtils.copyDirectory(join(pythonSrcDir.getAbsolutePath, "mmlspark"), destPyDir)

  Process(
    activateCondaEnv ++
      Seq(s"python", "setup.py", "bdist_wheel", "--universal", "-d", s"${pythonPackageDir.absolutePath}"),
    pythonSrcDir) ! s.log
}

val installPipPackageTask = TaskKey[Unit]("installPipPackage", "install python sdk")

installPipPackageTask := {
  val s = streams.value
  publishLocal.value
  packagePythonTask.value
  Process(
    activateCondaEnv ++ Seq("pip", "install", "-I",
      s"mmlspark-${pythonizedVersion.value}-py2.py3-none-any.whl"),
    pythonPackageDir) ! s.log
}

val testPythonTask = TaskKey[Unit]("testPython", "test python sdk")

testPythonTask := {
  val s = streams.value
  installPipPackageTask.value
  Process(
    activateCondaEnv ++ Seq("python",
      "-m",
      "pytest",
      "--cov=mmlspark",
      "--junitxml=../../../../python-test-results.xml",
      "--cov-report=xml",
      "mmlsparktest"
    ),
    new File("target/scala-2.11/generated/test/python/"),
  ) ! s.log
}

val getDatasetsTask = TaskKey[Unit]("getDatasets", "download datasets used for testing")
val datasetName = "datasets-2020-01-20.tgz"
val datasetUrl = new URL(s"https://mmlspark.blob.core.windows.net/installers/$datasetName")
val datasetDir = settingKey[File]("The directory that holds the dataset")
datasetDir := {
  join(target.value.toString, "scala-2.11", "datasets", datasetName.split(".".toCharArray.head).head)
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

  val buildInfo =
    s"""
      |MMLSpark Build and Release Information
      |---------------
      |
      |### Maven Coordinates
      | `${organization.value}:${name.value}_2.11:${version.value}`
      |
      |### Maven Resolver
      | `https://mmlspark.azureedge.net/maven`
      |
      |### Documentation Pages:
      |[Scala Documentation](https://mmlspark.blob.core.windows.net/docs/${version.value}/scala/index.html)
      |[Python Documentation](https://mmlspark.blob.core.windows.net/docs/${version.value}/pyspark/index.html)
      |
    """.stripMargin

  val infoFile = join("target", "Build.md")
  if (infoFile.exists()) FileUtils.forceDelete(infoFile)
  FileUtils.writeStringToFile(infoFile, buildInfo, "utf-8")
}

val setupTask = TaskKey[Unit]("setup", "set up library for intellij")
setupTask := {
  (Compile / compile).toTask.value
  (Test / compile).toTask.value
  (IntegrationTest2 / compile).toTask.value
  getDatasetsTask.value
}

val publishBlob = TaskKey[Unit]("publishBlob", "publish the library to mmlspark blob")
publishBlob := {
  val s = streams.value
  publishM2.value
  val scalaVersionSuffix = scalaVersion.value.split(".".toCharArray.head).dropRight(1).mkString(".")
  val nameAndScalaVersion = s"${name.value}_$scalaVersionSuffix"

  val localPackageFolder = join(
    Seq(new File(new URI(Resolver.mavenLocal.root)).getAbsolutePath)
      ++ organization.value.split(".".toCharArray.head)
      ++ Seq(nameAndScalaVersion, version.value): _*).toString

  val blobMavenFolder = organization.value.replace(".", "/") +
    s"/$nameAndScalaVersion/${version.value}"
  uploadToBlob(localPackageFolder, blobMavenFolder, "maven",  s.log)
}

val release = TaskKey[Unit]("release", "publish the library to mmlspark blob")
release := Def.taskDyn {
  val v = isSnapshot.value
  if (!v){
    Def.task {sonatypeBundleRelease.value}
  }else{
    Def.task {"Not a release"}
  }
}

val publishBadges = TaskKey[Unit]("publishBadges", "publish badges to mmlspark blob")
publishBadges := {
  val s = streams.value
  def enc(s: String): String = {
    s.replaceAllLiterally("_", "__").replaceAllLiterally(" ", "_").replaceAllLiterally("-", "--")
  }

  def uploadBadge(left: String, right: String, color: String, filename: String): Unit = {
    val badgeDir = join(baseDirectory.value.toString, "target", "badges")
    if (!badgeDir.exists()) badgeDir.mkdirs()
    Process(Seq("curl",
      "-o", join(badgeDir.toString, filename).toString,
      s"https://img.shields.io/badge/${enc(left)}-${enc(right)}-${enc(color)}")
    , new File(".")) ! s.log
    singleUploadToBlob(
      join(badgeDir.toString, filename).toString,
      s"badges/$filename", "icons",  s.log, extraArgs=Seq("--content-cache-control", "no-cache"))
  }
  uploadBadge("master version", version.value,"blue", "master_version3.svg")
}

val settings = Seq(
  (scalastyleConfig in Test) := baseDirectory.value / "scalastyle-test-config.xml",
  logBuffered in Test := false,
  buildInfoKeys := Seq[BuildInfoKey](
    name, version, scalaVersion, sbtVersion,
    baseDirectory, datasetDir, pythonizedVersion),
  parallelExecution in Test := false,
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
  buildInfoPackage := "com.microsoft.ml.spark.build") ++
  inConfig(IntegrationTest2)(Defaults.testSettings)

lazy val mmlspark = (project in file("."))
  .configs(IntegrationTest2)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(ScalaUnidocPlugin)
  .settings(settings: _*)

import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(
  GitHubHosting("Azure", "MMLSpark", "mmlspark-support@microsot.com"))
homepage := Some(url("https://github.com/Azure/mmlspark"))
developers := List(
  Developer("mhamilton723", "Mark Hamilton",
    "mmlspark-support@microsoft.com", url("https://github.com/mhamilton723")),
  Developer("imatiach-msft", "Ilya Matiach",
    "mmlspark-support@microsoft.com", url("https://github.com/imatiach-msft")),
  Developer("drdarshan", "Sudarshan Raghunathan",
    "mmlspark-support@microsoft.com", url("https://github.com/drdarshan"))
)

licenses += ("MIT", url("https://github.com/Azure/mmlspark/blob/master/LICENSE"))
publishMavenStyle := true

credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  Secrets.nexusUsername,
  Secrets.nexusPassword)

pgpPassphrase := Some(Secrets.pgpPassword.toCharArray)
pgpSecretRing := {
  val temp = File.createTempFile("secret", ".asc")
  new PrintWriter(temp) {
    write(Secrets.pgpPrivate); close()
  }
  temp
}
pgpPublicRing := {
  val temp = File.createTempFile("public", ".asc")
  new PrintWriter(temp) {
    write(Secrets.pgpPublic); close()
  }
  temp
}

dynverSonatypeSnapshots in ThisBuild := true
dynverSeparator in ThisBuild := "-"
publishTo := sonatypePublishToBundle.value
