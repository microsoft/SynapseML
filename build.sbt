import scala.sys.process.Process

name := "mmlspark"
organization := "com.microsoft.ml.spark"
version := "0.17.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
val sprayVersion = "1.3.4"
val cntkVersion = "2.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "compile",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5",
  "io.spray" %% "spray-json" % "1.3.2",
  "com.microsoft.cntk" % "cntk" % cntkVersion,
  "org.openpnp" % "opencv" % "3.2.0-1",
  "com.jcraft" % "jsch" % "0.1.54",
  "com.jcraft" % "jsch" % "0.1.54",
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "com.microsoft.ml.lightgbm" % "lightgbmlib" % "2.2.350"
)

lazy val IntegrationTest2 = config("it").extend(Test)

lazy val CodeGen = config("codegen").extend(Test)

val settings = Seq(
  (scalastyleConfig in Test) := baseDirectory.value / "scalastyle-test-config.xml",
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, baseDirectory),
  buildInfoPackage := "com.microsoft.ml.spark.build") ++
  inConfig(IntegrationTest2)(Defaults.testSettings) ++
  inConfig(CodeGen)(Defaults.testSettings)

lazy val mmlspark = (project in file("."))
  .configs(IntegrationTest2)
  .configs(CodeGen)
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(ScalaUnidocPlugin)
  .settings(settings: _*)

def join(folders: String*): File = {
  folders.tail.foldLeft(new File(folders.head)) { case (f, s) => new File(f, s) }
}

val packagePythonTask = TaskKey[Unit]("packagePython", "Package python sdk")
val genDir = join("target", "scala-2.11", "generated")
val pythonSrcDir = join(genDir.toString, "src", "python")
val pythonPackageDir = join(genDir.toString, "package", "python")
val pythonTestDir = join(genDir.toString, "test", "python")

packagePythonTask := {
  val s: TaskStreams = streams.value
  (run in CodeGen).toTask("").value
  Process(
    s"python setup.py bdist_wheel --universal -d ${pythonPackageDir.absolutePath}",
    pythonSrcDir,
    "MML_VERSION" -> version.value) ! s.log
}

val installPipPackageTask = TaskKey[Unit]("installPipPackage", "test python sdk")

installPipPackageTask := {
  val s: TaskStreams = streams.value
  packagePythonTask.value
  Process(
    Seq("python", "-m","wheel","install", s"mmlspark-${version.value}-py2.py3-none-any.whl", "--force"),
    pythonPackageDir) ! s.log
}

val testPythonTask = TaskKey[Unit]("testPython", "test python sdk")

testPythonTask := {
  val s: TaskStreams = streams.value
  installPipPackageTask.value
  Process(
    Seq("python", "-m","unittest","discover"),
    join(pythonTestDir.toString, "mmlspark")) ! s.log
}
