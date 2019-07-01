name := "mmlspark"
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

val settings = Seq((scalastyleConfig in Test) := baseDirectory.value / "scalastyle-test-config.xml") ++
  inConfig(IntegrationTest2)(Defaults.testSettings)

lazy val mmlspark = (project in file("."))
  .configs(IntegrationTest2)
  .enablePlugins(ScalaUnidocPlugin)
  .settings(settings: _*)
