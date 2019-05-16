name := "mmlspark"
version := "0.0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
val sprayVersion = "1.3.4"
val cntkVersion  = "2.4"

libraryDependencies ++= Seq(
    "org.apache.spark"   %% "spark-core"   % sparkVersion % "compile",
    "org.apache.spark"   %% "spark-mllib"  % sparkVersion % "compile",
    "org.scalactic" %% "scalactic" % "3.0.5",
    "org.scalatest" %% "scalatest" % "3.0.5" % "it,test",
    // should include these things in the distributed jar
    "io.spray"           %% "spray-json"   % "1.3.2",
    "com.microsoft.cntk"  % "cntk"         % cntkVersion,
    "org.openpnp"         % "opencv"       % "3.2.0-1",
    "com.jcraft"          % "jsch"         % "0.1.54",
    "com.jcraft"          % "jsch"         % "0.1.54",
    "org.apache.httpcomponents" % "httpclient"   % "4.5.6",
    "com.microsoft.ml.lightgbm" %  "lightgbmlib" % "2.2.300"
    // needed for wasb access, but it collides with the version that comes with Spark,
    // so it gets installed manually for now (see "tools/config.sh")
    // "org.apache.hadoop"   % "hadoop-azure" % "2.7.3"
)

lazy val IntegrationTest2 = config("it").extend(Test)

lazy val myProject = (project in file("."))
  .configs(IntegrationTest2)
  .settings(inConfig(IntegrationTest2)(Defaults.testSettings) : _*)
