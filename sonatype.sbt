import xerial.sbt.Sonatype._

ThisBuild / sonatypeProjectHosting := Some(
  GitHubHosting("Azure", "SynapseML", "mmlspark-support@microsoft.com"))
ThisBuild / homepage := Some(url("https://github.com/Microsoft/SynapseML"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/Azure/SynapseML"),
    "scm:git@github.com:Azure/SynapseML.git"
  )
)
ThisBuild / developers := List(
  Developer("mhamilton723", "Mark Hamilton",
    "synapseml-support@microsoft.com", url("https://github.com/mhamilton723")),
  Developer("imatiach-msft", "Ilya Matiach",
    "synapseml-support@microsoft.com", url("https://github.com/imatiach-msft")),
  Developer("drdarshan", "Sudarshan Raghunathan",
    "synapseml-support@microsoft.com", url("https://github.com/drdarshan")),
  Developer("svotaw", "Scott Votaw",
    "synapseml-support@microsoft.com", url("https://github.com/svotaw"))
)

ThisBuild / licenses += ("MIT", url("https://github.com/Microsoft/SynapseML/blob/master/LICENSE"))

ThisBuild / credentials += Credentials("Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  Secrets.nexusUsername,
  Secrets.nexusPassword)

pgpPassphrase := Some(Secrets.pgpPassword.toCharArray)
pgpSecretRing := Secrets.pgpPrivateFile
pgpPublicRing := Secrets.pgpPublicFile

if(Secrets.publishToFeed) {
  ThisBuild / publishTo := Some("SynapseML_PublicPackages" at
    "https://msdata.pkgs.visualstudio.com/A365/_packaging/SynapseML_PublicPackages/maven/v1")
} else {
  ThisBuild / publishTo := sonatypePublishToBundle.value
}

ThisBuild / dynverSonatypeSnapshots := true
ThisBuild / dynverSeparator := "-"