import java.io.File

import BlobMavenPlugin.autoImport.publishBlob
import BuildUtils.{join, uploadToBlob}
import sbt._
import Keys._
import org.apache.ivy.core.IvyPatternHelper

//noinspection ScalaStyle
object BlobMavenPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val publishBlob = TaskKey[Unit]("publishBlob", "publish the library to mmlspark blob")
    val blobArtifactInfo = SettingKey[String]("blobArtifactInfo")
  }

  import autoImport._

  override def requires: Plugins = sbt.Plugins.empty

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    publishBlob := {
      publishM2.value
      //TODO make this more general - 1.0 is a hack and not sure of a way to get this with sbt keys
      val sourceArtifactName = s"${moduleName.value}_${scalaBinaryVersion.value}_1.0"
      val destArtifactName = s"${moduleName.value}"
      val repositoryDir = new File(new URI(Resolver.mavenLocal.root))
      val orgDirs = organization.value.split(".".toCharArray.head)
      val localPackageFolder = join(repositoryDir, orgDirs ++ Seq(sourceArtifactName, version.value):_*).toString
      val blobMavenFolder = (orgDirs ++ Seq(destArtifactName, version.value)).mkString("/")
      uploadToBlob(localPackageFolder, blobMavenFolder, "maven")
      println(blobArtifactInfo.value)
    },
    blobArtifactInfo := {
      s"""
        |MMLSpark Build and Release Information
        |---------------
        |
        |### Maven Coordinates
        | `${organization.value}:${moduleName.value}:${version.value}`
        |
        |### Maven Resolver
        | `https://mmlspark.azureedge.net/maven`
        |""".stripMargin
    }
  )
}