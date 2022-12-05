import BuildUtils._
import sbt._

//scalastyle:off field.name
object PublishPlugin extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  object autoImport {
    val refreshCachedSecretsTask = TaskKey[Unit]("refreshCachedSecrets", "refresh publishing secrets")
  }

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(

    refreshCachedSecretsTask := {
      Secrets.refreshCachedSecrets()
    }
  )

  override def requires: Plugins = sbt.Plugins.empty

  override lazy val projectSettings: Seq[Setting[_]] = Seq()
}
