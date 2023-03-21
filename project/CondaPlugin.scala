import BuildUtils._
import EnvironmentUtils.osCommandPrefix
import sbt._

//noinspection ScalaStyle
object CondaPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val cleanCondaEnvTask = TaskKey[Unit]("cleanCondaEnv", "create conda env")
    val condaEnvLocation = TaskKey[File]("condaEnvLocation", "get install location of conda env")
    val createCondaEnvTask = TaskKey[Unit]("createCondaEnv", "create conda env")
  }

  private def getCondaEnvListOutput(): Seq[String] =
    getCmdOutput(osCommandPrefix ++ Seq("conda", "env", "list")).split("\\s*\n")

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    cleanCondaEnvTask := {
      runCmd(osCommandPrefix ++ Seq("conda", "env", "remove", "--name", condaEnvName, "-y"))
    },
    condaEnvLocation := {
      createCondaEnvTask.value
      new File(getCondaEnvListOutput
        .map(_.split("\\s+"))
        .map(l => (l.head, l.reverse.head))
        .filter(p => p._1 == condaEnvName)
        .head._2)
    },
    createCondaEnvTask := {
      val hasEnv = getCondaEnvListOutput.exists(_.split("\\s+").head == condaEnvName)
      if (!hasEnv) {
        runCmd(osCommandPrefix ++ Seq("conda", "env", "create", "-f", "environment.yml"))
      } else {
        println("Found conda env " + condaEnvName)
      }
    }
  )

  override def requires: Plugins = WindowsPlugin

  override lazy val projectSettings: Seq[Setting[_]] = Seq()
}
