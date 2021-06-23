import BuildUtils.{osPrefix, runCmd}
import sbt._
import Keys._

import scala.sys.process.Process

//noinspection ScalaStyle
object CondaPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val condaEnvName = settingKey[String]("Name of conda environment")
    val cleanCondaEnvTask = TaskKey[Unit]("cleanCondaEnv", "create conda env")
    val condaEnvLocation = TaskKey[File]("condaEnvLocation", "get install location of conda env")
    val createCondaEnvTask = TaskKey[Unit]("createCondaEnv", "create conda env")
    val activateCondaEnv  = settingKey[Seq[String]]("commands to activate conda environment")
  }

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    condaEnvName := "mmlspark",
    cleanCondaEnvTask := {
      runCmd(Seq("conda", "env", "remove", "--name", condaEnvName.value, "-y"))
    },
    condaEnvLocation := {
      createCondaEnvTask.value
      new File(Process("conda env list").lineStream.toList
        .map(_.split("\\s+"))
        .map(l => (l.head, l.reverse.head))
        .filter(p => p._1 == condaEnvName.value)
        .head._2)
    },
    createCondaEnvTask := {
      val hasEnv = Process("conda env list").lineStream.toList
        .map(_.split("\\s+").head).contains(condaEnvName.value)
      if (!hasEnv) {
        runCmd(Seq("conda", "env", "create", "-f", "environment.yaml"))
      } else {
        println("Found conda env " + condaEnvName.value)
      }
    },
    activateCondaEnv := {
      if (sys.props("os.name").toLowerCase.contains("windows")) {
        osPrefix ++ Seq("activate", condaEnvName.value, "&&")
      } else {
        Seq()
        //TODO figure out why this doesent work
        //Seq("/bin/bash", "-l", "-c", "source activate " + condaEnvName, "&&")
      }
    }
  )

  override def requires: Plugins = sbt.Plugins.empty

  override lazy val projectSettings: Seq[Setting[_]] = Seq()
}