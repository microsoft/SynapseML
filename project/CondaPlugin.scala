import BuildUtils._
import sbt._
import Keys._

import scala.sys.process.Process

//noinspection ScalaStyle
object CondaPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val cleanCondaEnvTask = TaskKey[Unit]("cleanCondaEnv", "create conda env")
    val condaEnvLocation = TaskKey[File]("condaEnvLocation", "get install location of conda env")
    val createCondaEnvTask = TaskKey[Unit]("createCondaEnv", "create conda env")
  }

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    cleanCondaEnvTask := {
      runCmd(Seq("conda", "env", "remove", "--name", condaEnvName, "-y"))
    },
    condaEnvLocation := {
      createCondaEnvTask.value
      new File(Process("conda env list").lineStream.toList
        .map(_.split("\\s+"))
        .map(l => (l.head, l.reverse.head))
        .filter(p => p._1 == condaEnvName)
        .head._2)
    },
    createCondaEnvTask := {
      val hasEnv = Process("conda env list").lineStream.toList
        .map(_.split("\\s+").head).contains(condaEnvName)
      if (!hasEnv) {
        runCmd(Seq("conda", "env", "create", "-f", "environment.yml"))
      } else {
        println("Found conda env " + condaEnvName)
      }
    }
  )

  override def requires: Plugins = sbt.Plugins.empty

  override lazy val projectSettings: Seq[Setting[_]] = Seq()
}
