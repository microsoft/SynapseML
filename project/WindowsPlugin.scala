import EnvironmentUtils._
import sbt.{AutoPlugin, Plugins, Setting, TaskKey}

import scala.language.postfixOps
import scala.reflect.io.Directory
import java.io.File

/*
 *
 * Windows has unique requirements for interoperability with Spark, such as Hadoop's dependency on WinUtils.
 * https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
 *
 * For this repo's source of WinUtils distribution, see https://github.com/cdarlint/winutils
 *
 */

//noinspection ScalaStyle
object WindowsPlugin extends AutoPlugin {

  override def requires: Plugins = sbt.Plugins.empty

  override def trigger = allRequirements

  override lazy val projectSettings: Seq[Setting[_]] = Seq()

  object autoImport {
    val getHadoopTask = TaskKey[Unit]("getHadoop", "download hadoop and set %HADOOP_HOME%")
    val verifyHadoopTask = TaskKey[Unit](
      "verifyHadoop",
      "verify that your Windows environment is correctly configured for Hadoop")
  }

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    verifyHadoopTask := {
      val genericHadoopAdvice = "Please run getHadoop and follow the subsequent instructions."
      if (IsWindows) {
        val hadoopHome = sys.env.get("HADOOP_HOME")
          .getOrElse(throw new Exception(f"HADOOP_HOME is missing. $genericHadoopAdvice"))

        val path = sys.env.get("Path")
          .getOrElse(throw new Exception("PATH is missing. You have more serious problems than Hadoop being missing."))

        if (!path.contains(hadoopHome)) {
          throw new Exception(f"%%HADOOP_HOME%%/bin is missing from PATH. $genericHadoopAdvice")
        }

        if (!hadoopFilesExist(Some(new File(hadoopHome)))) {
          // Use HADOOP_HOME instead of EnvironmentUtils.HadoopDir for backwards compatibility
          // with existing dev environments
          throw new Exception(f"Hadoop files are missing from $hadoopHome. $genericHadoopAdvice")
        }

        println("Hadoop files found and environment variables are set appropriately.")
      } else {
        println("This environment does not require special Hadoop configuration.")
      }
    },
    getHadoopTask := {
      def acquireHadoop(hadoopHome: File = HadoopDir): Unit = {
        new Directory(hadoopHome).deleteRecursively()
        try {
          downloadFilesToDirectory(HadoopFiles, hadoopHome)
        } catch {
          case e: Throwable => throw new Exception(f"Failed to provision Hadoop files. ${e.getMessage}", e)
        }
      }

      if (IsWindows) {
        val hadoopHome = sys.env.get("HADOOP_HOME")
        if (hadoopHome.nonEmpty) {
          if (hadoopFilesExist(hadoopHome.get)) {
            println("Hadoop installation already exists. Skipping Hadoop acquisition.")
          } else {
            acquireHadoop(new File(hadoopHome.get))
          }
        } else if (hadoopFilesExist()) {
          throw new Exception(f"HADOOP_HOME is not set but Hadoop files exist in $HadoopDir. Skipping Hadoop Acquisition. You need to set %%HADOOP_HOME%% and add %%HADOOP_HOME%%\\bin to your PATH")
        } else {
          acquireHadoop()
          println(
            f"""Downloaded hadoop files to local directory: $HadoopDir
               |Manual steps are required to complete the environment configuration:
               |   1. Create an environment variable %%HADOOP_HOME%%=$HadoopDir
               |   2. Append the bin directory to your PATH. PATH=%%PATH%%;%%HADOOP_HOME%%\\bin
               ||""".stripMargin)
        }
      } else {
        println("Running on a non-Windows host. Skipping Hadoop acquisition.")
      }
    }
  )

}

