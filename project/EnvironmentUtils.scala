import java.io.File
import java.net.URL
import java.nio.file.{Files, StandardCopyOption}
import scala.language.postfixOps
import scala.sys.process._

// Utils related to OS and runtime environment
object EnvironmentUtils {
  val HadoopFiles: Seq[String] = Seq(
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/winutils.exe",
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.0/bin/hadoop.dll"
  )

  val HadoopDir: File = new File(System.getProperty("user.dir"), ".hadoop")

  val IsWindows: Boolean = sbt.internal.util.Util.isWindows

  def hadoopFilesExist(dir: String): Boolean = hadoopFilesExist(Some(new File(dir)))

  def hadoopFilesExist(dir: Option[File] = None): Boolean = {
    val hadoopHome = dir.getOrElse(HadoopDir)
    HadoopFiles.forall(uri => {
      val localFile = new File(new File(HadoopDir, "bin"), uri.split("/").last).toPath
      Files.exists(localFile)
    })
  }

  def osCommandPrefix: Seq[String] = {
    if (IsWindows) {
      Seq("cmd", "/C") ++ HadoopDependencyCommandPrefix
    } else {
      Seq()
    }
  }

  val HadoopDependencyCommandPrefix: Seq[String] = {
    Seq() ++
      Seq("set", f"HADOOP_HOME=$HadoopDir", "&") ++
      Seq("set", f"PATH=%%PATH%%;%%HADOOP_HOME%%\\bin", "&")
  }

  def downloadFilesToDirectory(files: Seq[String], directory: File): Unit = {
    val uris = files.map(new URL(_))
    try {
      val tmpDir = Files.createTempDirectory("synapseml").toFile
      val tmpFiles = uris.map(uri => {
        val file = new File(tmpDir, uri.toString.split("/").last)
        uri #> file !!; // The semicolon is necessary here
        file
      })
      var targetDirectory = new File(directory, "bin")
      Files.createDirectories(targetDirectory.toPath)
      tmpFiles.map(file => {
        Files.move(
          file.toPath,
          new File(targetDirectory, file.getName).toPath, StandardCopyOption.ATOMIC_MOVE)
      })
      tmpDir.delete()
    }
    catch {
      case e: Throwable => throw new Exception(f"Failed to download files to directory: ${e.getMessage}", e)
    }
  }

}