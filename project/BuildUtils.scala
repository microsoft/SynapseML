
import EnvironmentUtils.{IsWindows, osCommandPrefix}

import java.io.{ByteArrayOutputStream, File}
import java.lang.ProcessBuilder.Redirect
import scala.util.Using

object BuildUtils {
  def join(root: File, folders: String*): File = {
    folders.foldLeft(root) { case (f, s) => new File(f, s) }
  }

  def join(folders: String*): File = {
    join(new File(folders.head), folders.tail: _*)
  }

  def pythonizedVersion(version: String): String = {
    version match {
      case s if s.contains("-") => s.split("-".head).head + ".dev1"
      case s => s
    }
  }

  def dotnetedVersion(version: String): String = {
    version match {
      case s if s.contains("-") =>
        val versionArray = s.split("-".toCharArray)
        versionArray.head + "-rc" + versionArray.drop(1).dropRight(1).mkString("")
      case s => s
    }
  }

  def getCmdOutput(cmd: Seq[String],
             wd: File = new File("."),
             envVars: Map[String, String] = Map()): String = {
    Using(new ByteArrayOutputStream()) { outputStream =>
      runCmd(cmd, wd, envVars, outputStream = Some(outputStream))
      outputStream.toString("UTF-8")
    }.get
  }

  def runCmd(cmd: Seq[String],
             wd: File = new File("."),
             envVars: Map[String, String] = Map(),
             outputStream: Option[ByteArrayOutputStream] = None): Unit = {
    val pb = new ProcessBuilder()
      .directory(wd)
      .command(cmd: _*)
      .redirectError(Redirect.INHERIT)
      .redirectOutput(if (outputStream.nonEmpty) Redirect.PIPE else Redirect.INHERIT)
    val env = pb.environment()
    envVars.foreach(p => env.put(p._1, p._2))
    val process = pb.start()
    outputStream.map(process.getInputStream.transferTo(_))
    val result = process.waitFor()
    if (result != 0) {
      println(s"Error: result code: $result")
      throw new Exception(s"Execution resulted in non-zero exit code: $result")
    }
  }

  def condaEnvName: String = "synapseml"

  def activateCondaEnv: Seq[String] = {
    if (IsWindows) {
      osCommandPrefix ++ Seq("activate", condaEnvName, "&&")
    } else {
      Seq()
      //TODO figure out why this doesn't work
      //Seq("/bin/bash", "-l", "-c", "source activate " + condaEnvName, "&&")
    }
  }

  def packagePythonWheelCmd(packageDir: String,
                            workDir: File = new File(".")): Unit = {
    runCmd(
      activateCondaEnv ++
        Seq(s"python", "setup.py", "bdist_wheel", "--universal", "-d", packageDir),
      workDir)
  }

  def packDotnetAssemblyCmd(outputDir: String,
                            workDir: File): Unit =
    runCmd(Seq("dotnet", "pack", "--output", outputDir), workDir)

  def publishDotnetAssemblyCmd(packagePath: String,
                               sleetConfigFile: File): Unit =
    runCmd(
      Seq("sleet", "push", packagePath, "--config", sleetConfigFile.getAbsolutePath,
        "--source", "SynapseMLNuget", "--force")
    )

  def uploadToBlob(source: String,
                   dest: String,
                   container: String,
                   accountName: String = "mmlspark"): Unit = {
    val command = Seq("az", "storage", "blob", "upload-batch",
      "--source", source,
      "--destination", container,
      "--destination-path", dest,
      "--account-name", accountName,
      "--account-key", Secrets.storageKey,
      "--overwrite", "true"
    )
    runCmd(osCommandPrefix ++ command)
  }

  def downloadFromBlob(source: String,
                       dest: String,
                       container: String,
                       accountName: String = "mmlspark"): Unit = {
    val command = Seq("az", "storage", "blob", "download-batch",
      "--destination", dest,
      "--pattern", source,
      "--source", container,
      "--account-name", accountName,
      "--account-key", Secrets.storageKey)
    runCmd(osCommandPrefix ++ command)
  }

  def singleUploadToBlob(source: String,
                         dest: String,
                         container: String,
                         accountName: String = "mmlspark",
                         extraArgs: Seq[String] = Seq()): Unit = {
    val command = Seq("az", "storage", "blob", "upload",
      "--file", source,
      "--container-name", container,
      "--name", dest,
      "--account-name", accountName,
      "--account-key", Secrets.storageKey,
      "--overwrite", "true"
    ) ++ extraArgs
    runCmd(osCommandPrefix ++ command)
  }


  private def allFiles(dir: File, predicate: Option[File => Boolean] = None): Array[File] = {
    def loop(dir: File): Array[File] = {
      val (dirs, files) = dir.listFiles.sorted.partition(_.isDirectory)
      val filteredFiles = predicate match {
        case None => files
        case _ => files.filter(predicate.get)
      }
      filteredFiles ++ dirs.flatMap(loop)
    }

    loop(dir)
  }

  // Perhaps this should move into a more specific place, not a generic build utils thing
  def zipFolder(dir: File, out: File): Unit = {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
    import java.util.zip.{ZipEntry, ZipOutputStream}
    val bufferSize = 2 * 1024
    val data = new Array[Byte](bufferSize)
    Using(new ZipOutputStream(new FileOutputStream(out))) { zip =>
      val prefixLen = dir.getParentFile.toString.length + 1
      allFiles(dir).foreach { file =>
        zip.putNextEntry(new ZipEntry(file.toString.substring(prefixLen).replace(java.io.File.separator, "/")))
        Using(new BufferedInputStream(new FileInputStream(file), bufferSize)) { in =>
          Stream.continually(in.read(data, 0, bufferSize)).takeWhile(_ != -1).foreach(b => zip.write(data, 0, b))
        }
        zip.closeEntry()
      }
    }
  }

}
