import java.io.File
import java.lang.ProcessBuilder.Redirect

object BuildUtils {
  def join(root: File, folders: String*): File = {
    folders.foldLeft(root) { case (f, s) => new File(f, s) }
  }

  def join(folders: String*): File = {
    join(new File(folders.head), folders.tail: _*)
  }

  def isWindows: Boolean = {
    sys.props("os.name").toLowerCase.contains("windows")
  }

  def osPrefix: Seq[String] = {
    if (isWindows) {
      Seq("cmd", "/C")
    } else {
      Seq()
    }
  }

  def runCmd(cmd: Seq[String],
             wd: File = new File("."),
             envVars: Map[String, String] = Map()): Unit = {
    val pb = new ProcessBuilder()
      .directory(wd)
      .command(cmd: _*)
      .redirectError(Redirect.INHERIT)
      .redirectOutput(Redirect.INHERIT)
    val env = pb.environment()
    envVars.foreach(p => env.put(p._1, p._2))
    assert(pb.start().waitFor() == 0)
  }

  def uploadToBlob(source: String,
                   dest: String,
                   container: String,
                   accountName: String = "mmlspark"): Unit = {
    val command = Seq("az", "storage", "blob", "upload-batch",
      "--source", source,
      "--destination", container,
      "--destination-path", dest,
      "--account-name", accountName,
      "--account-key", Secrets.storageKey)
    runCmd(osPrefix ++ command)
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
    runCmd(osPrefix ++ command)
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
      "--account-key", Secrets.storageKey) ++ extraArgs
    runCmd(osPrefix ++ command)
  }


  def allFiles(dir: File, pred: (File => Boolean) = null): Array[File] = {
    def loop(dir: File): Array[File] = {
      val (dirs, files) = dir.listFiles.sorted.partition(_.isDirectory)
      (if (pred == null) files else files.filter(pred)) ++ dirs.flatMap(loop)
    }

    loop(dir)
  }

  // Perhaps this should move into a more specific place, not a generic file utils thing
  def zipFolder(dir: File, out: File): Unit = {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream}
    import java.util.zip.{ZipEntry, ZipOutputStream}
    val bufferSize = 2 * 1024
    val data = new Array[Byte](bufferSize)
    val zip = new ZipOutputStream(new FileOutputStream(out))
    val prefixLen = dir.getParentFile.toString.length + 1
    allFiles(dir).foreach { file =>
      zip.putNextEntry(new ZipEntry(file.toString.substring(prefixLen).replace(java.io.File.separator, "/")))
      val in = new BufferedInputStream(new FileInputStream(file), bufferSize)
      var b = 0
      while (b >= 0) {
        zip.write(data, 0, b); b = in.read(data, 0, bufferSize)
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()
  }

}