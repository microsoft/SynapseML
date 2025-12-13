import java.io.File
import org.rauschig.jarchivelib.ArchiverFactory

object UnzipUtils {
  def unzip(source: File, dest: File): Unit = {
    import scala.sys.process._
    if (sys.props("os.name").toLowerCase.contains("windows")) {
      val archiver = ArchiverFactory.createArchiver("tar", "gz")
      archiver.extract(source, dest)
    } else {
      val command = s"tar -xzf ${source.getAbsolutePath} -C ${dest.getAbsolutePath}"
      val exitCode = command.!
      if (exitCode != 0) {
        throw new RuntimeException(s"Tar extraction failed with exit code $exitCode for command: $command")
      }
    }
  }
}
