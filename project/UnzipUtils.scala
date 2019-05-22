import java.io.File
import org.rauschig.jarchivelib.ArchiverFactory

object UnzipUtils {
  def unzip(source: File, dest: File): Unit = {
    val archiver = ArchiverFactory.createArchiver("tar", "gz")
    archiver.extract(source, dest)
  }
}
