package common.data.distributed.storage.file

import java.net.URL
import java.nio.file.{Files, Paths}

import common.data.distributed.storage.BaseFileSystem

class FileBase extends BaseFileSystem{
    var path = ""

  /**
   * Read content of the file using [[java.nio.file.Files]] object
   *
   * @param path
   * @return content
   */
  override def readContent(path: String): String = {
    val c = new String(Files.readAllBytes(Paths.get(path)))
    c
  }
  /**
   * Checks if the file(s) exists in the path
   * using [[java.nio.file.Files]] object
   * @param u
   * @return true/false
   */
  override def ifExists(u: URL): Boolean = {
    val path = Paths.get(u.getPath)
    Files.exists(path)
  }
}

object FileBase {
      def apply(path: String ) : FileBase = {
        val v = new FileBase()
        v.path = path
        v
      }
}
