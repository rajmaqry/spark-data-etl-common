package common.data.distributed.storage

import java.net.URL

import common.data.distributed.storage.factory.FileSystemFactory
import common.data.spark.beans.exceptions.DataProcessException

object FileSystemUtility {
  /**
   * Utility method will use the underlying FileSystems to read and return
   * file content. Useful for reading configuration/manifest files.
   * @param path
   * @return
   */
  def getFileContent(path: String): String = {
    var content = ""
    val u = new URL(path)
    val scheme = u.getProtocol
    val fs = FileSystemFactory.getFs(scheme)
    fs.ifExists(u) match{
      case true => content = fs.readContent(u.getPath)
      case false => throw new DataProcessException(s"Invalid configuration file path in $path")
    }
    content
  }
}
