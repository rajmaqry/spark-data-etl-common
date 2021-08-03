package common.data.distributed.storage

import java.net.URL

import common.data.distributed.storage.factory.FileSystemFactory
import common.data.spark.beans.exceptions.DataProcessException

object FileSystemUtility {
  /**
   * Utility method to write small content to files.
   * @param path
   * @param content
   * @param fileName
   */
  def writeContent(path: String, content: String, fileName: String): Unit = {
    val u = new URL(path)
    val scheme = u.getProtocol
    val fs = FileSystemFactory.getFs(scheme)
    fs.saveContent(path,content,fileName)
  }

  /**
   * Utility method to get or collect the file extension
   *
   * @param path :[[String]] : Path to the directory or files
   * @return
   */
  def getFileExtension(path: String): String = {
    var ext = ""
    val u = new URL(path)
    val scheme = u.getProtocol
    val fs = FileSystemFactory.getFs(scheme)
    if(fs.isDirectory(u)){
        ext = fs.getExtensions(u)
    }else{
      ext = fs.getExtension(u)
    }
    ext
  }

  /**
   * Utility method that will identify if directory/files are present in the path provided
   * @param path : [[String]] : Path to the directory or files.
   * @return
   */
  def checkIfFileExist(path: String): Boolean  = {
    var res = false;
    val u = new URL(path)
    val scheme = u.getProtocol
    val fs = FileSystemFactory.getFs(scheme)
    if(fs.isDirectory(u)){
      if(fs.numOfFiles(u) > 0) { res = true}
    }else{
      res = fs.ifExists(u)
    }
    res
  }

  /**
   * Utility method will use the underlying FileSystems to read and return
   * file content. Useful for reading configuration/manifest files.
   *
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

  def getSchema(path: String): String = {
    val u = new URL(path)
    val scheme = u.getProtocol
    scheme
  }
}
