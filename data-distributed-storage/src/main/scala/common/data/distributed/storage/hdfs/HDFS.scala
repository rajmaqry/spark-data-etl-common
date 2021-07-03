package common.data.distributed.storage.hdfs

import java.net.URL

import common.data.distributed.storage.BaseFileSystem

class HDFS extends BaseFileSystem{
      var path = ""

  /**
   * Abstract method to define read content of a file.
   * extended subclass will use this implement own api logic.
   */
  override def readContent(configPath: String): String = ""

  override def ifExists(u: URL): Boolean = true

  override def isDirectory(u: URL): Boolean = true

  override def numOfFiles(u: URL): Int = 1

  override def getExtension(u: URL): String = "csv"

  override def getExtensions(u: URL): String = ""
}
object HDFS {
  def apply(path: String) : HDFS ={
    val hdfs = new HDFS
    hdfs.path = path
    hdfs
  }
}

