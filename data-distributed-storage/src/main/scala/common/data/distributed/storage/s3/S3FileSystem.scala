package common.data.distributed.storage.s3

import java.net.URL

import common.data.distributed.storage.BaseFileSystem

class S3FileSystem extends BaseFileSystem{
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

object S3FileSystem{
      def apply(path: String): S3FileSystem ={
        val s3 = new S3FileSystem
        s3.path = path
        s3
      }
}
