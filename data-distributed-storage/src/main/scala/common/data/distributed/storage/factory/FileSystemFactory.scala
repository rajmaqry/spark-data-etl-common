package common.data.distributed.storage.factory

import common.data.distributed.storage.BaseFileSystem
import common.data.distributed.storage.file.FileBase
import common.data.distributed.storage.hdfs.HDFS
import common.data.distributed.storage.s3.S3FileSystem


object FileSystemFactory {
  val HDFS = "hdfs"
  val S3 = "s3"
  def getFs(scheme: String) :BaseFileSystem = {
    scheme.toLowerCase match {
      case FileSystemFactory.HDFS => new HDFS
      case FileSystemFactory.S3 => new S3FileSystem
      case _ => new FileBase
    }
  }
}
