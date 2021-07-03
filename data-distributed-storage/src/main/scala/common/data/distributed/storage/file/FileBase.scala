package common.data.distributed.storage.file

import java.net.URL
import java.nio.file.{Files, Paths}

import common.data.distributed.storage.BaseFileSystem

import scala.collection.mutable

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

  override def isDirectory(u: URL): Boolean = {
    val path = Paths.get(u.getPath)
    Files.isDirectory(path)
  }

  override def numOfFiles(u: URL): Int = {
    val path = Paths.get(u.getPath)
    Files.list(path).count().toInt
  }

  def getExtension(u: URL): String = {
    val path = Paths.get(u.getPath)
    getFileExtension(path.toAbsolutePath.toString)
  }

  override def getExtensions(u: URL): String = {
    var ext = ""
    val path = Paths.get(u.getPath)
    val c = numOfFiles(u)
    val extMap:mutable.Map[String,Int] = mutable.Map[String,Int]().withDefaultValue(0)
    Files.list(path).forEach(a =>extMap.update(getFileExtension(a.toAbsolutePath.toString),extMap(getFileExtension(a.toAbsolutePath.toString) +1)))
    val ver1 = extMap.filter(_._2 == extMap.values.max)
    if( ver1.size == 1) ext = ver1.head._1
    ext
  }
}

object FileBase {
      def apply(path: String ) : FileBase = {
        val v = new FileBase()
        v.path = path
        v
      }
}
