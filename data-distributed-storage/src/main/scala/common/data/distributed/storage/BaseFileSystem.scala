package common.data.distributed.storage

import java.net.URL

/**
 *  Trait class for file system usage.
 *  This approach gives more freedom on workers to work with the files without
 *  differentiating between different API calls
 *  {{{
 *    Sub class such as S3File system will implement its underlying logic
 *  }}}
 *
 */
trait BaseFileSystem {
  def isDirectory(u: URL): Boolean
  def ifExists(u: URL): Boolean
  def numOfFiles(u: URL): Int
  def getExtension(u: URL): String
  def getExtensions(u: URL): String
  def saveContent(path:String, content:String, fileName:String): Unit

  val NOT_FOUND = -1
  val UNIX_SEPARATOR = "/"
  val WINDOWS_SEPARATOR = "\\"
  val S3_SEPARATOR = "/"
  val EXTENSION_SEPARATOR = "."
  val EMPTY_STRING = ""
  def getSeparator(): String ={
    val os = System.getProperty("os.name")
    if(os.startsWith("Windows")) WINDOWS_SEPARATOR
    else UNIX_SEPARATOR
  }
  /**
   *  Abstract method to define read content of a file.
   *  extended subclass will use this implement own api logic.
   */
  def readContent(configPath: String):  String

  /**
   * Returns the index of the last directory separator character.
   * <p>
   * This method will handle a file in either Unix or Windows format.
   * The position of the last forward or backslash is returned.
   * <p>
   * The output will be the same irrespective of the machine that the code is running on.
   *
   * @param fileName the fileName to find the last path separator in, null returns -1
   * @return the index of the last separator character, or -1 if there
   *         is no such character
   */
  def indexOfLastSeparator(fileName: String): Int = {
    if (fileName == null) return NOT_FOUND
    val lastUnixPos = fileName.lastIndexOf(UNIX_SEPARATOR)
    val lastWindowsPos = fileName.lastIndexOf(WINDOWS_SEPARATOR)
    Math.max(lastUnixPos, lastWindowsPos)
  }

  /**
   * Returns the index of the last extension separator character, which is a dot.
   * <p>
   * This method also checks that there is no directory separator after the last dot. To do this it uses
   * {@link #indexOfLastSeparator(String)} which will handle a file in either Unix or Windows format.
   * </p>
   * <p>
   * The output will be the same irrespective of the machine that the code is running on, with the
   * exception of a possible {@link IllegalArgumentException} on Windows (see below).
   * </p>
   * @param fileName
   * the fileName to find the last extension separator in, null returns -1
   * @return the index of the last extension separator character, or -1 if there is no such character
   */

  def indexOfExtension(fileName: String): Int = {
    if (fileName == null) return NOT_FOUND
    val extensionPos = fileName.lastIndexOf(EXTENSION_SEPARATOR)
    val lastSeparator = indexOfLastSeparator(fileName)
    if (lastSeparator > extensionPos) NOT_FOUND
    else extensionPos
  }

  /**
   * Gets the extension of a fileName.
   * <p>
   * This method returns the textual part of the fileName after the last dot.
   * There must be no directory separator after the dot.
   * <pre>
   * foo.txt      --&gt; "txt"
   * a/b/c.jpg    --&gt; "jpg"
   * a/b.txt/c    --&gt; ""
   * a/b/c        --&gt; ""
   * </pre>
   * <p>
   *
   * @param fileName the fileName to retrieve the extension of.
   * @return the extension of the file or an empty string if none exists or { @code null}
   *                                                                                if the fileName is { @code null}.
   */
  def getFileExtension(fileName: String): String = {
    if (fileName == null) return null
    val index = indexOfExtension(fileName)
    if (index == NOT_FOUND) return EMPTY_STRING
    fileName.substring(index + 1)
  }

}
