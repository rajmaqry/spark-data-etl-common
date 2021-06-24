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
  def ifExists(u: URL): Boolean

  /**
   *  Abstract method to define read content of a file.
   *  extended subclass will use this implement own api logic.
   */
  def readContent(configPath: String):  String

}
