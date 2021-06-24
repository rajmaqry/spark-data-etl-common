package common.data.spark.args.beans


import java.util.concurrent.ConcurrentHashMap


import common.data.spark.beans.logger.Logging
import common.data.spark.args.beans

object ExecutionConfig extends Logging{

  def create(configPath: String) : ExecutionConfig = {
   null
  }


}

class ExecutionConfig extends Logging {
  def set(key: String, value: Any): ExecutionConfig = {
    set(key, value,false)
  }

  private def set(key: String, value: Any, silent: Boolean): ExecutionConfig = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logger.info(s"Handling configuration key : $key")
    }
    settings.put(key, value)
    this
  }
  /** return the option value of the key from [[settings]]**/
  private def getOption(key: String):Option[Any] = {
    Option(settings.get(key)).orElse(null)
  }
  /** public method to get the key from configuration */
  def get(key:String): Any ={
    getOption(key)
  }

  /** public method to check if key is there */
  def contains(key: String):Boolean ={
    settings.contains(key)
  }
  private var settings = new ConcurrentHashMap[String, Any]()


}
