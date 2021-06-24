package common.data.spark.beans.logger

import org.apache.log4j.Logger
import scala.util.DynamicVariable
/*object Logger {
  val caller = new DynamicVariable[String]("---")
  def warning(msg: String) = println(s"[${caller.value} : $time] $msg")
}*/
trait Logging  {
    protected val logger =  Logger.getLogger(this.getClass.getName)
}
