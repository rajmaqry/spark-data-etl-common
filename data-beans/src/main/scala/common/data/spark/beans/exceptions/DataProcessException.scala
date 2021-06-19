package common.data.spark.beans.exceptions

class DataProcessException(msg: String, e: Throwable) extends Exception{

    def this(message: String) = this(message,null)

}
