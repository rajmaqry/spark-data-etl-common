package common.data.spark.reader

import common.data.distributed.storage.FileSystemUtility
import common.data.spark.beans.logger.Logging
import common.data.spark.context.ProcessContext
import common.data.spark.reader.api.DataReaderApi
import common.data.spark.args.beans.RAW_FILE_INFO
import common.data.spark.constant.DataConstant
import common.data.spark.constant.DataConstant._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
class GenericDataReader(context: ProcessContext) extends DataReaderApi with Logging{

  override def readData(context: ProcessContext): ProcessContext = {
    logger.info(s"Executing task of reading data for : ${context.arg.applicationName}")
    val rawConfigs = context.arg.executionConfig.get(RAW_FILE_INFO.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]].asScala
    for(file <- rawConfigs){
      var df: DataFrame = null
      val fileType = file.getOrDefault(ConfigConstants.FILE_TYPE, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.FILE_TYPE)).head.defaultValueString.toString)
      val id = file.get(ConfigConstants.ID)
      df = fileType match {
        case "jdbc" => SparkJdbcReader.readDataUtil(context, id)
        case _ => FileTypeDataReader.readDataUtil(context, id)
      }
      context.data_bag.put(id,df)
      context.previousStepResult = context.previousStepResult :+ Right(true)
    }
    context
  }
}
