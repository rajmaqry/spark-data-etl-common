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
      var path = context.arg.inputPath
      val id = file.get(ConfigConstants.ID)
      val header = file.getOrDefault(ConfigConstants.HEADER, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.HEADER)).head.defaultValueBoolean.toString)
      val del = file.getOrDefault(ConfigConstants.DELIMITER, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.DELIMITER)).head.defaultValueString.toString)
      val fileType = file.getOrDefault(ConfigConstants.FILE_TYPE, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.FILE_TYPE)).head.defaultValueString.toString)
      val dataSet = file.getOrDefault(ConfigConstants.DATA_SET, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.DATA_SET)).head.defaultValueString.toString)
      val schema = FileSystemUtility.getSchema(dataSet)
      if( schema!= null ||  schema.length > 0) { path = dataSet}
      else if (dataSet.length > 1) {path += BaseConstants.FOR_SLASH + dataSet}
      if (fileType.equalsIgnoreCase("parquet")){
            df = context.sc.read.parquet(path).na.fill("")
      }else{
            df = context.sc.read.format(fileType).option("delimiter",del).option("header",header)
              .option("ignoreLeadingWhiteSpace", true)
              .option("ignoreTrailingWhiteSpace", true).load(path).na.fill("")
      }
      context.data_bag.put(id,df)
      context.previousStepResult = context.previousStepResult :+ Right(true)
      logger.info(s"completed reading file from path shared: $path and with id :$id")
    }
    context
  }
}
