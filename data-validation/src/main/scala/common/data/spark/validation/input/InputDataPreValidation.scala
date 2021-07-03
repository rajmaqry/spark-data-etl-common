package common.data.spark.validation.input

import common.data.distributed.storage.FileSystemUtility
import common.data.spark.args.beans.RAW_FILE_INFO
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.{BaseConstants, ConfigConstants}
import common.data.spark.context.ProcessContext
import common.data.spark.validation.input.api.InputPreValidatorApi
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
class InputDataPreValidation(context: ProcessContext) extends InputPreValidatorApi with Logging{
  /**
   * method definition to verify if file exists in the given input path
   *
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  override def validateFileExist(context: ProcessContext): ProcessContext = {
    logger.info(s"Executing task of validating file existence for  : ${context.arg.applicationName}")
    val rawConfigs = context.arg.executionConfig.get(RAW_FILE_INFO.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]].asScala
    var valReport = ""
    for(file <- rawConfigs) {
      var path = context.arg.inputPath
      val id = file.get(ConfigConstants.ID)
      val dataSet = file.getOrDefault(ConfigConstants.DATA_SET, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.DATA_SET)).head.defaultValueString.toString)
      val schema = FileSystemUtility.getSchema(dataSet)
      if( schema!= null ||  schema.length > 0) { path = dataSet}
      else if (dataSet.length > 1) {path += BaseConstants.FOR_SLASH + dataSet}
      if(!FileSystemUtility.checkIfFileExist(path)){
        valReport += s"File Exist Validation : Invalid path is given in the ${context.arg.dataMetaPath}, under the raw_file definition of $id, file does not exist in path $path \n"
        context.previousStepResult = context.previousStepResult :+ Left(valReport)
      }else{
        context.previousStepResult = context.previousStepResult :+ Right(true)
      }
      logger.info(s"completed validating file from path shared: $path and with id :$id")
    }
    context
  }

  /**
   * method definition to verify if file extension is valid in the given input path
   *
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  override def validateFileExtension(context: ProcessContext): ProcessContext = {
    logger.info(s"Executing task of reading data for : ${context.arg.applicationName}")
    val rawConfigs = context.arg.executionConfig.get(RAW_FILE_INFO.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]].asScala
    var valReport = ""
    for(file <- rawConfigs) {
      var path = context.arg.inputPath
      val id = file.get(ConfigConstants.ID)
      val dataSet = file.getOrDefault(ConfigConstants.DATA_SET, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.DATA_SET)).head.defaultValueString.toString)
      val fileType = file.getOrDefault(ConfigConstants.FILE_TYPE, RAW_FILE_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.FILE_TYPE)).head.defaultValueString.toString)
      val schema = FileSystemUtility.getSchema(dataSet)
      if( schema!= null ||  schema.length > 0) { path = dataSet}
      else if (dataSet.length > 1) {path += BaseConstants.FOR_SLASH + dataSet}
      if(!(FileSystemUtility.getFileExtension(path).equalsIgnoreCase(fileType))){
        valReport += s"File Extension Validation : Invalid extension is given in the ${context.arg.dataMetaPath}, under the raw_file definition of $id, file extention of --$fileType-- does not exist in path $path \n"
        context.previousStepResult = context.previousStepResult :+ Left(valReport)
      }else{
        context.previousStepResult = context.previousStepResult :+ Right(true)
      }
      logger.info(s"completed validating extension of files from path shared: $path and with id :$id")
    }
    context
  }

  /**
   * method definition to verify if file has the ID column mentioned in configuration
   *
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
 override def validateIdColumn(context: ProcessContext): ProcessContext = context

  /**
   * method definition to verify if manifest file is present in the path mentioned.
   *
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  override def validateManifestExist(context: ProcessContext): ProcessContext = context
}
