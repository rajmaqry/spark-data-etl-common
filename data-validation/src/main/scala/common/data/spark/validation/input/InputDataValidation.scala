package common.data.spark.validation.input

import common.data.distributed.storage.FileSystemUtility
import common.data.spark.args.beans.{RAW_FILE_INFO, VALIDATION}
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.{BaseConstants, ConfigConstants}
import common.data.spark.context.ProcessContext
import common.data.spark.util.ConfigUtil
import common.data.spark.validation.input.api.InputDataValidationApi
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
class InputDataValidation(context: ProcessContext) extends  InputDataValidationApi with Logging{
  /**
   * Abstract method to validate if the input file exist in the given input path
   *
   * @param context : [[ProcessContext]]
   * @return [[ProcessContext]]
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
   * Abstract method validate the record count in input file against manifest file
   *
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  override def validateRecordCount(context: ProcessContext): ProcessContext =  {
    var msg: String = ""
    for(data <- context.data_bag.asScala){
      val id = data._1
      val df = data._2
      logger.info(s"Started to check validation of record count for data id : $id")
      val isManifest = ConfigUtil.getConfigVal(context,RAW_FILE_INFO,id,ConfigConstants.MANIFEST).asInstanceOf[Boolean]
      val isRecordCountCheck = ConfigUtil.getConfigVal(context,VALIDATION,id,ConfigConstants.RECORD_COUNT_VAL).asInstanceOf[Boolean]
      if(isManifest && isRecordCountCheck){
        logger.info(s"manifest is present for the data bag : $id and record count check is TRUE:: will identify it's values")
        val manifestDf = getManifestDf(context,id)
        var recordCountCol = ConfigUtil.getL2ConfigVal(context,VALIDATION,id,ConfigConstants.MANIFEST,ConfigConstants.MANIFEST_REC_COUNT_COL)
        if(recordCountCol == null ) recordCountCol = VALIDATION.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST)).head.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST_REC_COUNT_COL)).head.defaultValueString
        val expectedRecordCount = manifestDf.agg(sum(recordCountCol.toString)).first().get(0).asInstanceOf[Number].longValue()
        val actualRecordCount = df.count()
        if(expectedRecordCount != actualRecordCount){
          msg = s"Expected record count is not present in the input data set of ID: $id, actual count is: $actualRecordCount, and expected from manifest is : $expectedRecordCount"
          logger.info(msg = s"Expected record count is not present in the input data set of ID: $id, actual count is: $actualRecordCount, and expected from manifest is : $expectedRecordCount")
        }
      }
    }
    if(msg.length > 0){
      context.previousStepResult = context.previousStepResult :+ Left(msg)
    }else{
      context.previousStepResult = context.previousStepResult :+ Right(true)
    }
    logger.info("Completed record count check validation")
    context
  }

  /**
   * Abstract method to check if ID Column in input data has null entry or not. Only if id_column defined.
   *
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
 override def validateNotNulIdColumn(context: ProcessContext): ProcessContext = {
   var msg: String = ""
   for(data <- context.data_bag.asScala) {
     val id = data._1
     val df = data._2
     logger.info(s"Started to check validation of not null ID column for data id : $id")
   }
   context
 }

  /**
   * Abstract method to ID column has unique values in it.
   *
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  override def validateUniqueIdColumn(context: ProcessContext): ProcessContext = ???

  /**
   * Abstract method to validate the number of columns
   *
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  override def validateColumnCount(context: ProcessContext): ProcessContext = ???


  def getManifestDf(context:ProcessContext, id:String): DataFrame = {
    val manifest = ConfigUtil.getConfigVal(context,VALIDATION,id,ConfigConstants.MANIFEST).asInstanceOf[java.util.LinkedHashMap[String,String]]
    var path = manifest.getOrDefault(ConfigConstants.MANIFEST_PATH,VALIDATION.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST)).head.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST_PATH)).head.defaultValueString)
    var format = manifest.getOrDefault(ConfigConstants.MANIFEST_FORMAT,VALIDATION.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST)).head.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST_FORMAT)).head.defaultValueString)
    val delimiter = manifest.getOrDefault(ConfigConstants.MANIFEST_DELIMITER,VALIDATION.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST)).head.childConfig.filter(a => a.key.equalsIgnoreCase(ConfigConstants.MANIFEST_DELIMITER)).head.defaultValueString)
    if(path.startsWith("/")){
      val prefPath = ConfigUtil.getConfigVal(context,RAW_FILE_INFO,id,ConfigConstants.DATA_SET).asInstanceOf[java.util.LinkedHashMap[String,String]]
      val inPath = context.arg.inputPath
      path = inPath + prefPath + path
    }
    if (format.equalsIgnoreCase("text")) format = "csv"
    logger.info(s"Manifest path built for reading : $path:: For the data ID : $id")
    context.sc.read.format(format).option("delimiter",delimiter)
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true).load(path).na.fill("")
  }
}
