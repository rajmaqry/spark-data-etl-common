package common.data.spark.util

import java.util

import common.data.spark.args.beans.{ConfigBuilder, ConfigEntry, ExecutionConfig, RAW_FILE_INFO}
import common.data.spark.constant.DataConstant.ConfigConstants
import scala.collection.JavaConverters._
/**
 *  Sealed trait for defining the validation methods for the [[ExecutionConfig]] data
 */
sealed trait ConfigValidator {

  def validateRawFileInfo(executionConfig: ExecutionConfig): Either[String,Boolean]

}

/**
 * --------------------------------------------------------------------------------
 *  Helper class to perform validations on the elements of the [[ExecutionConfig]]
 *  This will refer to the [[beans]] for each execution config built with [[ConfigBuilder]]
 *  That will allow this class to to call the child elements of each config using the variable
 *  [[ConfigBuilder._childConfig]]. Also it will get the required key using the reference of
 *  [[ConfigBuilder._required]] and then will validate if that is present or not.
 *  [[ConfigBuilder._valueType]] will allow to vaarify the child element values
 *  provided in the YAML file, such for boolean it should be true/false
 *
 *  Base [[org.yaml.snakeyaml.Yaml]] will load the variables as [[java.util.ArrayList[java.util.LinkedHashMap]]
 *  With usage of scala collection convert we can iterate the child elements like this to verify.
 *  {{
 *    val value = java.util.ArrayList[java.util.LinkedHashMap
 *    value.asScala.exists(_.containsKey(key))
 *  }}
 *  -------------------------------------------------------------------------------
 *
 */
object ConfigurationValidator extends ConfigValidator {

  private def isKeyInList(value: util.ArrayList[util.LinkedHashMap[String, String]], key: String): Boolean = {
    var res = false;
    import scala.util.control.Breaks._
    value.asScala.exists(_.containsKey(key))
    /* breakable {
       for (i <- value.asScala) {
         if (i.containsKey(key)) res = true
         break
       }
     }*/
    res
  }

  def _checkIfChildPresent(child: ConfigEntry[_], entry: ConfigEntry[String], value: util.ArrayList[util.LinkedHashMap[String, String]]): String = {
    var msg = ""
    if ( child.required && !value.asScala.exists(_.containsKey(child.key))){
      msg += s"=> missing required child item in the  parent key of : ${entry.key}"
    }
    msg
  }

  def _checkIfValidBoolean(child: ConfigEntry[_], entry: ConfigEntry[String], value: util.ArrayList[util.LinkedHashMap[String, String]]): String = {
    var msg = ""
    if (child.valueType.getClass == classOf[Boolean] ){
      val l = (value.asScala.filter(_.containsKey(child.key))).head
      if(!l.isEmpty){
        if (l.get(child.key).equalsIgnoreCase("true") || l.get(child.key).equalsIgnoreCase("false") ){
          msg += s"=> for a boolean type configuration the value must be true/false, invalid config for :${entry.key} --> ${child.key}"
        }
      }
    }
    msg
  }

  def _checkIfKeyIsPresent(value: util.ArrayList[util.LinkedHashMap[String, String]], key: String): Boolean = {
    value.asScala.exists(_.containsKey(key))
  }

  def _getKeyVal(value: util.ArrayList[util.LinkedHashMap[String, String]], key: String): String ={
    (value.asScala.filter(_.containsKey(key))).head.get(key)
  }

  def _checkIsvalidFileType(child: ConfigEntry[_], entry: ConfigEntry[String], value: util.ArrayList[util.LinkedHashMap[String, String]]): String = {
    var msg = ""
    if(!_checkIfKeyIsPresent(value, child.key) && !ConfigConstants.SUPPORTED_FILES.contains(_getKeyVal(value,child.key))){
      msg += s"=> unsupported file type in configuration, please provide among : ${ConfigConstants.SUPPORTED_FILES}"
    }
    msg
  }

  /**
   *  This is the private method to validate all the child elements under the key in YAML [[RAW_FILE_INFO]]
   *  This will iterate over the child elements and validate if required element is present,
   *  This will iterate over the child elements and validate if for boolean type value is true/false
   *
   * @param value
   * @return msg
   */
  private def _validateRawFileKeys(value: java.util.ArrayList[java.util.LinkedHashMap[String,String]]): String = {
    var msg = ""
    for(child <- RAW_FILE_INFO.childConfig){
      msg += _checkIfChildPresent(child,RAW_FILE_INFO,value)
      msg += _checkIfValidBoolean(child,RAW_FILE_INFO,value)
      if(child.key.equalsIgnoreCase(ConfigConstants.FILE_TYPE)){
        msg += _checkIsvalidFileType(child,RAW_FILE_INFO,value)
      }
    }
    msg
  }

  /**
   * This validation method does first level validation of the key [[RAW_FILE_INFO]] in the YAML file.
   * This will check if the key is present or not and if so the definition matches with expected [[RAW_FILE_INFO.valueType]]
   *
   * It uses private method [[_validateRawFileKeys()]] to determine child elements.
   * @param executionConfig
   * @return
   */
  override def validateRawFileInfo(executionConfig: ExecutionConfig): Either[String,Boolean] = {
    var msg = ""
    if(executionConfig.contains(RAW_FILE_INFO.key)){
      val value = executionConfig.get(RAW_FILE_INFO.key)
      value.getClass match {
        case RAW_FILE_INFO.valueType => msg += _validateRawFileKeys(value.asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]])
        case _ => msg += s"=> ${value.getClass} Improper deceleration under ${RAW_FILE_INFO.key} in YAML file \n"
      }
    }else{
      msg = s"=> raw_file_info is required for this ETL to proceed, please refer to the usage for definition, update file \n"
    }
    if(msg.length > 0) {
      Left(msg)
    }else{
      Right(true)
    }
  }
}
