package common.data.spark.constant

object DataConstant {

case object DataPipeline{
 val ENV = "env"
 val DEV = "dev"
 val LOCAL = "local"

 //config constants
}
 case object BaseConstants{
   val FOR_SLASH = "/"
 }
 case object ConfigConstants{
  val APPLICATION_NAME = "application_name"
  val DESCRIPTION = "description"
  val EMAIL_ID = "email"

  val RAW_FILE_INFO = "raw_file_info"
  val ID = "id"
  val DATA_SET = "data_set"
  val FILE_TYPE = "file_type"
  val DELIMITER = "delimiter"
  val HEADER = "header_present"
  val MANIFEST = "manifest"
  val ID_COLUMN = "id_column"
  val VALIDATION = "validation"

  val SUPPORTED_FILES = Array("parquet","json","csv","txt")
 }
}
