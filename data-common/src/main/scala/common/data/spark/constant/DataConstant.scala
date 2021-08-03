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
  val FILE_EXIST_VAL = "file_exist"
  val RECORD_COUNT_VAL = "record_count"
  val NOT_NULL_CHECK_VAL = "not_null_check"
  val UNIQUE_ID_CHECK_VAL = "unique_id_check"
  val COLUMN_COUNT_CHECK_VAL = "column_count"
  val MANIFEST_TYPE = "type"
  val MANIFEST_PATH = "path"
  val MANIFEST_FORMAT = "format"
  val MANIFEST_DELIMITER = "delimiter"
  val MANIFEST_REC_COUNT_COL = "record_count_col"
  val MANIFEST_COL_COUNT_COL = "column_count_col"

  val SUPPORTED_FILES = Array("parquet","json","csv","txt")
  val SUPPORTED_MANIFEST_FILES = Array("csv","txt")
 }
}
