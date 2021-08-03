package common.data.spark.args

import java.util

import common.data.spark.constant.DataConstant._
package object beans {

   val APPLICATION_NAME =
    ConfigBuilder(ConfigConstants.APPLICATION_NAME)
    .doc("Define application name to be associated with all context, if this passed then" +
      "it will override the argument value").setRequired(false)
    .stringConfig
    .createWithOptional
    val DESCRIPTION =
    ConfigBuilder(ConfigConstants.DESCRIPTION)
    .doc("short description on this ETL job")
    .stringConfig
    .createWithOptional
    val EMAIL =
    ConfigBuilder(ConfigConstants.EMAIL_ID)
    .doc("user EMAIL id, for notifications purpose")
    .stringConfig
    .createWithOptional
    val RAW_FILE_INFO =
    ConfigBuilder(ConfigConstants.RAW_FILE_INFO)
    .doc("use this list of configuration to pass different input files to the pipeline job")
    .withChildren(ConfigBuilder(ConfigConstants.ID)
                  .doc("Individual file ID to be used for reference, need to be unique").withDefault("1")
                  .setRequired(true).stringConfig.createWithDefaultString)
    .withChildren(ConfigBuilder(ConfigConstants.DATA_SET)
                  .doc("Prefix paths under the passed input path in argument")
                  .setRequired(false).stringConfig.createWithOptional)
    .withChildren(ConfigBuilder(ConfigConstants.FILE_TYPE)
                  .doc("File extension type for this RAW file").withDefault("CSV")
                  .setRequired(true).stringConfig.createWithDefaultString)
    .withChildren(ConfigBuilder(ConfigConstants.DELIMITER)
                  .doc("Pass this if file_type is CSV, otherwise default value will be used as `|`")
                  .setRequired(false)
                  .withDefault("|").stringConfig.createWithDefaultString)
    .withChildren(ConfigBuilder(ConfigConstants.HEADER)
                  .doc("Pass as if header is present, otherwise default is false")
                  .setRequired(false)
                  .withDefault(false).booleanConfig.createWithDefaultBoolean)
    .withChildren(ConfigBuilder(ConfigConstants.MANIFEST)
                  .doc("pass the manifest file path with metadata of this file, valid for validation")
                  .setRequired(false).stringConfig.createWithOptional)
    .withChildren(ConfigBuilder(ConfigConstants.ID_COLUMN)
                  .doc("set the ID column of the data set, valid for validation")
                  .setRequired(false).stringConfig.createWithOptional)
    .withChildren(ConfigBuilder(ConfigConstants.VALIDATION)
                  .doc("Set this to be true if validations need to be done on the data set")
                  .setRequired(false)
                  .withDefault(false).booleanConfig.createWithDefaultBoolean)
    .setRequired(true)
    .withValueType(classOf[java.util.ArrayList[_]]).stringConfig.createWithDefaultString
    val VALIDATION =
     ConfigBuilder(ConfigConstants.VALIDATION)
       .doc("use this list of configuration to pass different validations to be done on input")
       .withChildren(ConfigBuilder(ConfigConstants.ID)
         .doc("Individual file ID to be used for reference, need to be unique").withDefault("1")
         .setRequired(true).stringConfig.createWithDefaultString)
       .withChildren(ConfigBuilder(ConfigConstants.FILE_EXIST_VAL)
         .doc("Set this to be true if validations need to be done for file exist")
         .setRequired(false)
         .withDefault(true).booleanConfig.createWithDefaultBoolean)
       .withChildren(ConfigBuilder(ConfigConstants.RECORD_COUNT_VAL)
         .doc("Set this to be true if validations need to be done for record count")
         .setRequired(false)
         .withDefault(false).booleanConfig.createWithDefaultBoolean)
       .withChildren(ConfigBuilder(ConfigConstants.NOT_NULL_CHECK_VAL)
         .doc("Set this to be true if validations need to be done for null check in ID column")
         .setRequired(false)
         .withDefault(false).booleanConfig.createWithDefaultBoolean)
       .withChildren(ConfigBuilder(ConfigConstants.UNIQUE_ID_CHECK_VAL)
         .doc("Set this to be true if validations need to be done for unique check in ID column")
         .setRequired(false)
         .withDefault(false).booleanConfig.createWithDefaultBoolean)
       .withChildren(ConfigBuilder(ConfigConstants.COLUMN_COUNT_CHECK_VAL)
         .doc("Set this to be true if validations need to be done for column count")
         .setRequired(false)
         .withDefault(false).booleanConfig.createWithDefaultBoolean)
       .withChildren(ConfigBuilder(ConfigConstants.MANIFEST)
         .doc("Set this to be true if validations need to be based on manifest file")
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_TYPE)
           .doc("Pass this value in YAML based on manifest type accepted values file/")
           .setRequired(true)
           .withDefault("file").stringConfig.createWithDefaultString)
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_PATH)
           .doc("Pass this value in YAML based on manifest file path")
           .setRequired(true)
           .withDefault("./manifest.csv").stringConfig.createWithDefaultString)
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_FORMAT)
           .doc("Pass this value in YAML based on manifest file format")
           .setRequired(true)
           .withDefault("csv").stringConfig.createWithDefaultString)
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_DELIMITER)
           .doc("Pass this value in YAML based on manifest file delimiter")
           .setRequired(true)
           .withDefault(",").stringConfig.createWithDefaultString)
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_REC_COUNT_COL)
           .doc("Pass this value in YAML based on manifest file position of column with Record count values of each file")
           .setRequired(false)
           .withDefault("_c5").stringConfig.createWithDefaultString)
         .withChildren(ConfigBuilder(ConfigConstants.MANIFEST_REC_COUNT_COL)
           .doc("Pass this value in YAML based on manifest file position of column with Column count values of each file")
           .setRequired(false)
           .withDefault("_c6").stringConfig.createWithDefaultString)
         .setRequired(false)
         .withDefault(false).withValueType(classOf[java.util.ArrayList[_]]).booleanConfig.createWithDefaultBoolean)
       .setRequired(false)
       .withValueType(classOf[java.util.ArrayList[_]]).stringConfig.createWithOptional
}
