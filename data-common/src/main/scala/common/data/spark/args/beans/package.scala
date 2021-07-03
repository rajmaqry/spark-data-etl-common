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
                  .withDefault(false).stringConfig.createWithDefaultBoolean)
    .setRequired(true)
    .withValueType(classOf[java.util.ArrayList[_]]).stringConfig.createWithDefaultString
}
