package common.data.spark.util

import common.data.spark.constant.DataConstant
import common.data.spark.constant.DataConstant.DataPipeline
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession

object SparkSessionFactory extends Logging{

  /**
   *   Method to create or share current Spark Session
   *   @param appName
   */
  private def createOrGetSparkSession(appName: String): SparkSession = {
    logger.info(s"creating spark session for the application: $appName")
    val spark  = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.acl.default", "BucketOwnerFullControl")
    spark.sparkContext.hadoopConfiguration.set("fs.s3.canned.acl", "BucketOwnerFullControl")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.canned.acl", "BucketOwnerFullControl")

    spark
  }
  /**
   *   Method to create or share current Spark Session in local
   *   @param appName
   */
  def createOrGetLocalSparkSession(appName: String, enableHiveSupport: Boolean) : SparkSession = {
    val spark =
    if(enableHiveSupport){
      logger.info(s"Creating local spark session with hive enabled for app name :$appName")
         SparkSession
        .builder()
        .master("local[*}")
        .appName(appName)
        .enableHiveSupport()
        .getOrCreate()
    }else {
      logger.info(s"Creating local spark session with out hive enabled for app name :$appName")
         SparkSession
        .builder()
        .master("local[*}")
        .appName(appName)
        .getOrCreate()
    }
    spark
  }

  /**
   *   Generic utility method to create or share current Spark Session
   *
   *   @param appName
   *   @param enableHiveSupport
   *   @param env
   */
  def getSparkSession(appName : String , enableHiveSupport : Boolean , env : String = DataPipeline.ENV) :
   SparkSession = {
    "" match {
      case DataPipeline.DEV => createOrGetSparkSession(appName)
      case DataPipeline.LOCAL => createOrGetLocalSparkSession(appName, enableHiveSupport)
      case _ => createOrGetSparkSession(appName)
    }
  }

}
