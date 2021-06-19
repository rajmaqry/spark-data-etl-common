package common.data.spark.args.beans

import common.data.spark.beans.exceptions.DataProcessException
import common.data.spark.constant.DataConstant.DataPipeline
import common.data.spark.util.DataSubmitArgumentParser
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.Seq
import scala.jdk.CollectionConverters._


/**
 * Parse and encapsulate the job submit arguments
 * @param args
 * @param dataEnv
 * @param sysEnv
 */
class DataEtlSubmitArguments(args : Seq[String], dataEnv: String = DataPipeline.DEV, sysEnv : Map[String,String] = sys.env)
  extends DataSubmitArgumentParser with Logging {
  var inputPath :String = null
  var outputPath :String = null
  var validationPath :String = null
  var reportPath :String = null
  var dataMetaPath :String = null
  var applicationName :String = null
  var joinDataPath :String = null
  var partition :Int = 0
  var hiveTable :String = null
  var jobId :String = null
  var metaConfig :MetaConfig = null

  //parse command arguments based on keys
  parse(args.asJava)


  def throwError(msg: String): Unit = throw new DataProcessException(msg)

  def toMetaConfig(dataMetaPath: String): MetaConfig = {
    null
  }

  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case INPUT_PATH =>
        inputPath = value
      case OUTPUT_PATH =>
        outputPath = value
      case VALIDATION_PATH =>
        validationPath = value
      case REPORT_PATH =>
        reportPath = value
      case META_CON_PATH =>
        dataMetaPath = value
        metaConfig = toMetaConfig(dataMetaPath)
      case APP_NAME =>
        applicationName = value
      case JOIN_DATA_PAth =>
        joinDataPath = value
      case PARTITION =>
        partition = value.toInt
      case HIVE_TABLE =>
        hiveTable = value
      case JOB_ID =>
        jobId = value
      case _ =>
        logger.info(s"unexpected argument : $opt")
        throwError(s"unexpected argument : $opt")
    }
    false
  }

  override def toString: String = {
    val s = s"""
                      These are the configs created : "
                       | -inputPath -> $inputPath
                       | -outputPath -> $outputPath
                       | -validationPath -> $validationPath
                       | -reportPath -> $reportPath
                       | -dataMetaPath -> $dataMetaPath
                       | -applicationName -> $applicationName
                       | -joinDataPath -> $joinDataPath
                       | -partition -> $partition
                       | -hiveTable -> $hiveTable
                       | -jobId -> $jobId
                  """.stripMargin
    logger.info(s)
    s
  }
}
