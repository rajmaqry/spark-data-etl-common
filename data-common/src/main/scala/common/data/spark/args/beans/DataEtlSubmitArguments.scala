package common.data.spark.args.beans



import common.data.distributed.storage.FileSystemUtility

import common.data.spark.beans.exceptions.DataProcessException
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.{ConfigConstants, DataPipeline}
import common.data.spark.util.{ConfigurationValidator, DataSubmitArgumentParser, SparkSessionFactory}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor


import scala.collection.mutable.Seq
import scala.collection.JavaConverters._


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
  var executionConfig :ExecutionConfig = null

  //parse command arguments based on keys
  parse(args.asJava)

  //create execution configurations for context
  toExecutionConfig()

  //validate YAML keys
  validateConfigurations()
  def throwError(msg: String): Unit = throw new DataProcessException(msg)

  def _validateRawFileKeys(value: Any): Any = ???

  /**
   * This method validates the YAML configuration files.
   * It will use the [[ConfigBuilder]] from [[beans]] to get the
   * required status, child units and type of values
   * @param
   */
  def validateConfigurations(): Unit ={
    var result = List.empty[Either[String,Boolean]]
    //validate raw file info
    result = result :+ ConfigurationValidator.validateRawFileInfo(this.executionConfig)
    if(result.forall(_.isRight)){
      logger.info(s"Validation successful for the configuration data present in :$dataMetaPath")
    }else{
      logger.error((result.collect{
        case Left(v) => v
      }).mkString("\n"))
      throwError(s" Improper deceleration in YAML file : $dataMetaPath")
    }
  }
  /**
   *  This method to convert the YAML metadata configurations to  execution config class
   *  [[ ExecutionConfig]]
   *
   *  @param executionConfig : use an existing one or create new one
   */
  def toExecutionConfig(executionConfig: Option[ExecutionConfig] = None): ExecutionConfig = {
    logger.info(s"Trying to load configuration content from YAML file in path : $dataMetaPath")
    val content = FileSystemUtility.getFileContent(dataMetaPath)
    val yaml = new Yaml(new Constructor())
    var config = executionConfig.getOrElse(new ExecutionConfig)
    this.executionConfig = yaml.load(content).asInstanceOf[java.util.LinkedHashMap[String, Any]].asScala.foldLeft(config){
      case (config,(k,v) ) => config.set(k,v)
    }
    this.executionConfig
  }

  override protected def handle(opt: String, value: String): Boolean = {
    try{
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
        //metaConfig = toMetaConfig(dataMetaPath)
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
    }catch{
      case e: Exception  => logger.error(s"unaccepted argument or value for arg : $opt and passed value :$value")
        throw e
    }
    true
  }

  override def toString: String = {
    val s = s"""
    These are the configs created :
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
