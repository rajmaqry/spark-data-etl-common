package common.data.spark.context

import java.util.concurrent.ConcurrentHashMap

import common.data.spark.args.beans.DataEtlSubmitArguments
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.DataPipeline
import common.data.spark.util.SparkSessionFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.Seq

/**
 * This is the core class to hold the contextual data in one place.
 * Through out this ETL process steps will refer to this contextual instance to refer to the variables
 * necessary for the process.
 *
 * This will hold the instance of [[DataEtlSubmitArguments]] and [[SparkSession]]
 * This will also hold the latest [[DataFrame]] processed from the previous step. And after processing
 * new [[DataFrame]] will be added to the data frame referring to the ID of the data set.
 *
 * This class will record the task count {{dfLevel}} on the [[DataFrame]] so that driver can perform action if
 * the task count level is high.
 *
 * @param args
 * @param dataEnv
 * @param sysEnv
 */
class ProcessContext(args : Seq[String], dataEnv: String = DataPipeline.DEV, sysEnv : Map[String,String] = sys.env)
  extends Logging {

  var arg:DataEtlSubmitArguments = new DataEtlSubmitArguments(args,dataEnv,sysEnv)
  var data_bag = new ConcurrentHashMap[String, DataFrame]()
  var sc = SparkSessionFactory.getSparkSession(arg.applicationName, true,  dataEnv)
  var dfLevel = 0
  var previousStepResult = List.empty[Either[String,Boolean]]
}
