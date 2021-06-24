package common.data.spark.engine.main

import common.data.spark.args.beans.DataEtlSubmitArguments
import common.data.spark.beans.logger.Logging

import common.data.spark.args.beans.APPLICATION_NAME


object DataEngine_1 extends Logging{

  def parseArguments(args: Array[String]):  DataEtlSubmitArguments= {
    new DataEtlSubmitArguments(args)
  }

  def main(args: Array[String]): Unit = {
    val appArgs = parseArguments(args)
    appArgs.toString
    logger.info(APPLICATION_NAME.key)
    logger.info(appArgs.executionConfig.get(APPLICATION_NAME.key))
    logger.info(s"Hey it is done$args")
  }
}
