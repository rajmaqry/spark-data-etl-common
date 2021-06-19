package common.data.spark.engine.main

import common.data.spark.args.beans.DataEtlSubmitArguments
import org.apache.logging.log4j.scala.Logging

object DataEngine_1 extends Logging{

  def parseArguments(args: Array[String]):  DataEtlSubmitArguments= {
    new DataEtlSubmitArguments(args)
  }

  def main(args: Array[String]): Unit = {
    val appArgs = parseArguments(args)
    appArgs.toString
  }
}
