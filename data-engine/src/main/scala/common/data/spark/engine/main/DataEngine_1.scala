package common.data.spark.engine.main

import common.data.spark.args.beans.DataEtlSubmitArguments
import common.data.spark.beans.logger.Logging
import common.data.spark.context.ProcessContext
import common.data.spark.args.beans.{ConfigBuilder, ConfigEntry, ExecutionConfig, RAW_FILE_INFO}


object DataEngine_1 extends Logging{

  def main(args: Array[String]): Unit = {
    val c = new ProcessContext(args)
    val appArgs = c.arg

  }
}
