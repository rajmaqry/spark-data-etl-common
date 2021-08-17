package common.data.spark.engine.main

import common.data.spark.api.BaseExecutionApi
import common.data.spark.args.beans.DataEtlSubmitArguments
import common.data.spark.beans.logger.{Logging =>l}
import common.data.spark.context.ProcessContext
import common.data.spark.engine.driver.DriverStateMachine
import common.data.spark.args.beans.{ConfigBuilder, ConfigEntry, ExecutionConfig, RAW_FILE_INFO}


object DataEngine_1 extends l{

  def main(args: Array[String]): Unit = {
    val c = new ProcessContext(args)
    val m:DriverStateMachine = new DriverStateMachine(c)
    m.execute()
  }
  def test ={

  }
}
