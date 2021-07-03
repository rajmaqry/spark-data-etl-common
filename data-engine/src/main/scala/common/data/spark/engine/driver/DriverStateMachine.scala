package common.data.spark.engine.driver

import common.data.spark.beans.logger.Logging
import common.data.spark.context.ProcessContext
import common.data.spark.reader.api.DataReaderApi


class DriverStateMachine(context:ProcessContext) extends Logging{
  //raw file pre-validation API
  val preValidator:InputPreVa = PreValidatorApi(context,"")
  // raw file reader API based on the type.
  val dataReader:DataReaderApi = DataReaderApi(context,"")

  def execute():Unit = {

    dataReader.execute(context)

  }
}
