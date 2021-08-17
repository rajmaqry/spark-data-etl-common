package common.data.spark.reader.api

import common.data.spark.api.BaseExecutionApi

import common.data.spark.reader.GenericDataReader
import common.data.spark.context.ProcessContext

/**
 *  The abstract API around the Spark data read operations.
 *  The different Data Reader class can extend this and override it's own way of reading files
 */
trait DataReaderApi extends BaseExecutionApi{

   override def execute(context: ProcessContext): ProcessContext = readData(context)
  /**
   *  Abstract readData method to use Spark reading functions
   * @param context : [[ParamContex]] class instance which will hold the contextual data around the process
   * @return
   */
    def readData(context: ProcessContext): ProcessContext
}

object DataReaderApi{
  /**
   *
   * @param context : [[ParamContex]] class instance which will hold the contextual data around the process
   * @param readerType : It can extended to have different types defined extending [[DataReaderApi]] default is [[GenericDataReader]]
   * @return [[DataReaderApi]]
   */
  def apply(context: ProcessContext, readerType:String = ""): DataReaderApi ={
      readerType match{
        case _ => new GenericDataReader(context)
      }
  }
}