package common.data.spark.validation.input.api

import common.data.spark.api.BaseExecutionApi
import common.data.spark.context.ProcessContext
import common.data.spark.validation.input.InputDataValidation
trait InputDataValidationApi extends  BaseExecutionApi{

  override def execute(context: ProcessContext): ProcessContext ={
    context
  }

  /**
   * Abstract method to validate if the input file exist in the given input path
   * @param context : [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  def validateFileExist(context: ProcessContext): ProcessContext

  /**
   * Abstract method validate the record count in input file against manifest file
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  def validateRecordCount(context:ProcessContext): ProcessContext

  /**
   * Abstract method to check if ID Column in input data has null entry or not. Only if id_column defined.
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  def validateNotNulIdColumn(context:ProcessContext): ProcessContext

  /**
   * Abstract method to ID column has unique values in it.
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  def validateUniqueIdColumn(context:ProcessContext): ProcessContext

  /**
   * Abstract method to validate the number of columns
   * @param context [[ProcessContext]]
   * @return [[ProcessContext]]
   */
  def validateColumnCount(context:ProcessContext): ProcessContext
}

object InputDataValidationApi{
    def apply(context: ProcessContext, validationType:String = ""): InputDataValidationApi = {
      validationType match {
        case _ => new InputDataValidation(context)
      }
    }
}