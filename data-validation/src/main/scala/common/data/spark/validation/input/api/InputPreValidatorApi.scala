package common.data.spark.validation.input.api

import common.data.spark.context.ProcessContext
import common.data.spark.validation.input.InputDataPreValidation

trait InputPreValidatorApi {
  def execute(context: ProcessContext): ProcessContext ={
    validateFileExist(context)
    validateFileExtension(context)
    validateIdColumn(context)
    validateManifestExist(context)
    context
  }

  /**
   *  Abstract definition to verify if file exists in the given input path
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  def validateFileExist(context: ProcessContext): ProcessContext
  /**
   *  Abstract definition to verify if file extension is valid in the given input path
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  def validateFileExtension(context: ProcessContext): ProcessContext
  /**
   *  Abstract definition to verify if file has the ID column mentioned in configuration
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  def validateIdColumn(context: ProcessContext): ProcessContext
  /**
   *  Abstract definition to verify if manifest file is present in the path mentioned.
   * @param context : [[ProcessContext]] will be updated with validation result
   * @return
   */
  def validateManifestExist(context: ProcessContext): ProcessContext
}

object InputPreValidatorApi {
  def apply(context:ProcessContext, validationType: String = ""): Unit ={
    validationType match {
      case _ => new InputDataPreValidation(context)
    }
  }

}