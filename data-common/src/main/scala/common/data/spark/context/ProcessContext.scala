package common.data.spark.context

import java.util.concurrent.ConcurrentHashMap

import common.data.distributed.storage.FileSystemUtility
import common.data.spark.api.BaseExecutionApi
import common.data.spark.args.beans.DataEtlSubmitArguments
import common.data.spark.beans.exceptions.DataProcessException
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
 * This context class will also be executing the API tasks by calling there base method of [[execute]] implemented from
 * [[BaseExecutionApi]]. Based on the relationship defined for task flow using [[common.data.spark.api.TaskMixin]] context willl
 * call the next execution API and execute, until relationship ends.
 *
 * State machines needs to set the [[startAt()]] variable in the context to identify where the execution starts.
 *
 * Each API will update the [[previousStepResult]] var with the message or boolean and that will decide if the execution
 * should proceed or not.
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
  var startAt: BaseExecutionApi = null

  def startAt(a: BaseExecutionApi): Unit = {
    startAt = a
  }

  def isRight(): Boolean = {
    var res = true
    if(previousStepResult.forall(_.isRight)) return res
    else res = false
    res
  }

  def logQa(outputPath:String = arg.outputPath, content:String = "", fileName:String = arg.applicationName.toUpperCase()+"_QA_INFO.txt"): Unit = {
    val qa = (previousStepResult.collect{
      case Left(v) => v
    }).mkString("\n")
    logger.info(qa)
    FileSystemUtility.writeContent(outputPath,qa,fileName)
  }

  /**
   *  The entry point to start the context execution.
   *  The startAt should be defined in order to start the execution.
   *  For each API relatives this will call the execute method and check the previousStepResult after execution
   *  If the relatives end at one API, execution will stop
   *  If previousStepResult is all Right then execution will continue.
   */
  def run(): Unit = {
    if (startAt == null) {
      logger.error(s"This indicates that context is been set with start at variable, set the node of execution with startAt mehtod")
      throw new  DataProcessException(s"Root level for this execution is not been defined in context")
    }
    var con = true
    var end = false
    while(con && !end){
      //set the next relative of starting node
      var next = startAt.relatives
      //startAt.execute(this)
      logger.info(startAt.test)
      //validate execution ended in all Right
      con = isRight()
      //if starting node has no relative end it here
      if (next.isEmpty) {
        next = null
        end = true
      }
      //continue until relatives end
      while (next != null){
        var c = 0
          next.foreach(n => {
            //n.execute(this)
            logger.info(n.test)
            con = isRight()
            c += 1
            //possibility of attaching multiple relatives in one
            if(c == next.length){
              next = n.relatives
              //check if relative ends here
              if (next.isEmpty) {
                next = null
                end = true
              }
            }
          })
      }//end as relatives empty
    }//whole execution stops
    //this.logQa()
  }

}
