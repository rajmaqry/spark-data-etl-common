package common.data.spark.api

import common.data.spark.context.ProcessContext
import scala.collection.mutable.Seq
trait BaseExecutionApi extends  TaskMixin {
    /**
     * Base abstract method for API execution. Sub classes will expand this to it's own definition of
     * execution lines.
     * @param context : Current execution context
     * @type context [[ProcessContext]]
     * @return
     */
    def execute(context:ProcessContext) : ProcessContext
    var test:String = "sss"
    var relatives:List[BaseExecutionApi] = List.empty[BaseExecutionApi]

    override def root(): List[BaseExecutionApi] = List(this)
    override def leaves(): List[BaseExecutionApi] = List(this)

    private def _updateRelatives(a: TaskMixin): Unit = {
        relatives = relatives :+ a.asInstanceOf[BaseExecutionApi]
    }

    override def >>(a: TaskMixin): Unit = {
        _updateRelatives(a)
    }
}
