package common.data.spark.api

trait  TaskMixin {

    def root(): Any
    def leaves(): Any
    def _setDownStream(a: TaskMixin) : Unit = { throw new Exception("jok")}
    def >>(a: TaskMixin) : Unit = { _setDownStream(a)}
}
