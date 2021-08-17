package common.data.spark.api

trait  TaskMixin {

    def root(): Any
    def leaves(): Any
    def _setDownStream(a: TaskMixin) : AnyRef = { throw new Exception("jok")}
    def >>(a: TaskMixin) : AnyRef = { _setDownStream(a)}
}
