package common.data.spark.util

import common.data.spark.args.beans.{ConfigEntry, ExecutionConfig}
import common.data.spark.context.ProcessContext

import scala.collection.JavaConverters._
object ConfigUtil {

  def getKeyVal(context: ProcessContext, parent: ConfigEntry[_] = null,key:ConfigEntry[_]) : Any = {
    val executionConfig: ExecutionConfig = context.arg.executionConfig
    if(parent != null && executionConfig.contains(parent.key)) {
      val value = executionConfig.get(parent.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      if(value.asScala.exists(_.containsKey(key.key))){
        return value.asScala.filter(_.containsKey(key.key)).head.get(key.key)
      }
    }else if (executionConfig.contains(key.key)){
      val value = executionConfig.get(key.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      return value.asScala.filter(_.containsKey(key.key)).head.get(key.key)
    }
  }
  def getConfigVal(context: ProcessContext, parent: ConfigEntry[_] = null, keyval:String, subKey:String) : Any = {
    val executionConfig: ExecutionConfig = context.arg.executionConfig
    if(parent != null && executionConfig.contains(parent.key)) {
      val value = executionConfig.get(parent.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      if(value.asScala.exists(_.containsValue(keyval))){
        return value.asScala.filter(_.containsValue(keyval)).head.get(subKey)
      }
    }else if (executionConfig.contains(keyval)){
      val value = executionConfig.get(keyval).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      return value.asScala.filter(_.containsKey(keyval)).head.get(subKey)
    }
  }

  def getL2ConfigVal(context: ProcessContext, parent: ConfigEntry[_] = null, keyval:String, subKey:String,l1Key: String) : Any = {
    val executionConfig: ExecutionConfig = context.arg.executionConfig
    if(parent != null && executionConfig.contains(parent.key)) {
      val value = executionConfig.get(parent.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      if(value.asScala.exists(_.containsValue(keyval))){
        val l0_val = value.asScala.filter(_.containsValue(keyval)).head.get(subKey).asInstanceOf[java.util.LinkedHashMap[String,String]]
        return l0_val.get(l1Key)
      }
    }else if (executionConfig.contains(keyval)){
      val value = executionConfig.get(keyval).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,String]]]
      return value.asScala.filter(_.containsKey(keyval)).head.get(subKey)
    }
  }

}
