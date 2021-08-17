package common.data.spark.engine.driver

import common.data.spark.args.beans.RAW_FILE_INFO
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.ConfigConstants
import common.data.spark.context.ProcessContext
import common.data.spark.reader.api.DataReaderApi
import common.data.spark.util.ConfigUtil
import common.data.spark.validation.input.api.{InputDataValidationApi, InputPreValidatorApi}


class DriverStateMachine(context:ProcessContext) extends Logging{
  //raw file pre-validation API
  val preValidator:InputPreValidatorApi = InputPreValidatorApi(context,"")
  // raw file reader API based on the type.
  val dataReader:DataReaderApi = DataReaderApi(context,"")
  // data validation API based on type.
  val dataValidator: InputDataValidationApi = InputDataValidationApi(context,"")

  preValidator >> dataReader >> dataValidator
  def execute():Unit = {

    /*var df = context.sc.read.option("delimiter","#").csv("C:\\elements\\test.txt")
    //var df = context.sc.read.json("C:\\\\elements\\\\self\\\\aspiring\\\\gtu\\\\project\\\\spark\\\\spark\\\\examples\\\\src\\\\main\\\\resources\\\\people.json")
    val m = df.columns
    //df.rdd.map(x => (x,"1")).reduceByKey((x,y)=> x+y).saveAsTextFile("C:\\elements\\testnow\\test1.txt")
    var l = df.rdd.map(x =>{
      val a = x.get(0).toString
      var i = 0
      var o = ""
      for(c<-m){
        o = o + "::" + x.get(i).toString
        i +=1
      }
      (a,o)
    }).reduceByKey((x,y) => x+y)
    l.saveAsTextFile("C:\\elements\\testnow\\test3")*/
    context.startAt(preValidator)
    context.run()
  }
}
