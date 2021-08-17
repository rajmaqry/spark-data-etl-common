package common.data.spark.reader

import java.util

import common.data.spark.args.beans.{DB_CON_INFO, RAW_FILE_INFO}
import common.data.spark.beans.logger.Logging
import common.data.spark.constant.DataConstant.ConfigConstants
import common.data.spark.context.ProcessContext
import common.data.spark.reader.FileTypeDataReader.logger
import common.data.spark.reader.api.DataReaderApi
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
class SparkJdbcReader extends DataReaderApi with Logging{
  /**
   * Abstract readData method to use Spark reading functions
   *
   * @param context : [[ParamContex]] class instance which will hold the contextual data around the process
   * @return
   */
  override def readData(context: ProcessContext): ProcessContext = {

    context
  }
}

object SparkJdbcReader extends Logging{

  def getDbUrl(dbConfig: util.LinkedHashMap[String, String]):String ={
    var dbServer = "db2";
    val driver = dbConfig.get(ConfigConstants.DRIVER_NAME)
    val hostPortSchema = dbConfig.get(ConfigConstants.HOST_PORT_SCHEMA)
    if (driver.contains("mysql")) {
      dbServer = "mysql";
    }
    if (driver.contains("SQLServerDriver")) {
      dbServer = "sqlserver";
    }
    "jdbc:" + dbServer + "://" + hostPortSchema;
  }

  def readDataUtil(context: ProcessContext, id:String = "" ): DataFrame ={
    logger.info(s"Executing task of reading data for : ${context.arg.applicationName}")
    val rawConfigs = context.arg.executionConfig.get(RAW_FILE_INFO.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String, String]]].asScala
    val file =  rawConfigs.filter(x => x.containsKey(id)).head
    val dbConKey = file.get(ConfigConstants.DB_CON_ID,"")
    val dbConfig = context.arg.executionConfig.get(DB_CON_INFO.key).asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String, String]]].asScala.filter(p => p.containsKey(dbConKey)).head
    val mapper = file.getOrDefault(ConfigConstants.DB_MAPPER,"")//DB_CON_INFO.childConfig.filter(p => p.key.equalsIgnoreCase(ConfigConstants.DB_MAPPER)).head.defaultValueBoolean.toString)
    val splitCol = file.getOrDefault(ConfigConstants.SPLIT_COLUMN,null)
    val schema = file.getOrDefault(ConfigConstants.SCHEMA_PATH,"")
    val dbUrl = getDbUrl(dbConfig)
    val tableSchema = file.getOrDefault(ConfigConstants.TABLE_SCHEMA,"")
    val dataStart = file.getOrDefault(ConfigConstants.TABLE_DATA_START,null)
    val dataEnd = file.getOrDefault(ConfigConstants.TABLE_DATA_END,null)
    val boundaryCol = file.getOrDefault(ConfigConstants.TABLE_BOUNDARY_COL,null)
    val qualifiedTableName = if("".equals(tableSchema)) dbConKey else tableSchema+"."+dbConKey
    val sparkJdbc = context.sc.read.format("jdbc").option("driver",dbConfig.get(ConfigConstants.DRIVER_NAME))
                                  .option("url", dbUrl)
                                  .option("user",dbConfig.get(ConfigConstants.DB_USER_NAME))
                                  .option("password", dbConfig.get(ConfigConstants.DB_PASSWORD))
                                  .option("fetchsize", 100000);
    if (boundaryCol != null && dataStart != null && dataEnd != null) {
      val query = "SELECT * FROM " + qualifiedTableName + " WHERE " + boundaryCol + " BETWEEN '" + dataStart + "' AND '" + dataEnd + "'"
      sparkJdbc.option("dbtable", String.format("(%s) tmp", query))
    }
    else sparkJdbc.option("dbtable", qualifiedTableName)
    if(null != splitCol){
      val minMaxQuery = String.format("SELECT MIN(%s) as MIN, MAX(%s) as MAX from %S",splitCol,splitCol,qualifiedTableName)
      val minMaxValue = context.sc.read.format("jdbc").option("driver",dbConfig.get(ConfigConstants.DRIVER_NAME))
        .option("url", dbUrl)
        .option("user",dbConfig.get(ConfigConstants.DB_USER_NAME))
        .option("password", dbConfig.get(ConfigConstants.DB_PASSWORD)).option("query", minMaxQuery)
        .load();
      var lowerBound = 0
      var upperBound = 0
      val row = minMaxValue.collectAsList.get(0)
      //if table is empty then min max can be null
      if (row.get(0) != null) lowerBound = row.getInt(0)
      if (row.get(1) != null) upperBound = row.getInt(1)
      sparkJdbc.option("partitionColumn",splitCol)
        .option("numPartitions",mapper)
        .option("upperBound",upperBound)
        .option("lowerBound",lowerBound)
    }
    sparkJdbc.load()
  }
}
