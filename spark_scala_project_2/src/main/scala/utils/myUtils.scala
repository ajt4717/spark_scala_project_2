package utils

import commons.mySparkSession
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl
import org.apache.spark.sql.{DataFrame, SparkSession}

object myUtils {
  def getHiveSession(session : mySparkSession) : HiveWarehouseSessionImpl = {
    val hwc = HiveWarehouseSession.session(session.spark).build()
    hwc
  }

  def importOracleData(spSession : SparkSession, url1 : String, user1 : String, password1 : String, sqlQuery : String) : DataFrame = {
    spSession.read.format("jdbc")
      .option("url",url1)
      .option("user",user1)
      .option("password",password1)
      .option("query",sqlQuery)
      .option("driver","oracle.jdbc.driver.OracleDriver")
      .option("fetchsize",100000)
      .load()
  }

  def importMysqlData(spSession: SparkSession, url1: String, user1: String, password1: String, sqlQuery: String): DataFrame = {
    spSession.read.format("jdbc")
      .option("url", url1)
      .option("user", user1)
      .option("password", password1)
      .option("query", sqlQuery)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("fetchsize", 100000)
      .load()
  }

  def saveToHive(df : DataFrame, dbName : String, tableName : String, partition : Int) : Unit = {
      try{
        df.repartition(partition).write.mode("insert").saveAsTable(dbName + "." + tableName)
      }
    catch{
      case exception : Throwable => {}
    }
  }

}
