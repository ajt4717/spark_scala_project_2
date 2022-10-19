package utils

import commons.{MyException, mySparkSession}
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

  def saveToHive(df : DataFrame, dbName : String, tableName : String, partition : Int) : Unit =
  {
    try{
      df.repartition(partition).write.mode("insert").saveAsTable(dbName + "." + tableName)
    }
    catch{
      //Ref
      //https://www.geeksforgeeks.org/scala-try-catch-exceptions/
      //https://stackoverflow.com/questions/49475236/scala-what-are-the-guarantees-of-the-catch-throwable

      case exception : Throwable => {
        MyException(this.getClass, "Failed while saving to hive", exception)
        System.exit(1)
      }
    }
  }

}