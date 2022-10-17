package commons

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl
import config.DevConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import sql.OracleToHiveSql
import utils.myUtils.{importMysqlData, importOracleData, saveToHive}

class jdbcLoadIntoHive {
  def jdbcLoadOracleDataIntoHive(spSession : SparkSession, myConfig : DevConfig, hive : HiveWarehouseSessionImpl, sql : Map[String,(String,String)]) : Unit = {

    for (i <- 0 until sql.size) {
      val df = importOracleData(spSession, myConfig.url, myConfig.username, myConfig.password, sql(OracleToHiveSql().table1_oracle)._1)
      df.createOrReplaceTempView(OracleToHiveSql().table1_oracle)

      val df_target = spSession.sql(OracleToHiveSql().oracleTable1_transform + OracleToHiveSql().table1_oracle)
      saveToHive(df_target, myConfig.targetDb, OracleToHiveSql().table1_hive,100)
    }
  }

  def jdbcLoadMysqlDataIntoHive(spSession: SparkSession, myConfig: DevConfig, hive: HiveWarehouseSessionImpl, sql: String): Unit = {
    val df = importMysqlData(spSession, myConfig.url, myConfig.username, myConfig.password, sql)
    df
  }

}
