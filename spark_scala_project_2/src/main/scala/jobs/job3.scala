package jobs

import commons.{jdbcLoadIntoHive, mySparkSession}
import config.DevConfig
import sql.OracleToHiveSql
import utils.myUtils.{getHiveSession, saveToHive}

object job3 {
  def main(args : Array[String]) : Unit = {
    val env : String = "dev"
    val module : String = "module1"

    val envConfig : DevConfig = DevConfig(env,module)

    val sparkSessionSetup : mySparkSession = mySparkSession.initiateSparkSession()
    sparkSessionSetup.setSparkSession
    //sparkSessionSetup.setHiveserver2JdbcUrl("dummyHiveJdbcUrlFromDevConfigFile")
    //sparkSessionSetup.setHiveserver2JdbcUrl("jdbc:hive2://192.168.1.148:10000")
    val sparkSession = sparkSessionSetup.getSparkSession

    println("*************** MY SPARK SESSION ***************")
    println("APP Name :" + sparkSession.sparkContext.appName);
    println("Deploy Mode :" + sparkSession.sparkContext.deployMode);
    println("Master :" + sparkSession.sparkContext.master);

    val hive = getHiveSession(sparkSessionSetup)
    //println("hive : " + hive)

    println("*************** READ JDBC DATA ***************")
    val oracleDataLoadSql : Map[String, (String, String)] = Map(
      OracleToHiveSql().table1_oracle -> (OracleToHiveSql().oracleTable1_sql, OracleToHiveSql().oracleTable1_transform),
      OracleToHiveSql().table2_oracle -> (OracleToHiveSql().oracleTable2_sql, OracleToHiveSql().oracleTable2_transform)
    )

    val objJdbc = new jdbcLoadIntoHive()
    objJdbc.jdbcLoadOracleDataIntoHive(sparkSession, envConfig, hive, oracleDataLoadSql)


    //objJdbc.jdbcLoadMysqlDataIntoHive(sparkSession, envConfig, hive, mysqlDataLoadSql)

  }
}

