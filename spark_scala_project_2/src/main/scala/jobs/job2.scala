package jobs

import commons.mySparkSession
import config.DevConfig
import utils.myUtils.getHiveSession

object job2 {
  def main(args : Array[String]) : Unit = {
    val env : String = "dev"
    val module : String = "module1"

    val envConfig : DevConfig = DevConfig(env,module)

    val sparkSessionSetup : mySparkSession = mySparkSession.initiateSparkSession()
    sparkSessionSetup.setSparkSession
    //sparkSessionSetup.setHiveserver2JdbcUrl("dummyHiveJdbcUrlFromDevConfigFile")
    sparkSessionSetup.setHiveserver2JdbcUrl("jdbc:hive2://192.168.1.148:10000")
    val sparkSession = sparkSessionSetup.getSparkSession

    println("*************** MY SPARK SESSION ***************")
    println("APP Name :" + sparkSession.sparkContext.appName);
    println("Deploy Mode :" + sparkSession.sparkContext.deployMode);
    println("Master :" + sparkSession.sparkContext.master);

    val hive = getHiveSession(sparkSessionSetup)
    //println("hive : " + hive)
  }
}
