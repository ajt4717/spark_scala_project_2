package jobs

import commons.mySparkSession
import config.DevConfig
import org.apache.spark

object job2 {
  def main(args : Array[String]) : Unit = {
    val env : String = "dev"
    val module : String = "module1"

    val envConfig : DevConfig = DevConfig(env,module)

    val sparkSessionSetup : mySparkSession = mySparkSession.initiateSparkSession()
    sparkSessionSetup.setSparkSession
    val sparkSession = sparkSessionSetup.getSparkSession

    println("*************** MY SPARK SESSION ***************")
    println("APP Name :" + sparkSession.sparkContext.appName);
    println("Deploy Mode :" + sparkSession.sparkContext.deployMode);
    println("Master :" + sparkSession.sparkContext.master);
  }
}
