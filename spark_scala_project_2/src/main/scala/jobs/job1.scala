package jobs

import config.DevConfig
import org.apache.spark.sql.SparkSession

object job1 {
  def main(args : Array[String]) : Unit =  {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    val sparkSession2 = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample-test")
      .getOrCreate();

    println("Second SparkContext:")
    println("APP Name :" + sparkSession2.sparkContext.appName);
    println("Deploy Mode :" + sparkSession2.sparkContext.deployMode);
    println("Master :" + sparkSession2.sparkContext.master);

    //MODULE1
    println("*************MODULE1*******************")
    val envConfig: DevConfig = DevConfig.apply("dev", "module1")
    println(envConfig.url + "\n" + envConfig.source + "\n" + envConfig.target)
    print("")
  }
}