package commons

import org.apache.spark.sql.SparkSession

class mySparkSession {

  @transient var spark : SparkSession = _

  def setSparkSession {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("MySparkJob2")
      .getOrCreate();
  }

  def getSparkSession: SparkSession = {
    spark
  }

  def setHiveserver2JdbcUrl(url : String) : Unit = {
    spark.conf.set("spark.sql.hive.hiveserver2.jdbc.url",url)
  }

}

object mySparkSession {
  def initiateSparkSession() = new mySparkSession()
}
