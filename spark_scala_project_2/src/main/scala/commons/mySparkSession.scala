package commons

import org.apache.spark.sql.SparkSession

class mySparkSession {

  var spark : SparkSession = _

  def setSparkSession {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("MySparkJob2")
      .getOrCreate();
  }

  def getSparkSession: SparkSession = {
    spark
  }

}

object mySparkSession {
  def initiateSparkSession() = new mySparkSession()
}
