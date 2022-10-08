package utils

import commons.mySparkSession

object myUtils {
  def getHiveSession(session : mySparkSession) : HiveWarehouseSessionImpl = {
    val hive = HiveWarehouseSession
  }
}
