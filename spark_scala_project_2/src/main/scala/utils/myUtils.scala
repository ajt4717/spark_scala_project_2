package utils

import commons.mySparkSession
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl

object myUtils {
  def getHiveSession(session : mySparkSession) : HiveWarehouseSessionImpl = {
    val hwc = HiveWarehouseSession.session(session.spark).build()
    hwc
  }
}
