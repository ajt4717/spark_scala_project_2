package config

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

case class DevConfig(username : String,
                     password : String,
                     source : String,
                     target : String,
                     url : String,
                     targetDb : String
                    )

object DevConfig{
  def apply(env : String, module : String) : DevConfig = {
    val envConfig : Config = ConfigFactory.load().getConfig(s"app.$env")

    //common parameters
    val url = envConfig.getString("commonConf.targetUrl")
    val tgt = envConfig.getString("commonConf.targetTable")

    if (module == "module1") {
      val user = envConfig.getString("module1.user")
      val pwd = envConfig.getString("module1.password")
      val src = envConfig.getString("module1.source")
      val tgtDb = envConfig.getString("module1.targetDb")

      DevConfig(user,pwd,src,tgt,url,tgtDb)
    }

    else if (module == "module2") {
      val user = envConfig.getString("module2.user")
      val pwd = envConfig.getString("module2.password")

      DevConfig(user,pwd,"NA",tgt,url,"NA")
    }

    else
      DevConfig("NA","NA","NA","NA","NA","NA")

  }
}
