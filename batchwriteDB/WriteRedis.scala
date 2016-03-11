package batchwriteDB

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf,SparkContext}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object WriteRedis {
  def writeToRedis(args:Array[String]):Unit = {
    if(args.length < 4){
      System.err.println("Usage: <StoreOriginalRecommendHdfsDir> <KeyHead> <redis host> <redis port>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setAppName("write Redis")
    val sc = new SparkContext(conf)

    val cale = Calendar.getInstance
    val caledate = cale.getTime
    val dateformat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateformat.format(caledate)

    val hdfsDir = args(0)

    val data = sc.textFile(hdfsDir+"/vdate="+date,100).map(_.trim.split("\t")).map(x=>(x(0),(x(1),x(2))))

    val keyHead = args(1)
    val redisHost = args(2)
    val redisPort = args(3).toInt
    //    val keyHead = "UserCF"
    data.groupByKey().foreachPartition(partion=>{
      val config = new JedisPoolConfig()
      config.setMaxActive(50)
      config.setMaxIdle(10)
      config.setMaxWait(1000 * 5)
      val jedisPool = new JedisPool(config,redisHost,redisPort,1000 * 20)
      val jedis = jedisPool.getResource

      partion.foreach(x=>{
        val pipeline = jedis.pipelined
        pipeline.del(keyHead+":"+x._1)
        x._2.foreach{y=>{
          pipeline.zadd(keyHead+":"+x._1,y._2.toDouble,y._1)
        }
        }
        pipeline.expire(keyHead+":"+x._1, 24*3600*2)
        pipeline.sync()
      })
      jedis.disconnect()
      if (jedis != null) {
        jedisPool.returnResourceObject(jedis)
      }
    })
    sc.stop()
  }

  def main(args:Array[String]):Unit = {
    writeToRedis(args)
  }
}
