package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object KafkaReceiverMonitorComposition {
  def main(args: Array[String]): Unit = {

    if(args.length!=3){
      System.err.println("Usage: SparkKafkaDirect <checkPointDir> <redis host> <redis port>")
      System.exit(1)
    }
    val checkPointDir = args(0)
    val redisHost = args(1)
    val redisPort = args(2).toInt
    val ssc = StreamingContext.getOrCreate(checkPointDir,()=>functionToCreateContext(checkPointDir,redisHost,redisPort))
    ssc.start()
    ssc.awaitTermination()
  }

  def functionToCreateContext(checkpointDirectory:String,redisHost:String,redisPort:Int): StreamingContext = {
    val sparkConf = new SparkConf().setAppName("KafkaReceiverMonitor")
    sparkConf.set("spark.locality.wait","0")
    val ssc = new StreamingContext(sparkConf, Minutes(1)) // new context
    ssc.checkpoint(checkpointDirectory) // set checkpoint directory

    val zkQuorum = "your kafka zookeeper"

    val groupid = "spark-kafka"
//    val topics = Map("user-register" -> 6,"InboxMsgList"->20,"SendboxMsgList"->20,"login-history"->6)
    val imMessageTopics = Map("IMMessage"->10)
    val loginTopics = Map("login"->10)
    val registerTpoics = Map("register"->10)
    val photoTopics = Map("photo"->10)

    val imMessageKafkaStreams = KafkaUtils.createStream(ssc, zkQuorum, groupid, imMessageTopics, StorageLevel.MEMORY_AND_DISK_SER)
    val loginKafkaStreams =  KafkaUtils.createStream(ssc, zkQuorum, groupid, loginTopics, StorageLevel.MEMORY_AND_DISK_SER)
    val registerKafkaStreams = KafkaUtils.createStream(ssc, zkQuorum, groupid, registerTpoics, StorageLevel.MEMORY_AND_DISK_SER)
    val photoKafkaStreams = KafkaUtils.createStream(ssc, zkQuorum, groupid, photoTopics, StorageLevel.MEMORY_AND_DISK_SER)

/
    val imMessageWindows = imMessageKafkaStreams.countByWindow(Minutes(1),Minutes(1)).map(x=>("Kafka_IMMessage_Test",x))
    val loginWindows = loginKafkaStreams.countByWindow(Minutes(1),Minutes(1)).map(x=>("Kafka_Login_Test",x))
    val photoWindows = photoKafkaStreams.countByWindow(Minutes(1),Minutes(1)).map(x=>("Kafka_Photo_test",x))
    val inputsComposition = ssc.union(List(imMessageWindows,loginWindows,photoWindows).toSeq)
    inputsComposition.foreachRDD(rdd => {
      rdd.foreachPartition {
        partitionOfRecords => {
          val config = new JedisPoolConfig()
          config.setMaxActive(5)
          config.setMaxIdle(5)
          config.setMaxWait(1000 * 10)
          val jedisPool = new JedisPool(config, redisHost,redisPort, 1000 * 20)
          val jedis = jedisPool.getResource
          partitionOfRecords.foreach(y => {
//            jedis.set(y._1, y._2.toString)
//            jedis expire(y._1, 2 * 60)
            println(y._1+"::::"+y._2)
          }
          )
          if (jedis != null) {
            jedisPool.returnResourceObject(jedis)
          }
        }
      }
    })
    val registerWindows = registerKafkaStreams.countByWindow(Minutes(5),Minutes(1)).map(x=>("Kafka_Register_Test",x))
    registerWindows.foreachRDD(rdd => {
      rdd.foreachPartition {
        partitionOfRecords => {
          val config = new JedisPoolConfig()
          config.setMaxActive(5)
          config.setMaxIdle(5)
          config.setMaxWait(1000 * 10)
          val jedisPool = new JedisPool(config, redisHost, redisPort, 1000 * 20)
          val jedis = jedisPool.getResource
          partitionOfRecords.foreach(y => {
//            jedis.set(y._1, y._2.toString)
//            jedis expire(y._1, 2 * 60)
            println(y._1+"::::"+y._2)
          }
          )
          if (jedis != null) {
            jedisPool.returnResourceObject(jedis)
          }
        }
      }
    })
    ssc
  }
}
