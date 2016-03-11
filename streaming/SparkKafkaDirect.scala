package streaming

import java.io.File
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}
import org.json4s.jackson.JsonMethods._

object SparkKafkaDirect {

  def main(args: Array[String]) {

    if(args.length!=3){
      System.err.println("Usage: SparkKafkaDirect <checkPointDir> <redis host> <redis port>")
      System.exit(1)
    }
    val sc = new SparkContext()
    val checkPointDir = args(0)
    val redisHost = args(1)
    val redisPort = args(2).toInt
    val ssc = functionToCreateContext(sc,checkPointDir,redisHost,redisPort)

    ssc.start()
    ssc.awaitTermination()
  }
  def functionToCreateContext(sc:SparkContext,checkpointDirectory:String,redisHost:String,redisPort:Int): StreamingContext = {
    val outputFile = new File(checkpointDirectory)
    if (outputFile.exists()) outputFile.delete()

    val ssc = new StreamingContext(sc,Seconds(3))   // new context
    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory

    val topics = Set("your topics")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "your broker list example host:port,host1:port")
    val inputs = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    inputs.foreachRDD(rdd => {
      rdd.foreachPartition {
        partitionOfRecords => {
          val config = new JedisPoolConfig
          config.setMaxActive(100)
          config.setMaxIdle(10)
          config.setMaxWait(1000 * 10)

          val jedisPool = new JedisPool(config, redisHost, redisPort, 1000 * 20);

          val jedis = jedisPool.getResource
          partitionOfRecords.map(x => ((parse(x) \ "UserId"), (parse(x) \ "LoginDateTime"))).foreach(y => {
            jedis.del(y._1.values.toString)
            jedis.set(y._1.values.toString, y._2.values.toString)
            jedis expire(y._1.values.toString, 1 * 3600)
            println(y._1.values.toString + " : " + y._2.values.toString)
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