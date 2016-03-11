package batchwriteDB

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}


object WriteHbase {
  def writeToRedis(args:Array[String]):Unit = {
    if (args.length < 4) {
      System.err.println("Usage: <hdfsDir> <HBaseTable>")
      System.exit(1)
    }

    val conf = new SparkConf()
    conf.setAppName("write hbase")
    val sc = new SparkContext(conf)

    val cale = Calendar.getInstance
    val caledate = cale.getTime
    val dateformat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateformat.format(caledate)

    val hdfsDir = args(0)

    val data = sc.textFile(hdfsDir + "/vdate=" + date, 100).map(_.trim.split("\t")).map(x => (x(0), (x(1), x(2))))
    val tableName = args(1)

    data.groupByKey().foreachPartition{
      x =>{
        val myConf = HBaseConfiguration.create()
        myConf.set("hbase.zookeeper.quorum", "your zookeeper host")
        myConf.set("hbase.zookeeper.property.clientPort", "2181")
        myConf.set("hbase.defaults.for.version.skip", "true")
        val myTable = new HTable(myConf, TableName.valueOf(tableName))
        myTable.setAutoFlush(false, false)
        myTable.setWriteBufferSize(2 * 1024 * 1024)
        x.foreach { y => {
          println(y._1.toString + ":::" + y._2.toString)
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.add("Dynamic".getBytes, "DynamicValue".getBytes, Bytes.toBytes(y._2.toString))
          myTable.put(p)
        }
        }
        myTable.flushCommits()
      }
        sc.stop()
    }
  }

  def main(args:Array[String]):Unit = {
    writeToRedis(args)
  }
}
