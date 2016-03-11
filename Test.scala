package cf

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.{SparkConf, SparkContext}

object Test extends App{
  val prop = new Properties()
  prop.load(new FileInputStream("parameters.properties"))

  val sc = new SparkContext()

  System.out.println(prop.getProperty("database"))
  System.out.println(prop.getProperty("dbuser"))
  System.out.println(prop.getProperty("dbpassword"))
}
