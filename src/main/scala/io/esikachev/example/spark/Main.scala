package io.esikachev.example.spark

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

object Main{

  val logger = LogManager.getLogger(this.getClass())

  case class CLIParams(hiveHost: String = "")

  // Defining an Helloworld class
  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val parser = parseArgs("Main")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>

        // Configuration of SparkContext
        val conf = new SparkConf().setAppName("task-5")

        // Creation of SparContext and SQLContext
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        // Configuring hiveContext to find hive metastore
        hiveContext.setConf("hive.metastore.warehouse.dir", "/user/hive/warehouse")

        // ======= Reading files
        // Reading hive table into a Spark Dataframe
        val dfHive = hiveContext.sql("SELECT category, count(*) as cn1 from purchases group by category order by cn1 DESC LIMIT 10")
        logger.info("Reading hive table : OK")
        logger.info(dfHive.show())

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
      help("help") text "prints this usage text"

      opt[String]("hdfsHost") required() action { (data, conf) =>
        conf.copy(hiveHost = data)
      } text "hdfsHost. Example : hdfs://nn1.p2.prod.saagie.io:8020"
    }
  }
}
