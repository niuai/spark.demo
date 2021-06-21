package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object App {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val inputFile = args(1)

    val conf = new SparkConf().setAppName("App").setMaster(args(0))
    val session = SparkSession.builder.config(conf).getOrCreate()
    val sc = session.sparkContext
    val streamContext = new StreamingContext(sc, Seconds(2))
    val textFile = sc.textFile(inputFile)

    val inDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("127.0.0.1", 9000)
    inDStream.print()

    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    resultDStream.print()

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop()

    sc.stop()
  }
}