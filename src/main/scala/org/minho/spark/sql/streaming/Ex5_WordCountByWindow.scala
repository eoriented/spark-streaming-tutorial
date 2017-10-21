package org.minho.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object Ex5_WordCountByWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WordCountByWindow")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()
      .toDF("words", "ts")

    val words = lines.select(explode(split('words, " ")).as("word"), window('ts, "10 seconds", "5 seconds").as("time"))
    val wordCount = words.groupBy("time", "word").count

    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete)
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
