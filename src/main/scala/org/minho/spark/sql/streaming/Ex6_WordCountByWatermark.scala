package org.minho.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object Ex6_WordCountByWatermark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WatermarkSample")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .option("includeTimestamp", true)
      .load()
      .toDF("words", "timestamp")

    val words = lines.select(explode(split('words, " ")).as("word"), 'timestamp)

    val wordCount = words.withWatermark("timestamp", "5 seconds")
      .groupBy(window('timestamp, "10 seconds", "5 seconds"), 'word).count

    // When use complete mode, aggregate all data.
    val query = wordCount.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}
