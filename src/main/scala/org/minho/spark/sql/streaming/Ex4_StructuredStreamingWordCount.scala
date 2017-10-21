package org.minho.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.streaming.OutputMode

object Ex4_StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    import spark.implicits._

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.select(explode(split('value, " ")).as("word"))
    val wordCount = words.groupBy("word").count

    val query = wordCount.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()

    query.awaitTermination()
  }
}
