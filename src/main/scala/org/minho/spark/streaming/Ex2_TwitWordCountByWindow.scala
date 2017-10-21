package org.minho.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Ex2_TwitWordCountByWindow {
  def main(args: Array[String]): Unit = {
    val twitterAuth = new TwitterAuth(args(0))
    twitterAuth.setTwitterApps()

    val conf = new SparkConf().setAppName("twitPerSec").setMaster("local[2]")
    val slideInterval = new Duration(1 * 1000)
    val ssc = new StreamingContext(conf, slideInterval)
    ssc.sparkContext.setLogLevel("Error")

    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
    val twitterStream = TwitterUtils.createStream(ssc, auth)
      .map(x => x.getText)
      .window(Seconds(10), Seconds(1))
      .flatMap(strs => strs.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    twitterStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
