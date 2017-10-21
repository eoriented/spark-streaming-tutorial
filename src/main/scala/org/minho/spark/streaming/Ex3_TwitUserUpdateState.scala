package org.minho.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Ex3_TwitUserUpdateState {
  def main(args: Array[String]): Unit = {
    val twitterAuth = new TwitterAuth(args(0))
    twitterAuth.setTwitterApps()

    val conf = new SparkConf().setAppName("twitPerSec").setMaster("local[2]")
    val slideInterval = new Duration(1 * 1000)
    val ssc = new StreamingContext(conf, slideInterval)
    ssc.sparkContext.setLogLevel("Error")

    // set checkpoint directory
    ssc.checkpoint("./tmp")

    // update state function
    val updateFunc = (newValues: Seq[Long], currentValue: Option[Long]) => Option(currentValue.getOrElse(0L) + newValues.sum)

    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
    val twitterStream = TwitterUtils.createStream(ssc, auth)
      .map(x => x.getText)
      .map(name => (name, 1L))
      .updateStateByKey(updateFunc)

    twitterStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
