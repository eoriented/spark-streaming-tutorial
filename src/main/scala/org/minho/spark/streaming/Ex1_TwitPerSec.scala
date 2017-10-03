package org.minho.spark.streaming

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

class TwitterAuth(resourcePath: String) {
  val prop = new Properties()
  try {
    prop.load(new FileInputStream(resourcePath))
  } catch {
    case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
  }

  val CONSUMER_API_KEY = prop.getProperty("consumer.api.key")
  val CONSUMER_API_SECRET = prop.getProperty("consumer_api_secret")
  val ACCESS_TOKEN = prop.getProperty("access.token")
  val ACCESS_TOKEN_SECRET = prop.getProperty("access.token.secret")
}

object Ex1_TwitPerSec {
  def main(args: Array[String]): Unit = {
    val twitterAuthFilePath = args(0)
    val twitterAuth = new TwitterAuth(twitterAuthFilePath)

    System.setProperty("twitter4j.oauth.consumerKey", twitterAuth.CONSUMER_API_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", twitterAuth.CONSUMER_API_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", twitterAuth.ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", twitterAuth.ACCESS_TOKEN_SECRET)

    // Recompute the top hashtags every 1 second
    val conf = new SparkConf().setAppName("twitPerSec").setMaster("local[2]")
    val slideInterval = new Duration(1 * 1000)
    val ssc = new StreamingContext(conf, slideInterval)
    ssc.sparkContext.setLogLevel("Error")

    val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
    val twitterStream = TwitterUtils.createStream(ssc, auth)
    twitterStream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
