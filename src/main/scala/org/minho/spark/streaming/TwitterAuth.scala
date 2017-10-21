package org.minho.spark.streaming

import java.io.FileInputStream
import java.util.Properties

class TwitterAuth(resourcePath: String) {
  val prop = new Properties()
  try {
    prop.load(new FileInputStream(resourcePath))
  } catch {
    case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
  }

  private val CONSUMER_API_KEY = prop.getProperty("consumer.api.key")
  private val CONSUMER_API_SECRET = prop.getProperty("consumer_api_secret")
  private val ACCESS_TOKEN = prop.getProperty("access.token")
  private val ACCESS_TOKEN_SECRET = prop.getProperty("access.token.secret")


  def setTwitterApps() = {
    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_API_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_API_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ACCESS_TOKEN_SECRET)
  }
}
