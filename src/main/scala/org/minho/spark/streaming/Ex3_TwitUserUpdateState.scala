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

    // 작업 도중 상태 값이 유실되는 것을 방지하기 위한 저장소 설정
    // updateStateByKey와 같이 상태를 유지하는 연산을 다룰 때는 checkpoint를 지정해야 합니다.
    ssc.checkpoint("./tmp")

    // 상태 업데이트를 위한 함수 정의
    // currentValue 키의 상태값을 나타냅니다.
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
