name := "spark-streaming-tutorial"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.6",
  "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.2.0"
)