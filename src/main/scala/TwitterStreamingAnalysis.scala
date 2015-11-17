
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.collection.mutable
import scala.io.Source

/**
 * Created by saroj on 11/16/15.
 */
object TwitterStreamingAnalysis {

  val posWords = Source.fromURL(getClass.getResource("/pos-words.txt")).getLines()
  val negWords = Source.fromURL(getClass.getResource("/neg-words.txt")).getLines()
  val stopWords = Source.fromURL(getClass.getResource("/stop-words.txt")).getLines()

  val posWordsArr = mutable.MutableList("")
  val negateWordsArr = mutable.MutableList("")

  for(posWord <- posWords)
    posWordsArr+=(posWord)

  for(negWord <- negWords){
    negateWordsArr+=(negWord)
  }
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }




    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    //val filters = Array("iTunes","spark") // This is for any specific filter you want to search the tweets
    val filters = args.takeRight(args.length - 4)  // This is for any tweets
    val sparkConf = new SparkConf().setAppName("TwitterSentimentAnalysis")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val sentiments=stream.map(status=> findTweetSentiment(status.getText().toLowerCase()))
    val pairs = sentiments.map(sentiment=>(sentiment, 1))
    val sentimentCountsInEvery10Secs = pairs.reduceByKeyAndWindow(_ + _,Seconds(10))
    val sentimentCountsInEvery30Secs = pairs.reduceByKeyAndWindow(_ + _,Seconds(30))
    sentimentCountsInEvery10Secs.print() // printing the tweets in every 10 secs

    sentimentCountsInEvery30Secs.print() //printing the tweets in every 30 secs

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Custom function to find whether the tweet is positive or negative or neutral
   * Argument is String
   */


  def findTweetSentiment(tweet:String):String = {
    var count = 0
    for(w <- tweet.split(" ")){
      for (positiveW <- posWordsArr) {
        if (w != "" && positiveW.toString.toLowerCase() == w) {
          count = count + 1
        }
      }

      for (negativeW <- negateWordsArr) {
        if (w != "" && negativeW.toString.toLowerCase() == w) {
          count = count - 1
        }
      }
    }
    if(count>0){
      return "positivie"
    }
    else if(count<0) {
      return "negative"
    }
    else
      return "neutral"
  }


}
