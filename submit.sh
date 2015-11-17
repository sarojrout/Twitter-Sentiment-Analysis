#!/bin/sh 

source ./env.sh

$SPARK_HOME/bin/spark-submit --class "TwitterStreamingAnalysis" --master local[*] ./target/scala-2.10/twitter-streaming-assembly-1.0.jar $CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET
