"""
Tweet preprocessing and sentiment analysis using textblob library

Referances:
- Stream Tweets in real-time (https://developer.twitter.com/en/docs/tutorials/stream-tweets-in-real-time)
- How to analyze the sentiment of your own Tweets (https://developer.twitter.com/en/docs/tutorials/how-to-analyze-the-sentiment-of-your-own-tweets)
- Apache Spark Streaming Tutorial: Identifying Trending Twitter Hashtags (https://www.toptal.com/apache/apache-spark-streaming-twitter)
- Topic Modeling and Sentiment Analysis on Twitter Data Using Spark (https://towardsdatascience.com/topic-modeling-and-sentiment-analysis-on-twitter-data-using-spark-a145bfcc433)
"""
# Import the necessary packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob

# Tweet preprocessing
def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# Tweet sentiment analysis Functions
# Polarity of words detection (positive, negative) 
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity

# Subjectivity of tweet detection 
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity

# Tweet Classification (positive, negative) 
def text_classification(words):
    
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    
    return words


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("Sentiment analysis using Spark").getOrCreate()
    
    # read the tweet data from socket
    lines = spark.readStream.format("socket").option("host", "").option("port", 5555).load()
    
    # Preprocess the data
    words = preprocessing(lines)
    
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    words = words.repartition(1)
    
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("parquet")\
        .option("path", "./parc")\
        .option("checkpointLocation", "./check")\
        .trigger(processingTime='60 seconds').start()
    
    query.awaitTermination()







