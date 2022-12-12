from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import re

conf = SparkConf().setAppName('destinationAirports')
sc = SparkContext(conf = conf)

spark = SparkSession(sc)

df = spark.read.option("header", "true").csv(sys.argv[1])

df = df.filter(df['startingAirport'] == 'LAX').groupBy("destinationAirport").count().sort("count", ascending=False)
df.coalesce(1).write.csv(sys.argv[2])
df.show()