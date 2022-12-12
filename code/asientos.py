from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import avg, col, desc, round
import sys

conf = SparkConf().setAppName('Asientos')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = sys.argv[1]

#Devuelve un Dataframe con los datos del cs
df = spark.read.option("header", "true").csv(path)
df = df.select(df["seatsRemaining"].alias("REMAINING_SEATS")\
    .cast("int"), df["destinationAirport"])

#Agrupamos según aerolineas
#hacemos la media de valor de sitios vacios de cada grupo
dfGroup = df.groupBy("destinationAirport") \
    .agg(round(avg("REMAINING_SEATS"), 2).alias("AVG_REMAINING_SEATS")).sort("AVG_REMAINING_SEATS", ascending=False)

#mostramos
dfGroup.show()

dfGroup.coalesce(1).write.csv(sys.argv[2])
