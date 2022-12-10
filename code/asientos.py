from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import avg, col, desc


conf = SparkConf().setAppName('Asientos')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "./itineraries.csv"

#Devuelve un Dataframe con los datos del cs
df = spark.read.option("header", "true").csv(path)
df = df.select(df["seatsRemaining"].alias("REMAINING_SEATS").cast("int"), df["segmentsAirlineName"].alias("AIRLINE"))

#Agrupamos según aerolineas
#hacemos la media de valor de sitios vacios de cada grupo

dfGroup = df.groupBy("AIRLINE") \
    .agg(avg("REMAINING_SEATS").alias("AVG_REMAINING_SEATS")) \

#mostramos
dfGroup.show()
    

