from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, col, desc
import sys
from pyspark.sql.functions import split, explode

conf = SparkConf().setAppName('Aerolinea')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = sys.argv[1]

#Devuelve un Dataframe con los datos del cs
df = spark.read.option("header", "true").csv(path)

#Escogemos los datos que nos interesan
df = df.select(df["segmentsAirlineName"].alias("AIRLINE"))\
    .withColumn('AIRLINE',explode(split('AIRLINE',"\|\|")))

#Agrupamos según aerolineas
#contamos cuantos hay en cada grupo
#ordenamos de forma descendente

dfGroup = df.groupBy("AIRLINE") \
    .agg(count("*").alias("COUNT")) \
    .sort(col("COUNT").desc()) 

#mostramos el top    
dfGroup.show(5)

#volcamos a un csv los resultados
dfGroup.coalesce(1).write.csv(sys.argv[2])
