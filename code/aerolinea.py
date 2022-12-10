from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, col, desc


conf = SparkConf().setAppName('Aerolinea')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)


path = "./itineraries.csv"


#Devuelve un Dataframe con los datos del cs
df = spark.read.option("header", "true").csv(path)

#Escogemos los datos que nos interesan
df = df.select(df["segmentsAirlineName"].alias("AIRLINE"))

#Agrupamos según aerolineas
#contamos cuantos hay en cada grupo
#ordenamos de forma descendente

dfGroup = df.groupBy("AIRLINE") \
    .agg(count("*").alias("COUNT")) \
    .sort(col("COUNT").desc()) 

#mostramos el top    
dfGroup.show(5)

#volcamos a un excel los resultados
dfGroup.to_excel("TopAerolineas.xlsx", index = False)



