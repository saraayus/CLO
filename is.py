from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round

# Creamos la sesión de spark
conf = SparkConf().setAppName('Is')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Creamos el DataFrame a partir del archivo .csv
path = "./itineraries.csv"
isDF = spark.read.option("header", "true").csv(path)

# Creamos un nuevo DF a partir de las columnas que nos interesan
df = isDF.select(col("isBasicEconomy"), col("isRefundable"), col("isNonStop"))

# basicEconomy = df.groupBy('isBasicEconomy').agg(count("isBasicEconomy").alias("count")).withColumn('Porcentaje', col("count") / df.count() * 100)
# refundable = df.groupBy('isRefundable').agg(count("isRefundable").alias("count")).withColumn('Porcentaje', col("count") / df.count() * 100)
# nonStop = df.groupBy('isNonStop').agg(count("isNonStop").alias("count")).withColumn('Porcentaje', col("count") / df.count() * 100)

# Creamos 3 DF, analizando cada parametro, y añadiendo en cada uno una columna que calcule el porcentaje de cada uno sobre el total de vuelos
basicEconomy = df.groupBy('isBasicEconomy').agg(count("isBasicEconomy").alias("count")).withColumn('Porcentaje', round((col("count") / df.count() * 100), 2))
refundable = df.groupBy('isRefundable').agg(count("isRefundable").alias("count")).withColumn('Porcentaje', round((col("count") / df.count() * 100), 2))
nonStop = df.groupBy('isNonStop').agg(count("isNonStop").alias("count")).withColumn('Porcentaje', round((col("count") / df.count() * 100), 2))

# Mostramos los tres DF
basicEconomy.show()
refundable.show()
nonStop.show()
