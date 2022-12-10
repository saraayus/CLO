from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, to_date, col, month

# Creamos la sesion de spark
conf = SparkConf().setAppName('Fecha')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Creamos el DataFrame a partir de un .csv
path = "./itineraries.csv"
fechaDF = spark.read.option("header", "true").csv(path)

# Convertimos a tipo date los datos de las columnas searchDate y flightDate
fechaDF = fechaDF.withColumn("searchDate", to_date(fechaDF["searchDate"], "yyyy-MM-dd"))
fechaDF = fechaDF.withColumn("flightDate", to_date(fechaDF["flightDate"], "yyyy-MM-dd"))
# Creamos una nueva columna en el DF a partir de la diferencia de dias entre las fechas de la busqueda y vuelo
fechaDF = fechaDF.withColumn("Days between", datediff("flightDate", "searchDate"))

# Creamos un DF nuevo solo con las columnas que nos interesan
df = fechaDF.select(col("searchDate"), col("flightDate"), col("Days between"))
# Creamos una nueva columna en el nuevo DF con el mes del vuelo y lo mostramos
df = df.withColumn("Flight Month", month("flightDate"))
df.show()

# Contamos el numero de vuelos que se producen en cada mes y lo mostramos ordenado por mes
monthDF = df.groupby("Flight Month").count()
monthDF.sort("Flight Month").show()
