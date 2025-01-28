from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Ejemplo Básico Spark") \
    .getOrCreate()

# Crear un DataFrame desde un flujo de texto
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Dividir el texto en palabras
words = lines.select(
    explode(split(lines.value, " ")).alias("word")
)

# Contar palabras
word_counts = words.groupBy("word").count()

# Escribir la salida en la consola
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Esperar a que termine el streaming
try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()