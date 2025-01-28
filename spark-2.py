from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Ejemplo Básico Spark") \
    .getOrCreate()


data = [("A", 10), ("B", 5), ("C", 8)]
rdd = spark.sparkContext.parallelize(data)


# Transformación: Mapear datos
result = rdd.map(lambda x: (x[0], x[1] * 2)).collect()
print(result)
