from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Ejemplo Básico Spark") \
    .getOrCreate()

print("Sesión de Spark creada")

# Leer un archivo CSV
df = spark.read.csv("/home/cristianchiera/Documentos/clientes_q.csv", header=True, inferSchema=True)

# Mostrar las primeras filas
#df.show()

# Filtrar productos con cantidad mayor a 5
#df.filter(df["Fallecido"] == 'SI').show()

#Seleccionar columnas
#df.select("Cuil", "Localidad").show()

# Calcular el total (cantidad * precio)
#df.withColumn("Nuevo_Dato", df["Cuil"] /10000000).show()

#Agrupacion y agregacion
#df.groupBy("Provincia").agg({"item": "count"}).show()

#Ordenar los datos:
df.orderBy(df["Genero"].desc()).show()


df.write.csv("/home/cristianchiera/Documentos/transforms_clientes_q", header=True)
df.write.parquet("/home/cristianchiera/Documentos/transforms_clientes_q_parquet")

df_parquet = spark.read.parquet("/home/cristianchiera/Documentos/transforms_clientes_q_parquet")
df_parquet.show()
