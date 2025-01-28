from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("CSV Column to Database") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Leer el CSV
df = spark.read.csv("/home/user/Documentos/archivo.csv", header=True, inferSchema=True)

# Seleccionar solo la columna que quieres
df_una_columna = df.select("nombre_columna")  # Reemplaza "nombre_columna" con tu columna

# Configuración para PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/nombre_db"
properties = {
    "user": "usuario",
    "password": "clave",
    "driver": "org.postgresql.Driver"
}

# Escribir en la base de datos
df_una_columna.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, 
          table="spark_test",
          properties=properties)