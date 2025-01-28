from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, substring, lit, when

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("CSV Column to Database") \
    .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:9.4.0.jre8") \
    .getOrCreate()

# Leer el CSV
df = spark.read.csv("/home/user/Documentos/archivo.csv", header=True, inferSchema=True)

# Seleccionar solo la columna que quieres
df_una_columna = df.select("Cuil")  # Reemplaza "nombre_columna" con tu columna

df_transformado= df \
    .fillna("Sin Departamento", subset=["Departamento"]) \
    .fillna("Sin Localidad", subset=["Localidad"]) \
    .fillna("Sin Domicilio", subset=["Domicilio"]) \
    .fillna(0, subset=["Cuil"]) \
    .fillna("Sin Provincia", subset=["Provincia"]) \
    .withColumn("Cuil",  # Sobrescribir la columna existente
    when(
        col("Cuil") != "0",  # Condición: si Cuil no es igual a "0"
        concat(
            substring(col("Cuil"), 1, 2),  # Primeros 2 caracteres
            lit("-"),                      # Guion
            substring(col("Cuil"), 3, 8),  # Siguientes 8 caracteres
            lit("-"),                      # Guion
            substring(col("Cuil"), 11, 1)  # Último carácter
        )
    ).otherwise(col("Cuil"))  # Si Cuil es "0", mantener el valor original
)



# Configuración para SQL Server
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=nombre_de_la_db"
properties = {
    "user": "usuario",
    "password": "clave",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# Escribir en la base de datos
df_transformado.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, 
          table="spark_test_all_transformado",
          properties=properties)
