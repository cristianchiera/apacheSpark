# Ejemplo 4: Operaciones con DataFrames y SQL
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, year

spark = SparkSession.builder \
    .appName("Operaciones Avanzadas") \
    .getOrCreate()

# Crear DataFrame de ejemplo
ventas_data = [
    ("2023-01-01", "Laptop", 1200, 2),
    ("2023-01-02", "Mouse", 25, 10),
    ("2023-01-01", "Teclado", 50, 5),
    ("2023-02-01", "Laptop", 1200, 1)
]

df_ventas = spark.createDataFrame(ventas_data, ["fecha", "producto", "precio", "cantidad"])

# 1. Crear vista temporal para usar SQL
df_ventas.createOrReplaceTempView("ventas")

# Consulta SQL
spark.sql("""
    SELECT 
        producto,
        SUM(precio * cantidad) as total_ventas
    FROM ventas 
    GROUP BY producto
    ORDER BY total_ventas DESC
""").show()

# 2. Misma operación usando API DataFrame
df_ventas.groupBy("producto") \
    .agg(sum(col("precio") * col("cantidad")).alias("total_ventas")) \
    .orderBy(col("total_ventas").desc()) \
    .show()