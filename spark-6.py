# Ejemplo 6: Operaciones con ventanas
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, dense_rank, lag, col

spark = SparkSession.builder \
    .appName("Operaciones Avanzadas") \
    .getOrCreate()

# Datos de ventas por región
ventas_region_data = [
    ("Norte", "2023-01", 1000),
    ("Norte", "2023-02", 1200),
    ("Sur", "2023-01", 800),
    ("Sur", "2023-02", 900),
    ("Este", "2023-01", 1300),
    ("Este", "2023-02", 1400)
]

df_ventas_region = spark.createDataFrame(ventas_region_data, ["region", "mes", "ventas"])

# Definir ventana
ventana_region = Window.partitionBy("region").orderBy("mes")

# Aplicar funciones de ventana
df_analisis = df_ventas_region \
    .withColumn("rank", dense_rank().over(ventana_region)) \
    .withColumn("ventas_mes_anterior", lag("ventas").over(ventana_region)) \
    .withColumn("diferencia", col("ventas") - col("ventas_mes_anterior"))

df_analisis.show()