# Ejemplo 5: Transformaciones y limpieza de datos
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, when, concat, lit, col

spark = SparkSession.builder \
    .appName("Operaciones Avanzadas") \
    .getOrCreate()

# Crear DataFrame de ejemplo
empleados_data = [
    (1, "Juan", None, 30000),
    (2, "María", "IT", 45000),
    (3, "Pedro", "HR", None),
    (4, "Ana", "IT", 55000),
    (5, "Luis", "IT", 35000)
]

df_empleados = spark.createDataFrame(empleados_data, ["id", "nombre", "departamento", "salario"])

# 1. Manejo de valores nulos
df_limpio = df_empleados \
    .fillna("Sin Departamento", subset=["departamento"]) \
    .fillna(0, subset=["salario"])

# 2. Transformaciones de columnas
df_transformado = df_limpio \
    .withColumn("nombre_mayusculas", upper(col("nombre"))) \
    .withColumn("salario_categoria", 
                when(col("salario") > 40000, "Alto")
                .when(col("salario") > 30000, "Medio")
                .otherwise("Bajo")) \
    .withColumn("descripcion", 
                concat(col("nombre"), lit(" - "), col("departamento")))

df_transformado.show()