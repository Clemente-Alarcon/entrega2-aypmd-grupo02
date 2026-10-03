# %% [markdown]
# # Preprocesamiento - Otras Tablas (Notebook 3)

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DateType

# Inicializar Spark Session
spark = SparkSession.builder.appName("Preprocesamiento_Otras").getOrCreate()

# Definición de rutas
ruta_base = "s3://aypmd-sources-uandes-2026/entrega2"
# Definición de ruta destino
ruta_destino = "s3://grupo02-aypmd-uandes-2026/entrega2_procesado"

# %%
# ==============================================================================
# 1. TABLA PRECIOS_TRANSACCIONALES
# ==============================================================================
df_precios_t = spark.read.csv(f"{ruta_base}/precios_transaccionales/", header=True, inferSchema=True)
df_precios_uf = spark.read.csv(f"{ruta_base}/precios_uf/", header=True, inferSchema=True)

# Limpieza de duplicados
df_precios_t = df_precios_t.dropDuplicates()

# Unir con precios_uf para obtener el valor de la UF en el día de la transacción
df_unido = df_precios_t.join(df_precios_uf, df_precios_t.fecha_escritura == df_precios_uf.date, "left")

# Imputar valores cruzados de precios usando when().otherwise()
df_precios_imputados = df_unido.withColumn(
    "precio_clp", 
    when(col("precio_clp").isNull(), col("precio_uf") * col("uf")).otherwise(col("precio_clp"))
).withColumn(
    "precio_uf", 
    when(col("precio_uf").isNull(), col("precio_clp") / col("uf")).otherwise(col("precio_uf"))
)

# Castear tipos según Anexo y seleccionar columnas originales
df_precios_final = df_precios_imputados.select(
    col("cod_com").cast("integer"),
    col("cod_mz").cast("integer"),
    col("cod_pr").cast("integer"),
    col("fecha_escritura").cast(DateType()),
    col("precio_clp").cast("float"),
    col("precio_uf").cast("float")
)

# Guardar en S3
# df_precios_final.write.mode("overwrite").parquet(f"{ruta_destino}/precios_transaccionales/")
print("Tabla precios_transaccionales preprocesada exitosamente.")

# %%
# ==============================================================================
# 2. TABLA PRECIOS_UF
# ==============================================================================
df_p_uf_final = df_precios_uf.select(
    col("date").cast(DateType()),
    col("uf").cast("float")
)
# df_p_uf_final.write.mode("overwrite").parquet(f"{ruta_destino}/precios_uf/")
print("Tabla precios_uf preprocesada exitosamente.")

# %%
# ==============================================================================
# 3. TABLA COORDENADAS
# ==============================================================================
df_coordenadas = spark.read.csv(f"{ruta_base}/coordenadas/", header=True, inferSchema=True)
df_coord_final = df_coordenadas.select(
    col("cod_com").cast("integer"),
    col("cod_mz").cast("integer"),
    col("cod_pr").cast("integer"),
    col("latitud").cast("float"),
    col("longitud").cast("float")
)
# df_coord_final.write.mode("overwrite").parquet(f"{ruta_destino}/coordenadas/")
print("Tabla coordenadas preprocesada exitosamente.")

# %%
# ==============================================================================
# 4. TABLA CODIGO_COMUNA_REGION
# ==============================================================================
df_com_reg = spark.read.csv(f"{ruta_base}/codigo_comuna_region/", header=True, inferSchema=True)
df_com_reg_final = df_com_reg.select(
    col("cod_com").cast("integer"),
    col("cod_region").cast("integer"),
    col("comuna").cast("string"),
    col("region").cast("string")
)
# df_com_reg_final.write.mode("overwrite").parquet(f"{ruta_destino}/codigo_comuna_region/")
print("Tabla codigo_comuna_region preprocesada exitosamente.")

# %%
# ==============================================================================
# 5. TABLA CODIGO_DESTINO
# ==============================================================================
df_destino = spark.read.csv(f"{ruta_base}/codigo_destino/", header=True, inferSchema=True)
df_destino_final = df_destino.select(
    col("cod_destino").cast("string"),
    col("descripcion_destino").cast("string"),
    col("tipo").cast("string")
)
# df_destino_final.write.mode("overwrite").parquet(f"{ruta_destino}/codigo_destino/")
print("Tabla codigo_destino preprocesada exitosamente.")

# %%
# ==============================================================================
# 6. TABLA CODIGO_MATERIAL
# ==============================================================================
df_material = spark.read.csv(f"{ruta_base}/cod_material/", header=True, inferSchema=True)
df_mat_final = df_material.select(
    col("cod_material").cast("string"),
    col("descripcion_material").cast("string")
)
# df_mat_final.write.mode("overwrite").parquet(f"{ruta_destino}/cod_material/")
print("Tabla cod_material preprocesada exitosamente.")
