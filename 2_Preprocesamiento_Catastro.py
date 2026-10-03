# %% [markdown]
# # Preprocesamiento - Base de Datos Catastral (Notebook 2)

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as _sum, min as _min

# Inicializar Spark Session
spark = SparkSession.builder.appName("Preprocesamiento_Catastro").getOrCreate()

# %%
# Variable de periodo
periodo = '2026_1'

# Definición de rutas (S3 origen)
ruta_base = "s3://aypmd-sources-uandes-2026/entrega2"
ruta_construcciones = f"{ruta_base}/construcciones/{periodo}.csv"
ruta_no_agricola = f"{ruta_base}/no_agricola/{periodo}.csv"

# Definición de ruta destino
ruta_destino = "s3://grupo02-aypmd-uandes-2026/entrega2_procesado" 

# %%
# Lectura de datos
df_construcciones = spark.read.csv(ruta_construcciones, header=True, inferSchema=True)
df_no_agricola = spark.read.csv(ruta_no_agricola, header=True, inferSchema=True)

# %%
# ==============================================================================
# 1. TABLA CONSTRUCCIONES
# ==============================================================================
# Agregar periodo y castear tipos
df_const_final = df_construcciones.withColumn("periodo", lit(periodo)) \
    .withColumn("cod_com", col("cod_com").cast("integer")) \
    .withColumn("cod_mz", col("cod_mz").cast("integer")) \
    .withColumn("cod_pr", col("cod_pr").cast("integer")) \
    .withColumn("numero_linea", col("numero_linea").cast("integer")) \
    .withColumn("cod_calidad", col("cod_calidad").cast("integer")) \
    .withColumn("cod_destino", col("cod_destino").cast("string")) \
    .withColumn("cod_material", col("cod_material").cast("string")) \
    .withColumn("superficie_construida", col("superficie_construida").cast("integer")) \
    .withColumn("cod_condicion_especial", col("cod_condicion_especial").cast("string")) \
    .withColumn("ano_construccion", col("ano_construccion").cast("integer"))

# Seleccionamos las columnas en el orden del anexo
columnas_construcciones = ["cod_com", "cod_mz", "cod_pr", "periodo", "numero_linea", "cod_calidad", "cod_destino", "cod_material", "superficie_construida", "cod_condicion_especial", "ano_construccion"]
df_const_final = df_const_final.select(columnas_construcciones)

# Guardar en S3 particionado por periodo
# df_const_final.write.partitionBy("periodo").mode("overwrite").parquet(f"{ruta_destino}/construcciones/")
print("Tabla construcciones preprocesada exitosamente.")

# %%
# ==============================================================================
# 2. TABLA NO_AGRICOLA
# ==============================================================================
# A. Eliminar columnas relacionadas con bienes comunes
columnas_bc = ["cod_com_bc", "cod_mz_bc", "cod_pr_bc", "dir_bc"]
df_na_limpio = df_no_agricola.drop(*columnas_bc)

# B. Calcular superficie_total_construcciones y ano_construccion agrupando la tabla construcciones por ROL
agrupado_const = df_construcciones.groupBy("cod_com", "cod_mz", "cod_pr").agg(
    _sum("superficie_construida").alias("superficie_total_construcciones"),
    _min("ano_construccion").alias("ano_construccion")
)

# C. Unir (Left Join) la tabla limpia con los cálculos usando el ROL
df_na_unido = df_na_limpio.join(agrupado_const, ["cod_com", "cod_mz", "cod_pr"], "left")

# D. Agregar columna periodo y castear tipos según anexo
df_na_final = df_na_unido.withColumn("periodo", lit(periodo)) \
    .withColumn("cod_com", col("cod_com").cast("integer")) \
    .withColumn("cod_mz", col("cod_mz").cast("integer")) \
    .withColumn("cod_pr", col("cod_pr").cast("integer")) \
    .withColumn("direccion", col("direccion").cast("string")) \
    .withColumn("avaluo_fiscal_clp", col("avaluo_fiscal_clp").cast("integer")) \
    .withColumn("contrib_semestrales_clp", col("contrib_semestrales_clp").cast("integer")) \
    .withColumn("cod_destino", col("cod_destino").cast("string")) \
    .withColumn("avaluo_exento_clp", col("avaluo_exento_clp").cast("integer")) \
    .withColumn("superficie_total_terreno", col("superficie_total_terreno").cast("integer")) \
    .withColumn("superficie_total_construcciones", col("superficie_total_construcciones").cast("integer")) \
    .withColumn("ano_construccion", col("ano_construccion").cast("integer"))

# Seleccionamos las columnas en el orden del anexo
columnas_no_agricola = ["cod_com", "cod_mz", "cod_pr", "periodo", "direccion", "avaluo_fiscal_clp", "contrib_semestrales_clp", "cod_destino", "avaluo_exento_clp", "superficie_total_terreno", "superficie_total_construcciones", "ano_construccion"]
df_na_final = df_na_final.select(columnas_no_agricola)

# Guardar en S3 particionado
# df_na_final.write.partitionBy("periodo").mode("overwrite").parquet(f"{ruta_destino}/no_agricola/")
print("Tabla no_agricola preprocesada exitosamente.")

# %%
# ==============================================================================
# 3. TABLA NEXO_BC
# ==============================================================================
# A. Extraer ROL y Bien Común de no_agricola original
df_nexo = df_no_agricola.select("cod_com", "cod_mz", "cod_pr", "cod_com_bc", "cod_mz_bc", "cod_pr_bc")

# B. Filtrar (cod_com_bc no nulo ni 0, y cod_pr < 70000)
df_nexo_filtrado = df_nexo.filter(
    (col("cod_com_bc").isNotNull()) & 
    (col("cod_com_bc") != 0) & 
    (col("cod_pr") < 70000)
)

# C. Agregar periodo y castear
df_nexo_final = df_nexo_filtrado.withColumn("periodo", lit(periodo)) \
    .withColumn("cod_com", col("cod_com").cast("integer")) \
    .withColumn("cod_mz", col("cod_mz").cast("integer")) \
    .withColumn("cod_pr", col("cod_pr").cast("integer")) \
    .withColumn("cod_com_bc", col("cod_com_bc").cast("integer")) \
    .withColumn("cod_mz_bc", col("cod_mz_bc").cast("integer")) \
    .withColumn("cod_pr_bc", col("cod_pr_bc").cast("integer"))

columnas_nexo = ["cod_com", "cod_mz", "cod_pr", "periodo", "cod_com_bc", "cod_mz_bc", "cod_pr_bc"]
df_nexo_final = df_nexo_final.select(columnas_nexo)

# Guardar en S3 particionado
# df_nexo_final.write.partitionBy("periodo").mode("overwrite").parquet(f"{ruta_destino}/nexo_bc/")
print("Tabla nexo_bc preprocesada exitosamente.")
