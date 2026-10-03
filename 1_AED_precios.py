# %% [markdown]
# # Análisis Exploratorio de Datos (AED) - Precios Transaccionales

# %%
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

# Inicializar Spark Session
spark = SparkSession.builder.appName("AED_Precios").getOrCreate()

# %%
# 1. Carga de los datos
ruta_precios = "s3://aypmd-sources-uandes-2026/entrega2/precios_transaccionales/"
df_precios_spark = spark.read.csv(ruta_precios, header=True, inferSchema=True)

# %%
# Convertimos a Pandas para facilitar la exploración y hacer los gráficos
df_precios = df_precios_spark.toPandas()
df_precios.head()

# %%
# 2. Búsqueda de Datos Faltantes (Nulos)
print("Valores Nulos por columna:")
nulos = df_precios.isnull().sum()
print(nulos)

# %%
# 3. Búsqueda de Duplicados
duplicados = df_precios.duplicated().sum()
print(f"Número de filas duplicadas exactas: {duplicados}")

# %%
# 4. Búsqueda de Outliers (Valores atípicos)
# Gráfico de caja (boxplot) para precio_clp
plt.figure(figsize=(10, 6))
# Usamos dropna() para que el gráfico no falle por los nulos
plt.boxplot(df_precios['precio_clp'].dropna())
plt.title('Boxplot de Precios CLP')
plt.ylabel('Precio (CLP)')
plt.show()

# Gráfico de caja (boxplot) para precio_uf
plt.figure(figsize=(10, 6))
plt.boxplot(df_precios['precio_uf'].dropna())
plt.title('Boxplot de Precios UF')
plt.ylabel('Precio (UF)')
plt.show()

# %%
# 5. Inconsistencias
# Revisar estadísticas descriptivas (mínimos, máximos, promedios)
print(df_precios.describe())

# Prueba de edicion exitosa
