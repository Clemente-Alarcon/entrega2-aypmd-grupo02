# %% [markdown]
# # Preprocesamiento catastral
# Un único período por ejecución. Los resultados respetan el Anexo del enunciado.
# La primera celda de código es el punto de entrada del período.

# %%
periodo = "2026_1"

# %% [markdown]
# ## Configuración

# %%
import re
from pyspark.sql import SparkSession, functions as F

if not re.fullmatch(r"[0-9]{4}_[12]", periodo):
    raise ValueError("Use un período año_semestre, por ejemplo 2026_1.")
spark = SparkSession.builder.appName("Preprocesamiento_Catastro").getOrCreate()
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
ruta_base = "s3://aypmd-sources-uandes-2026/entrega2"
ruta_destino = "s3://grupo02-aypmd-uandes-2026/entrega2_procesado"
ruta_construcciones = f"{ruta_base}/construcciones/{periodo}.csv"
ruta_no_agricola = f"{ruta_base}/no_agricola/{periodo}.csv"
claves = ["cod_com", "cod_mz", "cod_pr"]

# %%
esquema_construcciones = {'cod_com': 'int',
 'cod_mz': 'int',
 'cod_pr': 'int',
 'periodo': 'string',
 'numero_linea': 'int',
 'cod_calidad': 'int',
 'cod_destino': 'string',
 'cod_material': 'string',
 'superficie_construida': 'int',
 'cod_condicion_especial': 'string',
 'ano_construccion': 'int'}

esquema_no_agricola = {'cod_com': 'int',
 'cod_mz': 'int',
 'cod_pr': 'int',
 'periodo': 'string',
 'direccion': 'string',
 'avaluo_fiscal_clp': 'int',
 'contrib_semestrales_clp': 'int',
 'cod_destino': 'string',
 'avaluo_exento_clp': 'int',
 'superficie_total_terreno': 'int',
 'superficie_total_construcciones': 'int',
 'ano_construccion': 'int'}

esquema_nexo = {'cod_com': 'int',
 'cod_mz': 'int',
 'cod_pr': 'int',
 'periodo': 'string',
 'cod_com_bc': 'int',
 'cod_mz_bc': 'int',
 'cod_pr_bc': 'int'}

# %%
def exigir_columnas(df, columnas, nombre):
    faltan = sorted(set(columnas) - set(df.columns))
    if faltan:
        raise ValueError(
            f"{nombre}: faltan columnas {faltan}. Columnas leídas: {df.columns}. "
            "Verifique encabezado y separador del CSV en S3; no se asume un orden crudo."
        )


def validar_esquema(df, esquema, nombre):
    esperado = list(esquema.items())
    if df.dtypes != esperado:
        raise ValueError(f"{nombre}: esquema {df.dtypes}; se esperaba {esperado}")
    print(f"{nombre}: columnas y tipos correctos: {df.columns}")
    df.printSchema()
    df.show(5, truncate=False)


def convertir(df, esquema):
    # ANSI evita convertir datos inválidos o enteros fuera de rango en nulos silenciosos.
    return df.select(*[F.col(c).cast(tipo).alias(c) for c, tipo in esquema.items()])

# %% [markdown]
# ## Lectura y columnas esenciales
# Se conservan rutas y `header=True`. El PDF describe el resultado, pero no confirma
# encabezados, separador ni todas las columnas crudas de no_agricola. Si las columnas
# no coinciden, se detiene la ejecución para revisar la fuente sin inventar su estructura.
# La lectura como texto conserva los códigos; los casts explícitos se hacen antes de agregar.
# Los casts inválidos y el desbordamiento de Int detienen la ejecución (modo ANSI).

# %%
df_construcciones = spark.read.csv(ruta_construcciones, header=True, inferSchema=False)
df_no_agricola = spark.read.csv(ruta_no_agricola, header=True, inferSchema=False)
columnas_const = [c for c in esquema_construcciones if c != "periodo"]
columnas_na = [c for c in esquema_no_agricola
               if c not in ["periodo", "superficie_total_construcciones", "ano_construccion"]]
columnas_bc = ["cod_com_bc", "cod_mz_bc", "cod_pr_bc"]
exigir_columnas(df_construcciones, columnas_const, "construcciones")
exigir_columnas(df_no_agricola, columnas_na + columnas_bc, "no_agricola")
df_construcciones.show(5, truncate=False)
df_no_agricola.select(columnas_na + columnas_bc).show(5, truncate=False)

# %% [markdown]
# ## Construcciones
# Conservamos todas las filas, incluidas varias construcciones para un mismo ROL.
# Solo seleccionamos las columnas del Anexo, convertimos sus tipos y agregamos el período.

# %%
df_const_final = convertir(df_construcciones.withColumn("periodo", F.lit(periodo)),
                          esquema_construcciones).cache()

# %% [markdown]
# ## No agrícola y nexo de bienes comunes
# Agregamos construcciones ya casteadas por las tres claves del ROL. El left join mantiene
# propiedades sin construcciones, con superficie y año nulos. La selección final de no_agricola
# excluye todos los campos de bienes comunes; el nexo usa solamente el bien común principal.

# %%
esquema_na_origen = {c: esquema_no_agricola[c] for c in columnas_na}
esquema_na_origen.update({c: "int" for c in columnas_bc})
df_na_tipado = convertir(df_no_agricola, esquema_na_origen).cache()
agrupado_const = df_const_final.groupBy(*claves).agg(
    F.sum("superficie_construida").alias("superficie_total_construcciones"),
    F.min("ano_construccion").alias("ano_construccion")
)
df_na_unido = (df_na_tipado.select(columnas_na)
               .join(agrupado_const, claves, "left")
               .withColumn("periodo", F.lit(periodo)))
df_na_final = convertir(df_na_unido, esquema_no_agricola).cache()

df_nexo_final = convertir(
    df_na_tipado.filter(F.col("cod_com_bc").isNotNull()
                       & (F.col("cod_com_bc") != 0)
                       & (F.col("cod_pr") < 70000))
    .withColumn("periodo", F.lit(periodo)), esquema_nexo
).cache()

# %% [markdown]
# ## Validaciones antes de escribir

# %%
tablas = {
    "construcciones": (df_const_final, esquema_construcciones),
    "no_agricola": (df_na_final, esquema_no_agricola),
    "nexo_bc": (df_nexo_final, esquema_nexo),
}
conteos = {}
for nombre, (datos, esquema) in tablas.items():
    validar_esquema(datos, esquema, nombre)
    resumen_periodos = datos.groupBy("periodo").count().cache()
    resumen_periodos.show()
    filas = resumen_periodos.collect()
    if any(f.periodo != periodo for f in filas):
        raise ValueError(f"{nombre}: contiene un período distinto de {periodo}")
    conteos[nombre] = sum(f['count'] for f in filas)
    resumen_periodos.unpersist()

if conteos["construcciones"] == 0 or conteos["no_agricola"] == 0:
    raise ValueError("Catastro fuente vacío. Revise las rutas antes de sobrescribir.")
if df_nexo_final.filter(F.col("cod_com_bc").isNull() | (F.col("cod_com_bc") == 0)
                        | F.col("cod_pr").isNull() | (F.col("cod_pr") >= 70000)).limit(1).count():
    raise ValueError("nexo_bc no cumple los filtros del enunciado")
print("Ejemplos de propiedades con construcciones incorporadas:")
df_na_final.filter(F.col("superficie_total_construcciones").isNotNull()).select(
    *claves, "periodo", "superficie_total_construcciones", "ano_construccion"
).show(5, truncate=False)

# %% [markdown]
# ## Escritura en S3
# Se sobrescriben dinámicamente las particiones presentes en esta ejecución.
# Los demás períodos se conservan. Si el nexo queda vacío, Spark no sobrescribe una
# partición vacía en modo dinámico: en ese caso escribimos un Parquet vacío directamente
# en `nexo_bc/periodo=.../` para reemplazar solamente ese período y evitar datos antiguos.

# %%
if ruta_destino != "s3://grupo02-aypmd-uandes-2026/entrega2_procesado":
    raise ValueError("La salida debe permanecer dentro de entrega2_procesado/")
for nombre, (datos, esquema) in tablas.items():
    ruta = f"{ruta_destino}/{nombre}/"
    if conteos[nombre] == 0:
        (datos.drop("periodo").write.mode("overwrite")
         .parquet(f"{ruta}periodo={periodo}/"))
        print(f"Partición vacía reemplazada: {ruta}periodo={periodo}/")
    else:
        (datos.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")
         .partitionBy("periodo").parquet(ruta))
        print(f"Escrito: {ruta}periodo={periodo}/ ({conteos[nombre]} registros)")

# %% [markdown]
# ## Validación final de los Parquet
# Leemos únicamente el período procesado y comparamos sus registros y tipos con la salida.
# Para probar la conservación de otros períodos, contrastar sus conteos en Athena antes y
# después de reejecutar este notebook cuando existan varios períodos.

# %%
for nombre, (datos, esquema) in tablas.items():
    ruta = f"{ruta_destino}/{nombre}/"
    leido = spark.read.option("basePath", ruta).parquet(f"{ruta}periodo={periodo}/")
    # Un archivo vacío puede no permitir inferir la columna de partición.
    if "periodo" not in leido.columns and conteos[nombre] == 0:
        leido = leido.withColumn("periodo", F.lit(periodo))
    leido = leido.select(list(esquema))
    validar_esquema(leido, esquema, nombre + " en S3")
    leido.groupBy("periodo").count().show()
    if leido.count() != conteos[nombre]:
        raise ValueError(f"{nombre}: el conteo escrito no coincide")
    print(f"Verificado: {ruta}periodo={periodo}/")
    datos.unpersist()
df_na_tipado.unpersist()
