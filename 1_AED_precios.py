# %% [markdown]
# # AED de precios transaccionales
# Entrega 2 - Grupo 02. Analizamos únicamente `precios_transaccionales`.
# Las tablas se calculan con PySpark; los gráficos usan una muestra limitada.
# Ejecutar en EMR y guardar el notebook con sus resultados reales.

# %% [markdown]
# ## Configuración
# Se conservan la ruta y `header=True` del repositorio. Se leen los campos como texto
# para detectar valores inválidos y no perder códigos con ceros iniciales.
# El formato de fecha ISO es el supuesto de la conversión original; si S3 usa otro,
# ajustar `formato_fecha` aquí y en el notebook 3 después de revisar los ejemplos.

# %%
from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("AED_Precios").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
ruta_precios = "s3://aypmd-sources-uandes-2026/entrega2/precios_transaccionales/"
formato_fecha = "yyyy-MM-dd"
max_muestra = 10000
semilla = 42
claves = ["cod_com", "cod_mz", "cod_pr"]
columnas = claves + ["fecha_escritura", "precio_clp", "precio_uf"]

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


def vacio(nombre):
    return F.col(nombre).isNull() | (F.trim(F.col(nombre).cast("string")) == "")


def numero(nombre):
    valor = F.expr(f"try_cast(trim(`{nombre}`) as double)")
    return F.when(~F.isnan(valor) & (F.abs(valor) != float("inf")), valor)


def fecha(nombre, formato):
    return F.try_to_timestamp(F.trim(F.col(nombre)), F.lit(formato)).cast("date")


def contar(condicion, nombre):
    return F.count(F.when(condicion, 1)).alias(nombre)

# %% [markdown]
# ## Lectura y revisión del origen

# %%
df_original = spark.read.csv(ruta_precios, header=True, inferSchema=False).cache()
exigir_columnas(df_original, columnas, "precios_transaccionales")
df_original.printSchema()
df_original.show(5, truncate=False)
total = df_original.count()
print(f"Registros originales: {total}")
if total == 0:
    raise ValueError("La fuente precios_transaccionales está vacía; revise la ruta.")

# %% [markdown]
# ## Faltantes y duplicados exactos
# Se muestran por separado los nulos y las cadenas vacías. Los duplicados se comparan
# usando todas las columnas originales, antes de normalizar los valores.

# %%
df_original.agg(*[
    contar(F.col(c).isNull(), c) for c in df_original.columns
]).show(truncate=False)
print("Faltantes por columna (nulos o cadenas vacías):")
df_original.agg(*[contar(vacio(c), c) for c in df_original.columns]).show(truncate=False)
duplicados = total - df_original.dropDuplicates().count()
print(f"Duplicados exactos adicionales: {duplicados}")

# %% [markdown]
# ## Inconsistencias
# Conservamos los valores originales y añadimos conversiones para distinguir faltantes
# de datos inválidos. Los precios no numéricos, NaN e infinitos se consideran inválidos.
# Un ROL incompleto, una fecha inválida y un precio no positivo se reportan explícitamente.

# %%
df = df_original.withColumn("fecha_convertida", fecha("fecha_escritura", formato_fecha))
for c in claves:
    df = df.withColumn(c + "_convertido", F.expr(f"try_cast(trim(`{c}`) as int)"))
for c in ["precio_clp", "precio_uf"]:
    df = df.withColumn(c + "_numero", numero(c))
df = df.cache()

controles = [
    contar(vacio("cod_com") | vacio("cod_mz") | vacio("cod_pr"), "rol_faltante"),
    contar(vacio("fecha_escritura"), "fecha_faltante"),
    contar(~vacio("fecha_escritura") & F.col("fecha_convertida").isNull(), "fecha_invalida"),
    contar(vacio("precio_clp") & vacio("precio_uf"), "ambos_precios_faltantes"),
    contar(F.col("precio_clp_numero").isNull() & F.col("precio_uf_numero").isNull(),
           "ambos_precios_sin_valor_numerico"),
]
for c in claves:
    controles += [contar(vacio(c), c + "_faltante"),
                  contar(~vacio(c) & F.col(c + "_convertido").isNull(), c + "_invalido")]
for c in ["precio_clp", "precio_uf"]:
    controles += [contar(vacio(c), c + "_faltante"),
                  contar(~vacio(c) & F.col(c + "_numero").isNull(), c + "_invalido"),
                  contar(F.col(c + "_numero") <= 0, c + "_no_positivo")]
resumen = df.agg(*controles).first().asDict()
spark.createDataFrame(list(resumen.items()), "control string, registros long").show(40, truncate=False)
print("Ejemplos de fechas no convertibles (para revisar el formato):")
df.filter(~vacio("fecha_escritura") & F.col("fecha_convertida").isNull()).select(
    "fecha_escritura"
).show(5, truncate=False)

# %% [markdown]
# ## Estadísticos e IQR
# Los descriptivos incluyen todos los precios numéricos finitos, incluso los no positivos.
# Para el IQR usamos los precios positivos, separando los valores inválidos de los atípicos.
# Límites: Q1 - 1,5 × IQR y Q3 + 1,5 × IQR. Cuartiles aproximados (error relativo 0,001)
# sobre la base completa, con conteos exactos respecto de esos límites. No eliminamos outliers.

# %%
df.select(F.col("precio_clp_numero").alias("precio_clp"),
          F.col("precio_uf_numero").alias("precio_uf")).describe().show(truncate=False)

filas_iqr = []
for precio in ["precio_clp", "precio_uf"]:
    c = precio + "_numero"
    positivos = df.filter(F.col(c) > 0)
    cuartiles = positivos.approxQuantile(c, [0.25, 0.75], 0.001)
    if not cuartiles:
        print(f"{precio}: no hay valores positivos para calcular el IQR.")
        continue
    q1, q3 = cuartiles
    iqr = q3 - q1
    inferior, superior = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    conteos = positivos.agg(
        F.count("*").alias("validos"),
        contar(F.col(c) < inferior, "bajo"),
        contar(F.col(c) > superior, "sobre")
    ).first()
    filas_iqr.append((precio, q1, q3, iqr, inferior, superior,
                      conteos.validos, conteos.bajo, conteos.sobre))
spark.createDataFrame(filas_iqr, "precio string, q1 double, q3 double, iqr double, "
                      "limite_inferior double, limite_superior double, validos long, "
                      "bajo_limite long, sobre_limite long").show(truncate=False)

# %% [markdown]
# ## Boxplots de una muestra
# Muestreo aleatorio reproducible y límite de 10.000 filas antes de `toPandas()`.
# Los gráficos muestran precios positivos y pueden diferir del IQR de la base completa.
# La semilla fija hace repetible el muestreo mientras no cambien los archivos y particiones.

# %%
fraccion = min(1.0, max_muestra / total)
muestra = (df.select("precio_clp_numero", "precio_uf_numero")
           .sample(withReplacement=False, fraction=fraccion, seed=semilla)
           .limit(max_muestra).toPandas())
print(f"Filas de la muestra: {len(muestra)} (máximo {max_muestra})")
fig, ejes = plt.subplots(1, 2, figsize=(12, 5))
for eje, precio, unidad in zip(ejes, ["precio_clp", "precio_uf"], ["CLP", "UF"]):
    valores = muestra[precio + "_numero"].dropna()
    valores = valores[valores > 0]
    if len(valores):
        eje.boxplot(valores, whis=1.5)
    else:
        eje.text(0.5, 0.5, "Sin precios positivos en la muestra", ha="center", transform=eje.transAxes)
    eje.set_title(f"{precio}: muestra de {len(valores)} valores")
    eje.set_ylabel(unidad)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Validación final
# Este notebook no escribe ni limpia datos en S3. Guardar las tablas y gráficos con el notebook.
# El notebook 3 contabiliza duplicados, claves/fechas inválidas y precios sin valor útil;
# imputa únicamente cuando hay un precio positivo y una conversión UF disponible.
# Después de ejecutar este AED, revisar si sus resultados requieren ajustes adicionales.

# %%
print("AED terminado. Guarde el notebook ejecutado antes de terminar el clúster.")
df.unpersist()
df_original.unpersist()
