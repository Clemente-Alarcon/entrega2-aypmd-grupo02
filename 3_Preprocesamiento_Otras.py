# %% [markdown]
# # Preprocesamiento de otras tablas
# Procesamiento exclusivamente con PySpark. Ejecutar después del AED y revisar sus resultados.
# Las seis salidas respetan los nombres y tipos del Anexo.

# %% [markdown]
# ## Configuración
# Se conservan rutas fuente y lectura con encabezados. Se lee texto para controlar los casts.
# Según la revisión de S3 del grupo, precios transaccionales tiene encabezado y fechas
# `yyyyMMdd`; precios_uf tiene encabezado `date,uf` y fechas `yyyy-MM-dd`.

# %%
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Preprocesamiento_Otras").getOrCreate()
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
ruta_base = "s3://aypmd-sources-uandes-2026/entrega2"
ruta_destino = "s3://grupo02-aypmd-uandes-2026/entrega2_procesado"
formato_fecha_escritura = "yyyyMMdd"
formato_fecha_uf = "yyyy-MM-dd"
claves = ["cod_com", "cod_mz", "cod_pr"]

# %%
esquemas = {'precios_transaccionales': {'cod_com': 'int',
                             'cod_mz': 'int',
                             'cod_pr': 'int',
                             'fecha_escritura': 'date',
                             'precio_clp': 'float',
                             'precio_uf': 'float'},
 'precios_uf': {'date': 'date', 'uf': 'float'},
 'coordenadas': {'cod_com': 'int',
                 'cod_mz': 'int',
                 'cod_pr': 'int',
                 'latitud': 'float',
                 'longitud': 'float'},
 'codigo_comuna_region': {'cod_com': 'int',
                          'cod_region': 'int',
                          'comuna': 'string',
                          'region': 'string'},
 'codigo_destino': {'cod_destino': 'string',
                    'descripcion_destino': 'string',
                    'tipo': 'string'},
 'codigo_material': {'cod_material': 'string',
                     'descripcion_material': 'string'}}

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


def convertir(df, esquema):
    # ANSI evita convertir datos inválidos o enteros fuera de rango en nulos silenciosos.
    return df.select(*[F.col(c).cast(tipo).alias(c) for c, tipo in esquema.items()])

# %% [markdown]
# ## Lectura de las seis fuentes

# %%
rutas = {nombre: f"{ruta_base}/{nombre}/" for nombre in esquemas}
rutas["codigo_material"] = f"{ruta_base}/cod_material/"
fuentes = {}
for nombre, ruta in rutas.items():
    fuentes[nombre] = spark.read.csv(ruta, header=True, inferSchema=False)
    exigir_columnas(fuentes[nombre], esquemas[nombre], nombre)
    print(f"Fuente {nombre}: {ruta}")
    fuentes[nombre].show(5, truncate=False)

# %% [markdown]
# ## UF diaria sin multiplicar transacciones
# La tabla UF se convierte al esquema del Anexo. Para el cruce usamos una referencia con
# una sola pareja fecha/valor: los duplicados idénticos no multiplican transacciones.
# Si hay fechas inválidas, UF no positivas, valores no numéricos o dos valores diferentes
# para la misma fecha, detenemos el proceso para revisar el origen; no elegimos uno al azar.
# La salida `precios_uf` conserva las filas originales válidas, como pide el PDF para otras tablas.

# %%
df_uf_tipado = fuentes["precios_uf"].select(
    fecha("date", formato_fecha_uf).alias("date"), numero("uf").alias("uf")
).cache()
uf_invalida = df_uf_tipado.filter(F.col("date").isNull() | F.col("uf").isNull() | (F.col("uf") <= 0))
if uf_invalida.limit(1).count():
    uf_invalida.show(5, truncate=False)
    raise ValueError("precios_uf: fechas o valores inválidos. Revise formato y fuente antes de imputar.")
uf_referencia = df_uf_tipado.dropDuplicates(["date", "uf"]).cache()
conflictos = uf_referencia.groupBy("date").count().filter(F.col("count") > 1)
if conflictos.limit(1).count():
    conflictos.show(5, truncate=False)
    raise ValueError("precios_uf tiene valores distintos para una misma fecha; no se puede elegir uno sin evidencia.")
df_p_uf_final = convertir(df_uf_tipado, esquemas["precios_uf"])
print("UF lista: la referencia del join tiene como máximo un valor por fecha.")

# %% [markdown]
# ## Limpieza de precios transaccionales
# Eliminamos duplicados exactos originales. Normalizamos ROL y fechas y contamos las filas
# sin claves o fecha utilizables antes de excluirlas. Los precios no positivos, no numéricos,
# NaN o infinitos pasan a nulo y se intenta imputarlos con el otro precio y la UF del día.
# Conservamos los casos con ambos precios ausentes y los que no tienen conversión diaria,
# con nulos y conteos explícitos. No descartamos precios positivos por superar el IQR.

# %%
df_original = fuentes["precios_transaccionales"].cache()
total_original = df_original.count()
df_unicos = df_original.dropDuplicates().cache()
total_unicos = df_unicos.count()
print(f"Filas originales: {total_original}; duplicados exactos eliminados: {total_original - total_unicos}")
df_tipado = df_unicos.select(
    *[F.expr(f"try_cast(trim(`{c}`) as int)").alias(c) for c in claves],
    fecha("fecha_escritura", formato_fecha_escritura).alias("fecha_escritura"),
    numero("precio_clp").alias("precio_clp"),
    numero("precio_uf").alias("precio_uf")
).cache()
rol_invalido = F.col("cod_com").isNull() | F.col("cod_mz").isNull() | F.col("cod_pr").isNull()
fecha_invalida = F.col("fecha_escritura").isNull()
df_tipado.agg(
    contar(rol_invalido, "rol_faltante_o_invalido"),
    contar(fecha_invalida, "fecha_faltante_o_invalida"),
    contar(rol_invalido | fecha_invalida, "filas_eliminadas_por_rol_o_fecha"),
    contar(F.col("precio_clp") <= 0, "clp_no_positivo"),
    contar(F.col("precio_uf") <= 0, "uf_no_positivo"),
    contar(F.col("precio_clp").isNull() & F.col("precio_uf").isNull(), "ambos_sin_valor_numerico")
).show(truncate=False)
df_limpio = df_tipado.filter(~(rol_invalido | fecha_invalida))
for c in ["precio_clp", "precio_uf"]:
    df_limpio = df_limpio.withColumn(c, F.when(F.col(c) > 0, F.col(c)))

# %% [markdown]
# ## Imputación por fecha
# Ambas fechas son Date antes del left join. CLP = UF × valor diario; UF = CLP / valor diario.
# Las dos expresiones utilizan los precios originales limpios, calculados con Double antes
# del cast Float requerido por el Anexo. Los casos imposibles de imputar se conservan.

# %%
df_unido = df_limpio.join(uf_referencia, df_limpio.fecha_escritura == uf_referencia.date, "left").cache()
df_unido.agg(
    F.count("*").alias("transacciones_tras_limpieza"),
    contar(F.col("precio_clp").isNull() & F.col("precio_uf").isNull(), "ambos_ausentes_conservados"),
    contar(F.col("uf").isNull(), "sin_conversion_diaria"),
    contar(F.col("precio_clp").isNull() & F.col("precio_uf").isNotNull() & F.col("uf").isNotNull(), "clp_a_imputar"),
    contar(F.col("precio_uf").isNull() & F.col("precio_clp").isNotNull() & F.col("uf").isNotNull(), "uf_a_imputar")
).show(truncate=False)
df_imputado = df_unido.select(
    *claves, "fecha_escritura",
    F.coalesce(F.col("precio_clp"), F.col("precio_uf") * F.col("uf")).alias("precio_clp"),
    F.coalesce(F.col("precio_uf"), F.col("precio_clp") / F.col("uf")).alias("precio_uf")
)
df_precios_final = convertir(df_imputado, esquemas["precios_transaccionales"]).cache()
df_precios_final.agg(
    contar(F.col("precio_clp").isNull(), "clp_pendiente"),
    contar(F.col("precio_uf").isNull(), "uf_pendiente"),
    contar(F.col("precio_clp").isNull() & F.col("precio_uf").isNull(), "ambos_pendientes_conservados")
).show(truncate=False)

# %% [markdown]
# ## Coordenadas y códigos
# Solo seleccionamos y casteamos según el Anexo; no imputamos ni eliminamos filas.
# Los casts inválidos detienen el proceso con modo ANSI.
# La fuente de material continúa siendo `cod_material/`; la salida es `codigo_material/`.

# %%
salidas = {"precios_transaccionales": df_precios_final, "precios_uf": df_p_uf_final}
for nombre in ["coordenadas", "codigo_comuna_region", "codigo_destino", "codigo_material"]:
    salidas[nombre] = convertir(fuentes[nombre], esquemas[nombre])

# %% [markdown]
# ## Validaciones antes de escribir
# Se comprueban nombres, orden, tipos y conteos. También se detecta desbordamiento Float
# en los precios/UF/coordenadas. No se escribe si una salida está completamente vacía;
# revisar el origen o el formato de fecha en ese caso.

# %%
conteos = {}
for nombre, datos in salidas.items():
    datos.cache()
    validar_esquema(datos, esquemas[nombre], nombre)
    conteos[nombre] = datos.count()
    print(f"{nombre}: {conteos[nombre]} registros")
    if conteos[nombre] == 0:
        raise ValueError(f"{nombre}: salida vacía; revise la fuente y las conversiones.")
    flotantes = [c for c, tipo in esquemas[nombre].items() if tipo == "float"]
    if flotantes:
        invalidos = datos.agg(*[
            contar(F.isnan(c) | (F.abs(F.col(c)) == float("inf")), c) for c in flotantes
        ]).first().asDict()
        if any(invalidos.values()):
            raise ValueError(f"{nombre}: valores no finitos después del cast Float: {invalidos}")

# %% [markdown]
# ## Escritura de las seis salidas en Parquet

# %%
if ruta_destino != "s3://grupo02-aypmd-uandes-2026/entrega2_procesado":
    raise ValueError("La salida debe permanecer dentro de entrega2_procesado/")
for nombre, datos in salidas.items():
    ruta = f"{ruta_destino}/{nombre}/"
    datos.write.mode("overwrite").parquet(ruta)
    print(f"Escrito: {ruta}")

# %% [markdown]
# ## Validación final de los archivos guardados

# %%
for nombre, datos in salidas.items():
    ruta = f"{ruta_destino}/{nombre}/"
    leido = spark.read.parquet(ruta)
    validar_esquema(leido, esquemas[nombre], nombre + " en S3")
    total_leido = leido.count()
    if total_leido != conteos[nombre]:
        raise ValueError(f"{nombre}: conteo leído {total_leido}, esperado {conteos[nombre]}")
    print(f"Verificado: {ruta} ({total_leido} registros)")
    datos.unpersist()
for datos in [df_original, df_unicos, df_tipado, df_unido, df_uf_tipado, uf_referencia]:
    datos.unpersist()
print("Guarde este notebook con sus outputs antes de terminar el clúster.")
