# Entrega 2 - Grupo 02

Los archivos principales son los **tres `.ipynb`**. Cada uno contiene todo su código;
los `.py` son respaldos sincronizados. Los notebooks se entregan sin outputs inventados:
hay que ejecutarlos en EMR y guardar los resultados reales.

Validación local realizada: estructura Jupyter (`nbformat`), sintaxis de las 29
celdas de código, respaldos `.py` coincidentes y nueve esquemas contrastados con
el Anexo. No se ejecutaron las transformaciones: falta un entorno local Spark/Java.
La prueba funcional y las verificaciones S3/Glue/Athena quedan para EMR.

## Ejecución en AWS

1. Levantar/clonar `grupo02-cluster` con Spark 3.5.6 y JupyterHub, según el taller.
   Conectarse por SSH usando la clave del grupo, como indica el enunciado.
   Respetar la cuota de 12 horas de EMR.
2. Abrir/subir los tres `.ipynb` en JupyterHub. Usar un kernel Python 3 del entorno
   EMR con PySpark; el AED también necesita pandas y matplotlib. Ejecutar todas las
   celdas en orden. No usar un Python local sin conexión al clúster.
3. Ejecutar `1_AED_precios.ipynb`, revisar anomalías y formatos, y guardar sus outputs.
   Todos los gráficos/tablas que se usen en el informe deben estar en este notebook.
4. Ejecutar `2_Preprocesamiento_Catastro.ipynb` con `periodo = "2026_1"` en la
   **primera celda de código**. Procesa un solo período por ejecución.
5. Ejecutar `3_Preprocesamiento_Otras.ipynb`. Revisar los conteos de limpieza,
   imputación y precios pendientes.
6. Verificar las nueve carpetas en `s3://grupo02-aypmd-uandes-2026/entrega2_procesado/`:

   ```text
   construcciones/periodo=2026_1/
   no_agricola/periodo=2026_1/
   nexo_bc/periodo=2026_1/
   precios_transaccionales/
   precios_uf/
   coordenadas/
   codigo_comuna_region/
   codigo_destino/
   codigo_material/
   ```

7. Crear manualmente un crawler nuevo, por ejemplo `grupo02-crawler-entrega2`.
8. Apuntarlo al prefijo `s3://grupo02-aypmd-uandes-2026/entrega2_procesado/`.
   Configurarlo para reconocer cada subcarpeta como tabla separada, sin fusionar
   tablas con columnas similares. No usar el crawler de la Entrega 1.
9. Elegir como destino la base `grupo02-entrega2`, ya existente según el estado
   informado. Si no existe, crearla manualmente. No agregar un prefijo a los nombres
   de tabla si se utilizarán las consultas de abajo.
10. Ejecutar el crawler.
11. Comprobar en Glue las nueve tablas, tipos del Anexo y particiones por `periodo`
    en las tres tablas catastrales. Si un nexo vacío no permite al crawler descubrir
    su partición, revisar/agregar esa partición manualmente en Glue.
12. Consultarlas desde Athena. Usar la ubicación de resultados autorizada para el
    grupo y contrastar los conteos con los notebooks.
13. Descargar y guardar localmente los **tres notebooks ejecutados** antes de
    terminar el clúster. Guardar también evidencias de S3, Glue y Athena para el informe.

## Consultas Athena

```sql
SELECT * FROM "grupo02-entrega2"."no_agricola" LIMIT 10;
SELECT COUNT(*) FROM "grupo02-entrega2"."no_agricola";
SELECT periodo, COUNT(*) FROM "grupo02-entrega2"."no_agricola" GROUP BY periodo;

SELECT * FROM "grupo02-entrega2"."construcciones" LIMIT 10;
SELECT COUNT(*) FROM "grupo02-entrega2"."construcciones";
SELECT periodo, COUNT(*) FROM "grupo02-entrega2"."construcciones" GROUP BY periodo;

SELECT * FROM "grupo02-entrega2"."nexo_bc" LIMIT 10;
SELECT COUNT(*) FROM "grupo02-entrega2"."nexo_bc";
SELECT periodo, COUNT(*) FROM "grupo02-entrega2"."nexo_bc" GROUP BY periodo;
SELECT COUNT(*) AS nexos_invalidos
FROM "grupo02-entrega2"."nexo_bc"
WHERE cod_com_bc IS NULL OR cod_com_bc = 0 OR cod_pr IS NULL OR cod_pr >= 70000;

SELECT * FROM "grupo02-entrega2"."precios_transaccionales" LIMIT 10;
SELECT COUNT(*) FROM "grupo02-entrega2"."precios_transaccionales";
SELECT COUNT(*) AS ambos_precios_pendientes
FROM "grupo02-entrega2"."precios_transaccionales"
WHERE precio_clp IS NULL AND precio_uf IS NULL;
```

`precios_transaccionales` no tiene columna `periodo` en el Anexo.

## Decisiones y verificaciones pendientes

- Se mantienen las rutas originales y el separador CSV por defecto (coma).
  Construcciones y no_agricola se leen con `header=False`; precios transaccionales
  y precios_uf con `header=True`, según la revisión de S3 informada por el grupo.
  Construcciones recibe los diez nombres del Anexo. **Pendiente antes de ejecutar:**
  confirmar el orden crudo completo de no_agricola para asignar sus nombres, incluidos
  bienes comunes. Por ahora su validación de columnas detiene el notebook 2.
- Se leen columnas como texto en lugar de inferir tipos para no perder códigos.
  Las fechas transaccionales usan `yyyyMMdd` (por ejemplo, `20030721`) en el AED y
  notebook 3; precios_uf conserva `yyyy-MM-dd` y encabezado `date,uf`.
  Falta confirmar el orden de las seis columnas transaccionales para renombrarlas.
  No se cambia el separador decimal.
- El catastro suma superficies y obtiene el año mínimo después de castear.
  Conserva todas las construcciones y los nulos sin rellenarlos arbitrariamente.
  Los casts estrictos fallan ante datos inválidos o desbordamiento de Int.
- El notebook 3 elimina duplicados exactos originales y filas sin ROL/fecha
  utilizables, mostrando conteos. Precios inválidos o no positivos pasan a nulo
  antes de imputar; los precios positivos no se eliminan por IQR. Se conservan y
  contabilizan los casos con ambos precios faltantes o sin UF diaria.
- La referencia UF del join elimina parejas idénticas fecha/valor y rechaza valores
  contradictorios por fecha. La salida `precios_uf` conserva sus filas válidas
  originales; las otras tablas auxiliares solo se castean al esquema requerido.
- La fuente `cod_material/` se mantiene; la salida se llama `codigo_material/`
  según el Anexo. No se escriben otras rutas de datos.
- El catastro usa sobrescritura dinámica. Si el nexo queda vacío, reemplaza solo
  `nexo_bc/periodo=<período>/` con Parquet vacío para evitar residuos antiguos.
  Cuando haya dos períodos, guardar sus conteos, reejecutar uno y comprobar que el
  otro permanece intacto. Verificar en EMR la compatibilidad del committer S3.
  Referencias: [Spark 3.5.6](https://spark.apache.org/docs/3.5.6/configuration.html#runtime-sql-configuration)
  y [protocolo de escritura de EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-commit-protocol-reqs.html).
- Las escrituras de las nueve tablas son independientes: si una falla, corregir
  la causa y reejecutar el notebook completo. El crawler se ejecuta al finalizar
  ambas etapas, después de verificar los Parquet.

## Cruce con el enunciado

| Requisito del PDF | Preparado en el repositorio / pendiente |
| --- | --- |
| 2.1: AED solo de precios, faltantes, duplicados, inconsistencias, outliers | Notebook 1; resultados reales pendientes de EMR |
| 2.2: dos notebooks de preprocesamiento exclusivamente PySpark | Notebooks 2 y 3 |
| 2.2.1: un período, tres tablas catastrales, suma/mínimo por ROL y filtro BC | Notebook 2 |
| 2.2.2: limpieza e imputación con UF; otras tablas según Anexo | Notebook 3 |
| Parquet, particiones y sobrescritura del catastro | Escrituras activas y lectura de verificación; ejecución S3 pendiente |
| Nuevo crawler/base Glue y consultas Athena | Pasos manuales anteriores; no se modificaron recursos AWS |
| 2.3: tres ipynb, gráficos en AED, respaldo local | Tres notebooks autónomos; descargar sus versiones ejecutadas |
| 3: cuota de 12 horas | A controlar durante la ejecución real |
| 4: informe, presupuesto y entrega en Canvas | Pendientes por instrucción del usuario; no se redactaron |
| 5: nueve esquemas del Anexo | Nombres y tipos explícitos con validaciones en notebooks |

El informe posterior deberá incluir portada, introducción, herramientas, AED,
preprocesamiento, presupuesto y conclusión, conforme al Centro de Escritura.
El presupuesto pendiente debe considerar EMR on-demand, EC2 m5.xlarge, S3 y Glue:
12 horas de clúster y 10 ejecuciones de crawler, excluyendo el bucket de fuentes,
con enlace a AWS Pricing Calculator. El PDF fija la entrega para el domingo
4 de octubre a las 23:59 en Canvas, con informe PDF y códigos `.ipynb`.
