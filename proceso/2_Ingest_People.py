# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Ingesta Bronze — Gerentes Regionales (Parquet)
# MAGIC
# MAGIC **Raw → Bronze** · Fuente: `abfss://raw@<storage>.dfs.core.windows.net/superstore/people/` (Parquet)
# MAGIC
# MAGIC Técnicas: lectura Parquet (esquema embebido), validación de esquema esperado, `dropDuplicates`,
# MAGIC data quality, `insertInto` y auditoría.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Ambiente")
dbutils.widgets.text("storageName", "adlssuperstorervc1", "Storage Account")
dbutils.widgets.text("container", "raw", "Contenedor origen")
dbutils.widgets.text("run_id", "", "Run ID (lo envía el workflow)")

# COMMAND ----------

env = dbutils.widgets.get("env")
storageName = dbutils.widgets.get("storageName")
container = dbutils.widgets.get("container")
run_id = dbutils.widgets.get("run_id") or f"manual-{uuid.uuid4().hex[:8]}"

catalogo = f"superstore_{env}"
tabla_destino = f"{catalogo}.bronze.people"
ruta_origen = f"abfss://{container}@{storageName}.dfs.core.windows.net/superstore/people/"
inicio = datetime.now()

# COMMAND ----------

def insertar(df, tabla, modo="overwrite"):
    """insertInto es posicional: alineamos las columnas al orden de la tabla destino."""
    columnas_destino = spark.table(tabla).columns
    df.select(*columnas_destino).write.mode(modo).insertInto(tabla)


def auditar(proceso, tabla, leidas, escritas, rechazadas, estado="OK"):
    tabla_audit = f"{catalogo}.control.auditoria_etl"
    fila = [(run_id, env, proceso, tabla, int(leidas), int(escritas), int(rechazadas),
             inicio, datetime.now(), estado)]
    spark.createDataFrame(fila, spark.table(tabla_audit).schema) \
         .write.mode("append").insertInto(tabla_audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Lectura Parquet y validación del esquema esperado

# COMMAND ----------

df_raw = (spark.read.parquet(ruta_origen)
          .withColumn("source_file", F.col("_metadata.file_path")))

columnas_esperadas = {"Regional Manager", "Region"}
faltantes = columnas_esperadas - set(df_raw.columns)
if faltantes:
    raise ValueError(f"El archivo de gerentes no trae las columnas esperadas: {faltantes}")

df_raw.printSchema()
display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Estandarización, deduplicación y calidad

# COMMAND ----------

df_std = (df_raw
          .select(F.trim(F.col("`Regional Manager`")).alias("regional_manager"),
                  F.initcap(F.trim(F.col("Region"))).alias("region"),
                  F.col("source_file"))
          .dropDuplicates(["region"])               # una fila por región
          .withColumn("ingestion_ts", F.current_timestamp()))

reglas = {
    "region_nula": F.col("region").isNull() | (F.col("region") == ""),
    "gerente_nulo": F.col("regional_manager").isNull() | (F.col("regional_manager") == ""),
}
df_eval = df_std.withColumn(
    "motivo_rechazo",
    F.concat_ws(",", *[F.when(condicion, F.lit(nombre)) for nombre, condicion in reglas.items()])
)
df_validos = df_eval.filter(F.col("motivo_rechazo") == "")
df_rechazados = df_eval.filter(F.col("motivo_rechazo") != "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Escritura y auditoría

# COMMAND ----------

insertar(df_validos.drop("motivo_rechazo"), tabla_destino, "overwrite")

df_rech_out = df_rechazados.select(
    F.lit(run_id).alias("run_id"),
    F.lit("people").alias("fuente"),
    F.col("motivo_rechazo").alias("motivo"),
    F.to_json(F.struct("regional_manager", "region")).alias("registro"),
    F.col("source_file"),
    F.col("ingestion_ts"),
)
insertar(df_rech_out, f"{catalogo}.control.rechazados", "append")

leidas = df_raw.count()
escritas = spark.table(tabla_destino).count()
auditar("2_Ingest_People", tabla_destino, leidas, escritas, leidas - escritas)

print(f"Leídas: {leidas} | Escritas: {escritas}")

# COMMAND ----------

dbutils.notebook.exit(f"OK people: {escritas} filas")
