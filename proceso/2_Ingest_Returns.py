# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Ingesta Bronze — Devoluciones (JSON)
# MAGIC
# MAGIC **Raw → Bronze** · Fuente: `abfss://raw@<storage>.dfs.core.windows.net/superstore/returns/` (JSON multilínea)
# MAGIC
# MAGIC Técnicas: esquema explícito para JSON, `multiLine`, normalización de valores (`lower`/`trim`),
# MAGIC data quality hacia `control.rechazados`, `insertInto` y auditoría.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

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
tabla_destino = f"{catalogo}.bronze.returns"
ruta_origen = f"abfss://{container}@{storageName}.dfs.core.windows.net/superstore/returns/"
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
# MAGIC ### 1. Lectura JSON con esquema explícito

# COMMAND ----------

schema_returns = StructType([
    StructField("Returned", StringType(), True),
    StructField("Order ID", StringType(), True),
])

df_raw = (spark.read
          .schema(schema_returns)
          .option("multiLine", True)
          .json(ruta_origen)
          .withColumn("source_file", F.col("_metadata.file_path")))

display(df_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Estandarización y reglas de calidad

# COMMAND ----------

df_std = (df_raw
          .select(F.trim(F.col("`Order ID`")).alias("order_id"),
                  F.lower(F.trim(F.col("Returned"))).alias("returned"),
                  F.col("source_file"))
          .withColumn("ingestion_ts", F.current_timestamp()))

reglas = {
    "order_id_nulo": F.col("order_id").isNull() | (F.col("order_id") == ""),
    "flag_devolucion_invalido": ~F.col("returned").isin("yes", "no", "si", "sí", "true", "false"),
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
    F.lit("returns").alias("fuente"),
    F.col("motivo_rechazo").alias("motivo"),
    F.to_json(F.struct("order_id", "returned")).alias("registro"),
    F.col("source_file"),
    F.col("ingestion_ts"),
)
insertar(df_rech_out, f"{catalogo}.control.rechazados", "append")

leidas = df_std.count()
escritas = spark.table(tabla_destino).count()
auditar("2_Ingest_Returns", tabla_destino, leidas, escritas, leidas - escritas)

print(f"Leídas: {leidas} | Escritas: {escritas} | Rechazadas: {leidas - escritas}")
dbutils.notebook.exit(f"OK returns: {escritas} filas")
