# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Ingesta Bronze — Órdenes (CSV)
# MAGIC
# MAGIC **Raw → Bronze** · Fuente: `abfss://raw@<storage>.dfs.core.windows.net/superstore/orders/` (CSV, ADLS Gen2 vía **Managed Identity**)
# MAGIC
# MAGIC | Paso | Técnica PySpark |
# MAGIC |---|---|
# MAGIC | Lectura con esquema explícito | `StructType` / `StructField`, `spark.read.csv` con opciones |
# MAGIC | Trazabilidad | columna oculta `_metadata.file_path`, `current_timestamp()` |
# MAGIC | Estandarización | renombrado a *snake_case*, `trim`, parseo multi-formato de fechas |
# MAGIC | Data quality | reglas declarativas → válidos a `bronze.orders`, inválidos a `control.rechazados` |
# MAGIC | Escritura | `insertInto` (la tabla ya existe, creada en Preparación de Ambiente) |
# MAGIC | Auditoría | fila en `control.auditoria_etl` |

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
tabla_destino = f"{catalogo}.bronze.orders"
ruta_origen = f"abfss://{container}@{storageName}.dfs.core.windows.net/superstore/orders/"
inicio = datetime.now()

# Formatos de fecha aceptados (en orden de prioridad)
FORMATOS_FECHA = ["M/d/yyyy", "yyyy-MM-dd", "d-M-yyyy"]

print(f"run_id={run_id} | origen={ruta_origen} | destino={tabla_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funciones de apoyo

# COMMAND ----------

def insertar(df, tabla, modo="overwrite"):
    """insertInto es posicional: alineamos las columnas al orden de la tabla destino."""
    columnas_destino = spark.table(tabla).columns
    df.select(*columnas_destino).write.mode(modo).insertInto(tabla)


def auditar(proceso, tabla, leidas, escritas, rechazadas, estado="OK"):
    """Registra la ejecución en control.auditoria_etl."""
    tabla_audit = f"{catalogo}.control.auditoria_etl"
    fila = [(run_id, env, proceso, tabla, int(leidas), int(escritas), int(rechazadas),
             inicio, datetime.now(), estado)]
    spark.createDataFrame(fila, spark.table(tabla_audit).schema) \
         .write.mode("append").insertInto(tabla_audit)


def parsear_fecha(nombre_columna):
    """Prueba varios formatos y devuelve el primero válido (try_to_timestamp no falla con ANSI)."""
    intentos = [F.to_date(F.try_to_timestamp(F.col(nombre_columna), F.lit(fmt))) for fmt in FORMATOS_FECHA]
    return F.coalesce(*intentos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Lectura desde la capa Raw con esquema explícito

# COMMAND ----------

schema_orders = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),   # se parsea después (multi-formato)
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),  # string: conserva ceros a la izquierda
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
])

df_raw = (spark.read
          .option("header", True)
          .option("quote", '"')
          .option("escape", '"')
          .option("mode", "PERMISSIVE")      # campos mal formados -> null (los atrapa data quality)
          .schema(schema_orders)
          .csv(ruta_origen)
          .withColumn("source_file", F.col("_metadata.file_path")))

display(df_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Estandarización (snake_case, trim y fechas)

# COMMAND ----------

renombres = {
    "Row ID": "row_id", "Order ID": "order_id", "Order Date": "order_date_raw",
    "Ship Date": "ship_date_raw", "Ship Mode": "ship_mode", "Customer ID": "customer_id",
    "Customer Name": "customer_name", "Segment": "segment", "Country": "country",
    "City": "city", "State": "state", "Postal Code": "postal_code", "Region": "region",
    "Product ID": "product_id", "Category": "category", "Sub-Category": "sub_category",
    "Product Name": "product_name", "Sales": "sales", "Quantity": "quantity",
    "Discount": "discount", "Profit": "profit",
}

df_std = df_raw.select(
    *[F.col(f"`{origen}`").alias(destino) for origen, destino in renombres.items()],
    F.col("source_file"),
)

columnas_texto = [c for c, t in df_std.dtypes if t == "string"]
for c in columnas_texto:
    df_std = df_std.withColumn(c, F.trim(F.col(c)))

df_std = (df_std
          .withColumn("order_date", parsear_fecha("order_date_raw"))
          .withColumn("ship_date", parsear_fecha("ship_date_raw"))
          .withColumn("ingestion_ts", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reglas de calidad de datos
# MAGIC Cada regla que falla agrega su nombre a `motivo_rechazo`. Si no falla ninguna, el registro es válido.

# COMMAND ----------

reglas = {
    "row_id_nulo": F.col("row_id").isNull(),
    "order_id_nulo": F.col("order_id").isNull() | (F.col("order_id") == ""),
    "fecha_orden_invalida": F.col("order_date").isNull(),
    "fecha_envio_invalida": F.col("ship_date").isNull(),
    "envio_antes_de_orden": F.col("ship_date") < F.col("order_date"),
    "cliente_nulo": F.col("customer_id").isNull(),
    "producto_nulo": F.col("product_id").isNull(),
    "ventas_no_positivas": F.col("sales").isNull() | (F.col("sales") <= 0),
    "cantidad_no_positiva": F.col("quantity").isNull() | (F.col("quantity") <= 0),
    "descuento_fuera_rango": F.col("discount").isNull() | (F.col("discount") < 0) | (F.col("discount") > 1),
    "utilidad_nula": F.col("profit").isNull(),
}

df_eval = df_std.withColumn(
    "motivo_rechazo",
    F.concat_ws(",", *[F.when(condicion, F.lit(nombre)) for nombre, condicion in reglas.items()])
)

df_validos = df_eval.filter(F.col("motivo_rechazo") == "")
df_rechazados = df_eval.filter(F.col("motivo_rechazo") != "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Escritura en Bronze y en Control (rechazados)

# COMMAND ----------

insertar(df_validos.drop("order_date_raw", "ship_date_raw", "motivo_rechazo"), tabla_destino, "overwrite")

columnas_originales = [c for c in df_std.columns if c not in ("ingestion_ts",)]
df_rech_out = df_rechazados.select(
    F.lit(run_id).alias("run_id"),
    F.lit("orders").alias("fuente"),
    F.col("motivo_rechazo").alias("motivo"),
    F.to_json(F.struct(*columnas_originales)).alias("registro"),
    F.col("source_file"),
    F.col("ingestion_ts"),
)
insertar(df_rech_out, f"{catalogo}.control.rechazados", "append")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Auditoría y validación

# COMMAND ----------

leidas = df_std.count()
escritas = spark.table(tabla_destino).count()
rechazadas = leidas - escritas
auditar("2_Ingest_Orders", tabla_destino, leidas, escritas, rechazadas)

print(f"Leídas: {leidas} | Escritas en bronze: {escritas} | Rechazadas: {rechazadas}")
display(df_rechazados.groupBy("motivo_rechazo").count())

dbutils.notebook.exit(f"OK orders: {escritas} filas ({rechazadas} rechazadas)")
