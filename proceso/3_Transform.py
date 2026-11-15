# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Transform — Bronze → Silver
# MAGIC
# MAGIC Construye la capa **Silver** (datos limpios, conformados y enriquecidos) a partir de las 3 tablas Bronze.
# MAGIC
# MAGIC | Tabla Silver | Contenido |
# MAGIC |---|---|
# MAGIC | `silver.ventas_detalle` | Línea de venta enriquecida: fechas, márgenes, descuento, devolución, gerente regional (particionada por `anio`) |
# MAGIC | `silver.dim_cliente` | Un registro por cliente, con región principal y primera/última compra |
# MAGIC | `silver.dim_producto` | Nombre canónico por `id_producto` (el dataset trae IDs con más de un nombre) |
# MAGIC
# MAGIC **Técnicas PySpark aplicadas:** deduplicación con `Window` + `row_number`, manejo de nulos (`fillna`, `coalesce`),
# MAGIC columnas derivadas (`withColumn`, `when/otherwise`, funciones de fecha), **UDF**, **broadcast join**, **left join**,
# MAGIC agregaciones (`groupBy/agg`), `cache/persist`, `repartition` por la columna de partición e `insertInto`.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Ambiente")
dbutils.widgets.text("run_id", "", "Run ID (lo envía el workflow)")

# COMMAND ----------

env = dbutils.widgets.get("env")
run_id = dbutils.widgets.get("run_id") or f"manual-{uuid.uuid4().hex[:8]}"
catalogo = f"superstore_{env}"
inicio = datetime.now()

# COMMAND ----------

def insertar(df, tabla, modo="overwrite"):
    """insertInto es posicional: alineamos las columnas al orden de la tabla destino."""
    columnas_destino = spark.table(tabla).columns
    df.select(*columnas_destino).write.mode(modo).insertInto(tabla)


def auditar(proceso, tabla, leidas, escritas, rechazadas=0, estado="OK"):
    tabla_audit = f"{catalogo}.control.auditoria_etl"
    fila = [(run_id, env, proceso, tabla, int(leidas), int(escritas), int(rechazadas),
             inicio, datetime.now(), estado)]
    spark.createDataFrame(fila, spark.table(tabla_audit).schema) \
         .write.mode("append").insertInto(tabla_audit)


def cachear(df):
    """persist solo en el clúster clásico de prod.
    En dev se usa cómputo serverless, que no soporta cache/persist (puede invalidar la sesión): se omite."""
    if env != "prod":
        print("(info) dev/serverless: se omite persist")
        return df
    df = df.persist()
    print("DataFrame persistido en memoria (clúster clásico)")
    return df


def liberar(df):
    """unpersist solo donde se hizo persist (prod)."""
    if env == "prod":
        df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Lectura de Bronze

# COMMAND ----------

b_orders = spark.table(f"{catalogo}.bronze.orders")
b_returns = spark.table(f"{catalogo}.bronze.returns")
b_people = spark.table(f"{catalogo}.bronze.people")

filas_bronze = b_orders.count()
print(f"bronze.orders: {filas_bronze} | bronze.returns: {b_returns.count()} | bronze.people: {b_people.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deduplicación con función de ventana
# MAGIC Si un mismo `row_id` llega más de una vez (reprocesos), nos quedamos con la versión más reciente.

# COMMAND ----------

w_dedup = Window.partitionBy("row_id").orderBy(F.col("ingestion_ts").desc())

orders = (b_orders
          .withColumn("_rn", F.row_number().over(w_dedup))
          .filter(F.col("_rn") == 1)
          .drop("_rn"))

duplicados = filas_bronze - orders.count()
print(f"Duplicados eliminados: {duplicados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Manejo de nulos y normalización de texto

# COMMAND ----------

orders = (orders
          .fillna({"postal_code": "SIN_CP", "ship_mode": "No especificado", "segment": "No especificado"})
          .withColumn("region", F.initcap(F.col("region")))
          .withColumn("customer_name", F.initcap(F.col("customer_name"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. UDF — banda de descuento
# MAGIC Se implementa como **UDF de Python** (tema del curso). En producción a gran escala conviene la versión nativa
# MAGIC con `when/otherwise` (sin serialización Python ↔ JVM); dejamos ambas para comparar.

# COMMAND ----------

def banda_descuento(descuento):
    if descuento is None:
        return "Sin dato"
    if descuento == 0:
        return "Sin descuento"
    if descuento <= 0.20:
        return "Bajo (hasta 20%)"
    if descuento <= 0.40:
        return "Medio (21% a 40%)"
    return "Alto (más de 40%)"

banda_descuento_udf = F.udf(banda_descuento, StringType())

# Equivalente nativo (referencia, más eficiente):
banda_descuento_nativa = (F.when(F.col("discount").isNull(), "Sin dato")
                          .when(F.col("discount") == 0, "Sin descuento")
                          .when(F.col("discount") <= 0.20, "Bajo (hasta 20%)")
                          .when(F.col("discount") <= 0.40, "Medio (21% a 40%)")
                          .otherwise("Alto (más de 40%)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Columnas derivadas (fechas, márgenes, descuento y precio)

# COMMAND ----------

orders = (orders
          .withColumn("anio", F.year("order_date"))
          .withColumn("mes", F.month("order_date"))
          .withColumn("trimestre", F.quarter("order_date"))
          .withColumn("anio_mes", F.trunc("order_date", "month"))
          .withColumn("dias_envio", F.datediff("ship_date", "order_date"))
          .withColumn("margen_pct", F.when(F.col("sales") != 0, F.round(F.col("profit") / F.col("sales"), 4)))
          .withColumn("precio_unitario", F.round(F.col("sales") / F.col("quantity"), 4))
          .withColumn("ventas_brutas", F.when(F.col("discount") < 1,
                                               F.round(F.col("sales") / (1 - F.col("discount")), 2)))
          .withColumn("monto_descuento", F.round(F.col("ventas_brutas") - F.col("sales"), 2))
          .withColumn("es_perdida", F.col("profit") < 0)
          .withColumn("banda_descuento", banda_descuento_udf(F.col("discount"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Enriquecimiento con joins
# MAGIC - **Left join** con devoluciones (una orden devuelta marca todas sus líneas).
# MAGIC - **Broadcast join** con gerentes regionales (tabla de 4 filas: se replica a cada executor y evita el shuffle).

# COMMAND ----------

devoluciones = (b_returns
                .filter(F.col("returned").isin("yes", "si", "sí", "true"))
                .select("order_id")
                .dropDuplicates()
                .withColumn("devuelto", F.lit(True)))

gerentes = b_people.select(F.initcap("region").alias("region"), "regional_manager")

ventas = (orders
          .join(devoluciones, on="order_id", how="left")
          .withColumn("devuelto", F.coalesce(F.col("devuelto"), F.lit(False)))
          .join(F.broadcast(gerentes), on="region", how="left")
          .withColumn("regional_manager", F.coalesce(F.col("regional_manager"), F.lit("Sin asignar")))
          .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Modelo Silver: renombrado a nombres de negocio

# COMMAND ----------

ventas_detalle = ventas.select(
    F.col("row_id").alias("id_fila"),
    F.col("order_id").alias("id_orden"),
    F.col("order_date").alias("fecha_orden"),
    F.col("ship_date").alias("fecha_envio"),
    "anio", "mes", "trimestre", "anio_mes",
    F.col("ship_mode").alias("modo_envio"),
    "dias_envio",
    F.col("customer_id").alias("id_cliente"),
    F.col("customer_name").alias("nombre_cliente"),
    F.col("segment").alias("segmento"),
    F.col("country").alias("pais"),
    F.col("city").alias("ciudad"),
    F.col("state").alias("estado"),
    F.col("postal_code").alias("codigo_postal"),
    "region",
    F.col("regional_manager").alias("gerente_regional"),
    F.col("product_id").alias("id_producto"),
    F.col("category").alias("categoria"),
    F.col("sub_category").alias("subcategoria"),
    F.col("product_name").alias("nombre_producto"),
    F.col("sales").alias("ventas"),
    F.col("quantity").alias("cantidad"),
    F.col("discount").alias("descuento"),
    "banda_descuento",
    F.col("profit").alias("utilidad"),
    "margen_pct", "precio_unitario", "ventas_brutas", "monto_descuento",
    "es_perdida", "devuelto", "fecha_proceso",
)

ventas_detalle = cachear(ventas_detalle)   # se reutiliza para 3 tablas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Dimensiones
# MAGIC - **dim_producto:** el dataset Superstore tiene `id_producto` con más de un nombre → elegimos el nombre más frecuente
# MAGIC   (ventana ordenada por frecuencia) y dejamos `nombres_distintos` como indicador de calidad.
# MAGIC - **dim_cliente:** región principal = la región con más ventas del cliente.

# COMMAND ----------

w_prod = Window.partitionBy("id_producto").orderBy(F.col("frecuencia").desc(), F.col("nombre_producto"))

dim_producto = (ventas_detalle
                .groupBy("id_producto", "nombre_producto", "categoria", "subcategoria")
                .agg(F.count(F.lit(1)).alias("frecuencia"))
                .withColumn("nombres_distintos", F.count(F.lit(1)).over(Window.partitionBy("id_producto")).cast("int"))
                .withColumn("_rn", F.row_number().over(w_prod))
                .filter(F.col("_rn") == 1)
                .drop("_rn", "frecuencia")
                .withColumn("fecha_proceso", F.current_timestamp()))

w_region = Window.partitionBy("id_cliente").orderBy(F.col("ventas_region").desc())
region_principal = (ventas_detalle
                    .groupBy("id_cliente", "region")
                    .agg(F.sum("ventas").alias("ventas_region"))
                    .withColumn("_rn", F.row_number().over(w_region))
                    .filter(F.col("_rn") == 1)
                    .select("id_cliente", F.col("region").alias("region_principal")))

dim_cliente = (ventas_detalle
               .groupBy("id_cliente")
               .agg(F.max("nombre_cliente").alias("nombre_cliente"),
                    F.max("segmento").alias("segmento"),
                    F.min("fecha_orden").alias("primera_compra"),
                    F.max("fecha_orden").alias("ultima_compra"),
                    F.countDistinct("id_orden").alias("total_ordenes"))
               .join(region_principal, on="id_cliente", how="left")
               .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Escritura en Silver (`insertInto`) y auditoría

# COMMAND ----------

t_ventas = f"{catalogo}.silver.ventas_detalle"
t_cliente = f"{catalogo}.silver.dim_cliente"
t_producto = f"{catalogo}.silver.dim_producto"

insertar(ventas_detalle.repartition("anio"), t_ventas)   # agrupa por la columna de partición antes de escribir
insertar(dim_cliente, t_cliente)
insertar(dim_producto, t_producto)

for tabla in (t_ventas, t_cliente, t_producto):
    escritas = spark.table(tabla).count()
    auditar("3_Transform", tabla, filas_bronze, escritas, duplicados if tabla == t_ventas else 0)
    print(f"✔ {tabla}: {escritas} filas")

liberar(ventas_detalle)

# COMMAND ----------

display(spark.table(t_ventas).groupBy("anio").agg(F.round(F.sum("ventas"), 2).alias("ventas"),
                                                  F.round(F.sum("utilidad"), 2).alias("utilidad")).orderBy("anio"))

# COMMAND ----------

dbutils.notebook.exit("OK silver")
