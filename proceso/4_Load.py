# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Load — Silver → Golden
# MAGIC
# MAGIC Construye los **marts de negocio** que consumen el Dashboard, Genie, Power BI y Delta Sharing.
# MAGIC
# MAGIC | Tabla Golden | Pregunta de negocio que responde |
# MAGIC |---|---|
# MAGIC | `golden.kpi_mensual` | ¿Cómo evolucionan ventas, utilidad y margen por región y categoría? ¿Crecemos vs. el año anterior? |
# MAGIC | `golden.rentabilidad_subcategoria` | ¿Qué subcategorías destruyen margen y cuánto "regalamos" en descuentos? |
# MAGIC | `golden.clientes_pareto` | ¿Qué clientes explican el 80 % de las ventas (clasificación ABC)? |
# MAGIC | `golden.desempeno_envios` | ¿Cuánto demora cada modo de envío y qué margen deja? |
# MAGIC
# MAGIC **Técnicas PySpark:** agregaciones, `countDistinct`, `percentile_approx`, funciones de ventana
# MAGIC (`sum` acumulado, `lag` YoY, `dense_rank`, `row_number`), joins, cálculos condicionales e `insertInto`.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


def ratio(numerador, denominador, decimales=4):
    """División segura: null si el denominador es 0 o null."""
    return F.when(denominador != 0, F.round(numerador / denominador, decimales))

# COMMAND ----------

ventas = spark.table(f"{catalogo}.silver.ventas_detalle")
clientes = spark.table(f"{catalogo}.silver.dim_cliente")
filas_silver = ventas.count()
print(f"silver.ventas_detalle: {filas_silver} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. `kpi_mensual` — KPIs mensuales con acumulado del año y crecimiento YoY

# COMMAND ----------

kpi = (ventas
       .groupBy("anio_mes", "anio", "mes", "region", "categoria")
       .agg(F.round(F.sum("ventas"), 2).alias("ventas"),
            F.round(F.sum("utilidad"), 2).alias("utilidad"),
            F.countDistinct("id_orden").alias("ordenes"),
            F.sum("cantidad").cast("long").alias("unidades"),
            F.round(F.avg("descuento"), 4).alias("descuento_promedio"),
            F.round(F.sum("monto_descuento"), 2).alias("monto_descuento"),
            F.countDistinct(F.when(F.col("devuelto"), F.col("id_orden"))).alias("ordenes_devueltas"))
       .withColumn("margen_pct", ratio(F.col("utilidad"), F.col("ventas")))
       .withColumn("tasa_devolucion", ratio(F.col("ordenes_devueltas"), F.col("ordenes"))))

w_acum = (Window.partitionBy("anio", "region", "categoria").orderBy("mes")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))
w_yoy = Window.partitionBy("mes", "region", "categoria").orderBy("anio")

kpi = (kpi
       .withColumn("ventas_acum_anio", F.round(F.sum("ventas").over(w_acum), 2))
       # lag solo si el registro anterior es exactamente el año previo
       .withColumn("ventas_mismo_mes_anio_ant",
                   F.when(F.lag("anio").over(w_yoy) == F.col("anio") - 1, F.lag("ventas").over(w_yoy)))
       .withColumn("crecimiento_yoy_pct",
                   ratio(F.col("ventas") - F.col("ventas_mismo_mes_anio_ant"), F.col("ventas_mismo_mes_anio_ant")))
       .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. `rentabilidad_subcategoria` — ranking de utilidad y subcategorías deficitarias

# COMMAND ----------

w_rank = Window.partitionBy("region").orderBy(F.col("utilidad").desc())

rentabilidad = (ventas
                .groupBy("region", "categoria", "subcategoria")
                .agg(F.round(F.sum("ventas"), 2).alias("ventas"),
                     F.round(F.sum("utilidad"), 2).alias("utilidad"),
                     F.round(F.sum("monto_descuento"), 2).alias("monto_descuento"),
                     F.round(F.avg("descuento"), 4).alias("descuento_promedio"))
                .withColumn("margen_pct", ratio(F.col("utilidad"), F.col("ventas")))
                .withColumn("ranking_utilidad_region", F.dense_rank().over(w_rank))
                .withColumn("es_deficitaria", F.col("utilidad") < 0)
                .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. `clientes_pareto` — participación acumulada y clasificación ABC

# COMMAND ----------

por_cliente = (ventas
               .groupBy("id_cliente")
               .agg(F.round(F.sum("ventas"), 2).alias("ventas"),
                    F.round(F.sum("utilidad"), 2).alias("utilidad"),
                    F.countDistinct("id_orden").alias("ordenes"))
               .withColumn("ticket_promedio", ratio(F.col("ventas"), F.col("ordenes"), 2))
               .join(clientes.select("id_cliente", "nombre_cliente", "segmento", "region_principal"),
                     on="id_cliente", how="left"))

total_ventas = por_cliente.agg(F.sum("ventas").alias("t")).first()["t"]

w_pareto = Window.orderBy(F.col("ventas").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segmento = Window.partitionBy("segmento").orderBy(F.col("ventas").desc())

clientes_pareto = (por_cliente
                   .withColumn("participacion_acum_pct", F.round(F.sum("ventas").over(w_pareto) / F.lit(total_ventas), 4))
                   .withColumn("clase_abc", F.when(F.col("participacion_acum_pct") <= 0.80, "A")
                                             .when(F.col("participacion_acum_pct") <= 0.95, "B")
                                             .otherwise("C"))
                   .withColumn("ranking_segmento", F.row_number().over(w_segmento))
                   .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. `desempeno_envios` — tiempos de despacho y margen por modo de envío

# COMMAND ----------

envios = (ventas
          .groupBy("modo_envio", "region")
          .agg(F.countDistinct("id_orden").alias("ordenes"),
               F.round(F.avg("dias_envio"), 2).alias("dias_envio_prom"),
               F.percentile_approx("dias_envio", 0.9).cast("int").alias("dias_envio_p90"),
               F.round(F.sum("ventas"), 2).alias("ventas"),
               F.round(F.sum("utilidad"), 2).alias("utilidad"))
          .withColumn("margen_pct", ratio(F.col("utilidad"), F.col("ventas")))
          .withColumn("fecha_proceso", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Escritura en Golden (`insertInto`) y auditoría

# COMMAND ----------

destinos = {
    f"{catalogo}.golden.kpi_mensual": kpi,
    f"{catalogo}.golden.rentabilidad_subcategoria": rentabilidad,
    f"{catalogo}.golden.clientes_pareto": clientes_pareto,
    f"{catalogo}.golden.desempeno_envios": envios,
}

for tabla, df in destinos.items():
    insertar(df, tabla)
    escritas = spark.table(tabla).count()
    auditar("4_Load", tabla, filas_silver, escritas)
    print(f"✔ {tabla}: {escritas} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Control de cuadre (Silver vs Golden)
# MAGIC La suma de ventas en Golden debe ser igual a la de Silver: si no cuadra, el job falla.

# COMMAND ----------

v_silver = ventas.agg(F.round(F.sum("ventas"), 0)).first()[0]
v_golden = spark.table(f"{catalogo}.golden.kpi_mensual").agg(F.round(F.sum("ventas"), 0)).first()[0]
print(f"Ventas silver: {v_silver:,.0f} | Ventas golden: {v_golden:,.0f}")
assert abs(v_silver - v_golden) <= 5, "Descuadre entre Silver y Golden (tolerancia de redondeo: 5)"

display(spark.table(f"{catalogo}.golden.kpi_mensual").orderBy("anio_mes", "region", "categoria").limit(20))

# COMMAND ----------

dbutils.notebook.exit("OK golden")
