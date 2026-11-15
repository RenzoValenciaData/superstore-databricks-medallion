# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Preparación de Ambiente (Unity Catalog)
# MAGIC
# MAGIC **Proyecto:** Superstore · Arquitectura Medallion en Azure Databricks
# MAGIC
# MAGIC Crea (de forma **idempotente**) todos los objetos que necesita el ETL:
# MAGIC
# MAGIC | Objeto | Detalle |
# MAGIC |---|---|
# MAGIC | External Locations | `exlt_raw`, `exlt_bronze`, `exlt_silver`, `exlt_golden`, `exlt_metastore` sobre ADLS Gen2, usando la Storage Credential `cred_superstore_mi` (**Managed Identity** del Access Connector) |
# MAGIC | Catálogo | `superstore_<env>` con *managed location* en el contenedor `metastore` |
# MAGIC | Esquemas | `bronze`, `silver`, `golden`, `control` |
# MAGIC | Tablas | Todas las tablas del ETL se crean **aquí**; el ETL solo hace `insertInto` |
# MAGIC
# MAGIC **Parámetros (widgets / base_parameters del workflow):** `env` (dev | prod), `storageName`, `reset` (true | false)
# MAGIC
# MAGIC > Aislamiento de ambientes: ambos workspaces comparten el mismo metastore y el mismo Data Lake, por eso cada ambiente
# MAGIC > escribe en su propio catálogo (`superstore_dev` / `superstore_prod`) y en su propio prefijo físico (`/dev/…`, `/prod/…`).

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Ambiente")
dbutils.widgets.text("storageName", "adlssuperstorervc1", "Storage Account (ADLS Gen2)")
dbutils.widgets.dropdown("reset", "false", ["false", "true"], "Reset total (borra catálogo y datos)")

# COMMAND ----------

env = dbutils.widgets.get("env").strip().lower()
storageName = dbutils.widgets.get("storageName").strip()
reset = dbutils.widgets.get("reset").strip().lower() == "true"

assert env in ("dev", "prod"), f"env inválido: {env}"
assert storageName and "XXXX" not in storageName, "Configura el widget storageName con tu storage account real"

credencial = "cred_superstore_mi"
catalogo = f"superstore_{env}"

def abfss(contenedor: str, ruta: str = "") -> str:
    """Construye la ruta abfss:// del Data Lake para un contenedor."""
    return f"abfss://{contenedor}@{storageName}.dfs.core.windows.net/{ruta}"

print(f"Ambiente: {env} | Catálogo: {catalogo} | Storage: {storageName} | Reset: {reset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. External Locations (conexión a ADLS vía Managed Identity)
# MAGIC La **Storage Credential** `cred_superstore_mi` se crea una sola vez desde *Catalog Explorer → External data → Credentials*
# MAGIC con el Resource ID del Access Connector (no se puede crear por SQL para Managed Identity).
# MAGIC Como dev y prod comparten metastore, estas ubicaciones se reutilizan en ambos ambientes.

# COMMAND ----------

ubicaciones = {
    "exlt_metastore": "metastore",
    "exlt_raw": "raw",
    "exlt_bronze": "bronze",
    "exlt_silver": "silver",
    "exlt_golden": "golden",
}

for nombre, contenedor in ubicaciones.items():
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{nombre}`
        URL '{abfss(contenedor)}'
        WITH (STORAGE CREDENTIAL `{credencial}`)
        COMMENT 'Contenedor {contenedor} del Data Lake - proyecto Superstore'
    """)
    print(f"✔ External location {nombre} -> {abfss(contenedor)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reset opcional (solo si `reset = true`)
# MAGIC Borra el catálogo del ambiente y sus rutas físicas para reconstruir desde cero.
# MAGIC Los scripts formales de contingencia están en la carpeta `/reversion`.

# COMMAND ----------

if reset:
    spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
    for contenedor in ["bronze", "silver", "golden", "metastore"]:
        try:
            dbutils.fs.rm(abfss(contenedor, env), True)
        except Exception as e:
            print(f"(info) No se pudo borrar {contenedor}/{env}: {e}")
    print(f"⚠ Catálogo {catalogo} y rutas /{env} eliminados")
else:
    print("Reset desactivado: se crean solo los objetos que no existan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Catálogo y esquemas

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {catalogo}
    MANAGED LOCATION '{abfss("metastore", env)}'
    COMMENT 'Catálogo Superstore - arquitectura medallion - ambiente {env}'
""")

esquemas = {
    "bronze": "Datos crudos tipados desde la capa raw (ADLS)",
    "silver": "Datos limpios, deduplicados y enriquecidos",
    "golden": "Marts de negocio listos para BI",
    "control": "Auditoría de cargas y registros rechazados (data quality)",
}
for esquema, descripcion in esquemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema} COMMENT '{descripcion}'")
    print(f"✔ Esquema {catalogo}.{esquema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tablas · capa Bronze

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.orders (
  row_id INT, order_id STRING, order_date DATE, ship_date DATE, ship_mode STRING,
  customer_id STRING, customer_name STRING, segment STRING, country STRING, city STRING,
  state STRING, postal_code STRING, region STRING, product_id STRING, category STRING,
  sub_category STRING, product_name STRING, sales DOUBLE, quantity INT, discount DOUBLE,
  profit DOUBLE, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION '{abfss("bronze", f"{env}/orders")}'
COMMENT 'Bronze - órdenes de venta (fuente CSV)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.returns (
  order_id STRING, returned STRING, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION '{abfss("bronze", f"{env}/returns")}'
COMMENT 'Bronze - devoluciones (fuente JSON)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.people (
  regional_manager STRING, region STRING, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION '{abfss("bronze", f"{env}/people")}'
COMMENT 'Bronze - gerentes regionales (fuente Parquet)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tablas · capa Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.ventas_detalle (
  id_fila INT, id_orden STRING, fecha_orden DATE, fecha_envio DATE, anio INT, mes INT,
  trimestre INT, anio_mes DATE, modo_envio STRING, dias_envio INT, id_cliente STRING,
  nombre_cliente STRING, segmento STRING, pais STRING, ciudad STRING, estado STRING,
  codigo_postal STRING, region STRING, gerente_regional STRING, id_producto STRING,
  categoria STRING, subcategoria STRING, nombre_producto STRING, ventas DOUBLE,
  cantidad INT, descuento DOUBLE, banda_descuento STRING, utilidad DOUBLE,
  margen_pct DOUBLE, precio_unitario DOUBLE, ventas_brutas DOUBLE, monto_descuento DOUBLE,
  es_perdida BOOLEAN, devuelto BOOLEAN, fecha_proceso TIMESTAMP
)
USING DELTA
PARTITIONED BY (anio)
LOCATION '{abfss("silver", f"{env}/ventas_detalle")}'
COMMENT 'Silver - detalle de ventas limpio y enriquecido (particionado por año)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.dim_cliente (
  id_cliente STRING, nombre_cliente STRING, segmento STRING, region_principal STRING,
  primera_compra DATE, ultima_compra DATE, total_ordenes BIGINT, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("silver", f"{env}/dim_cliente")}'
COMMENT 'Silver - dimensión clientes'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.dim_producto (
  id_producto STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
  nombres_distintos INT, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("silver", f"{env}/dim_producto")}'
COMMENT 'Silver - dimensión productos (nombre canónico por id_producto)'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tablas · capa Golden

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.kpi_mensual (
  anio_mes DATE, anio INT, mes INT, region STRING, categoria STRING,
  ventas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, ordenes BIGINT, unidades BIGINT,
  descuento_promedio DOUBLE, monto_descuento DOUBLE, ordenes_devueltas BIGINT,
  tasa_devolucion DOUBLE, ventas_acum_anio DOUBLE, ventas_mismo_mes_anio_ant DOUBLE,
  crecimiento_yoy_pct DOUBLE, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("golden", f"{env}/kpi_mensual")}'
COMMENT 'Golden - KPIs mensuales por región y categoría (acumulado y YoY)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.rentabilidad_subcategoria (
  region STRING, categoria STRING, subcategoria STRING, ventas DOUBLE, utilidad DOUBLE,
  margen_pct DOUBLE, monto_descuento DOUBLE, descuento_promedio DOUBLE,
  ranking_utilidad_region INT, es_deficitaria BOOLEAN, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("golden", f"{env}/rentabilidad_subcategoria")}'
COMMENT 'Golden - rentabilidad por subcategoría y región'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.clientes_pareto (
  id_cliente STRING, nombre_cliente STRING, segmento STRING, region_principal STRING,
  ventas DOUBLE, utilidad DOUBLE, ordenes BIGINT, ticket_promedio DOUBLE,
  ranking_segmento INT, participacion_acum_pct DOUBLE, clase_abc STRING, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("golden", f"{env}/clientes_pareto")}'
COMMENT 'Golden - clientes con análisis Pareto / ABC'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.golden.desempeno_envios (
  modo_envio STRING, region STRING, ordenes BIGINT, dias_envio_prom DOUBLE,
  dias_envio_p90 INT, ventas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{abfss("golden", f"{env}/desempeno_envios")}'
COMMENT 'Golden - desempeño logístico por modo de envío y región'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tablas · esquema Control (auditoría y data quality)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.control.auditoria_etl (
  run_id STRING, env STRING, proceso STRING, tabla_destino STRING,
  filas_leidas BIGINT, filas_escritas BIGINT, filas_rechazadas BIGINT,
  inicio TIMESTAMP, fin TIMESTAMP, estado STRING
)
USING DELTA
COMMENT 'Bitácora de ejecución del ETL (una fila por tabla cargada)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.control.rechazados (
  run_id STRING, fuente STRING, motivo STRING, registro STRING,
  source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
COMMENT 'Registros que no pasaron las reglas de calidad en Bronze'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verificación

# COMMAND ----------

for esquema in esquemas:
    tablas = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {catalogo}.{esquema}").collect()]
    print(f"{catalogo}.{esquema}: {tablas}")

# COMMAND ----------

dbutils.notebook.exit(f"OK - ambiente {env} preparado en {catalogo}")
