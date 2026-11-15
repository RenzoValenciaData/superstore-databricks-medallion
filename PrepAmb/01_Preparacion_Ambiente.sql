-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Preparación de Ambiente (versión SQL)
-- MAGIC
-- MAGIC Script SQL equivalente a `proceso/1_Preparacion_Ambiente` (la versión que despliega y ejecuta el workflow en producción).
-- MAGIC Crea **external locations, catálogo, esquemas y TODAS las tablas** del ETL. El ETL solo inserta (`insertInto`).
-- MAGIC
-- MAGIC **Pre-requisitos (una sola vez, por portal/UI):**
-- MAGIC 1. ADLS Gen2 con contenedores `raw`, `bronze`, `silver`, `golden`, `metastore`.
-- MAGIC 2. Access Connector `ac-superstore-dbx` con rol **Storage Blob Data Contributor** sobre el storage.
-- MAGIC 3. Storage Credential `cred_superstore_mi` (Catalog Explorer → External data → Credentials → Azure Managed Identity).

-- COMMAND ----------

CREATE WIDGET DROPDOWN env DEFAULT "dev" CHOICES SELECT * FROM (VALUES ("dev"), ("prod"));
CREATE WIDGET TEXT storageName DEFAULT "adlssuperstorervc1";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. External Locations (Managed Identity)

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_metastore`
URL 'abfss://metastore@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cred_superstore_mi`)
COMMENT 'Contenedor metastore (managed location de los catálogos)';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_raw`
URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cred_superstore_mi`)
COMMENT 'Capa raw - insumos del ETL';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_bronze`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cred_superstore_mi`)
COMMENT 'Capa bronze';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_silver`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cred_superstore_mi`)
COMMENT 'Capa silver';

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt_golden`
URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `cred_superstore_mi`)
COMMENT 'Capa golden';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Catálogo y esquemas

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS superstore_${env}
MANAGED LOCATION 'abfss://metastore@${storageName}.dfs.core.windows.net/${env}'
COMMENT 'Catálogo Superstore - arquitectura medallion';

CREATE SCHEMA IF NOT EXISTS superstore_${env}.bronze  COMMENT 'Datos crudos tipados desde raw';
CREATE SCHEMA IF NOT EXISTS superstore_${env}.silver  COMMENT 'Datos limpios y enriquecidos';
CREATE SCHEMA IF NOT EXISTS superstore_${env}.golden  COMMENT 'Marts de negocio para BI';
CREATE SCHEMA IF NOT EXISTS superstore_${env}.control COMMENT 'Auditoría y data quality';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Tablas Bronze

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS superstore_${env}.bronze.orders (
  row_id INT, order_id STRING, order_date DATE, ship_date DATE, ship_mode STRING,
  customer_id STRING, customer_name STRING, segment STRING, country STRING, city STRING,
  state STRING, postal_code STRING, region STRING, product_id STRING, category STRING,
  sub_category STRING, product_name STRING, sales DOUBLE, quantity INT, discount DOUBLE,
  profit DOUBLE, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@${storageName}.dfs.core.windows.net/${env}/orders'
COMMENT 'Bronze - órdenes de venta (fuente CSV)';

CREATE TABLE IF NOT EXISTS superstore_${env}.bronze.returns (
  order_id STRING, returned STRING, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@${storageName}.dfs.core.windows.net/${env}/returns'
COMMENT 'Bronze - devoluciones (fuente JSON)';

CREATE TABLE IF NOT EXISTS superstore_${env}.bronze.people (
  regional_manager STRING, region STRING, source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@${storageName}.dfs.core.windows.net/${env}/people'
COMMENT 'Bronze - gerentes regionales (fuente Parquet)';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Tablas Silver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS superstore_${env}.silver.ventas_detalle (
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
LOCATION 'abfss://silver@${storageName}.dfs.core.windows.net/${env}/ventas_detalle'
COMMENT 'Silver - detalle de ventas limpio y enriquecido';

CREATE TABLE IF NOT EXISTS superstore_${env}.silver.dim_cliente (
  id_cliente STRING, nombre_cliente STRING, segmento STRING, region_principal STRING,
  primera_compra DATE, ultima_compra DATE, total_ordenes BIGINT, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@${storageName}.dfs.core.windows.net/${env}/dim_cliente'
COMMENT 'Silver - dimensión clientes';

CREATE TABLE IF NOT EXISTS superstore_${env}.silver.dim_producto (
  id_producto STRING, nombre_producto STRING, categoria STRING, subcategoria STRING,
  nombres_distintos INT, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://silver@${storageName}.dfs.core.windows.net/${env}/dim_producto'
COMMENT 'Silver - dimensión productos';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Tablas Golden

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS superstore_${env}.golden.kpi_mensual (
  anio_mes DATE, anio INT, mes INT, region STRING, categoria STRING,
  ventas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, ordenes BIGINT, unidades BIGINT,
  descuento_promedio DOUBLE, monto_descuento DOUBLE, ordenes_devueltas BIGINT,
  tasa_devolucion DOUBLE, ventas_acum_anio DOUBLE, ventas_mismo_mes_anio_ant DOUBLE,
  crecimiento_yoy_pct DOUBLE, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://golden@${storageName}.dfs.core.windows.net/${env}/kpi_mensual'
COMMENT 'Golden - KPIs mensuales por región y categoría';

CREATE TABLE IF NOT EXISTS superstore_${env}.golden.rentabilidad_subcategoria (
  region STRING, categoria STRING, subcategoria STRING, ventas DOUBLE, utilidad DOUBLE,
  margen_pct DOUBLE, monto_descuento DOUBLE, descuento_promedio DOUBLE,
  ranking_utilidad_region INT, es_deficitaria BOOLEAN, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://golden@${storageName}.dfs.core.windows.net/${env}/rentabilidad_subcategoria'
COMMENT 'Golden - rentabilidad por subcategoría y región';

CREATE TABLE IF NOT EXISTS superstore_${env}.golden.clientes_pareto (
  id_cliente STRING, nombre_cliente STRING, segmento STRING, region_principal STRING,
  ventas DOUBLE, utilidad DOUBLE, ordenes BIGINT, ticket_promedio DOUBLE,
  ranking_segmento INT, participacion_acum_pct DOUBLE, clase_abc STRING, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://golden@${storageName}.dfs.core.windows.net/${env}/clientes_pareto'
COMMENT 'Golden - clientes Pareto / ABC';

CREATE TABLE IF NOT EXISTS superstore_${env}.golden.desempeno_envios (
  modo_envio STRING, region STRING, ordenes BIGINT, dias_envio_prom DOUBLE,
  dias_envio_p90 INT, ventas DOUBLE, utilidad DOUBLE, margen_pct DOUBLE, fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION 'abfss://golden@${storageName}.dfs.core.windows.net/${env}/desempeno_envios'
COMMENT 'Golden - desempeño logístico';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Tablas de control

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS superstore_${env}.control.auditoria_etl (
  run_id STRING, env STRING, proceso STRING, tabla_destino STRING,
  filas_leidas BIGINT, filas_escritas BIGINT, filas_rechazadas BIGINT,
  inicio TIMESTAMP, fin TIMESTAMP, estado STRING
)
USING DELTA
COMMENT 'Bitácora de ejecución del ETL';

CREATE TABLE IF NOT EXISTS superstore_${env}.control.rechazados (
  run_id STRING, fuente STRING, motivo STRING, registro STRING,
  source_file STRING, ingestion_ts TIMESTAMP
)
USING DELTA
COMMENT 'Registros rechazados por reglas de calidad';

-- COMMAND ----------

SHOW TABLES IN superstore_${env}.golden;
