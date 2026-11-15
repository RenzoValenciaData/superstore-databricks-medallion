-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reversión 1 · Eliminación de objetos lógicos (Unity Catalog)
-- MAGIC
-- MAGIC **Plan de contingencia:** si el pase a producción falla a mitad de camino, se ejecuta la reversión en este orden:
-- MAGIC 1. `01_Drop_Objetos_Logicos` → tablas, esquemas, catálogo y Delta Sharing (este notebook)
-- MAGIC 2. `02_Drop_Rutas_Fisicas` → archivos Delta en ADLS
-- MAGIC 3. `03_Delete_Workflow_Notebooks` → workflow y notebooks desplegados vía API
-- MAGIC
-- MAGIC ⚠️ Irreversible. Ejecutar solo con aprobación.

-- COMMAND ----------

CREATE WIDGET DROPDOWN env DEFAULT "prod" CHOICES SELECT * FROM (VALUES ("dev"), ("prod"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Delta Sharing (primero, porque referencia tablas golden)

-- COMMAND ----------

DROP SHARE IF EXISTS share_superstore_golden;
DROP RECIPIENT IF EXISTS rcp_socio_comercial;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Tablas Golden

-- COMMAND ----------

DROP TABLE IF EXISTS superstore_${env}.golden.kpi_mensual;
DROP TABLE IF EXISTS superstore_${env}.golden.rentabilidad_subcategoria;
DROP TABLE IF EXISTS superstore_${env}.golden.clientes_pareto;
DROP TABLE IF EXISTS superstore_${env}.golden.desempeno_envios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Tablas Silver

-- COMMAND ----------

DROP TABLE IF EXISTS superstore_${env}.silver.ventas_detalle;
DROP TABLE IF EXISTS superstore_${env}.silver.dim_cliente;
DROP TABLE IF EXISTS superstore_${env}.silver.dim_producto;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Tablas Bronze y Control

-- COMMAND ----------

DROP TABLE IF EXISTS superstore_${env}.bronze.orders;
DROP TABLE IF EXISTS superstore_${env}.bronze.returns;
DROP TABLE IF EXISTS superstore_${env}.bronze.people;
DROP TABLE IF EXISTS superstore_${env}.control.auditoria_etl;
DROP TABLE IF EXISTS superstore_${env}.control.rechazados;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Esquemas y catálogo

-- COMMAND ----------

DROP SCHEMA IF EXISTS superstore_${env}.bronze CASCADE;
DROP SCHEMA IF EXISTS superstore_${env}.silver CASCADE;
DROP SCHEMA IF EXISTS superstore_${env}.golden CASCADE;
DROP SCHEMA IF EXISTS superstore_${env}.control CASCADE;
DROP CATALOG IF EXISTS superstore_${env} CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. (Opcional, desmontaje total) External locations y credencial
-- MAGIC Solo si se elimina el proyecto completo: son compartidas por dev y prod.

-- COMMAND ----------

-- DROP EXTERNAL LOCATION IF EXISTS `exlt_raw`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_bronze`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_silver`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_golden`;
-- DROP EXTERNAL LOCATION IF EXISTS `exlt_metastore`;
-- DROP STORAGE CREDENTIAL IF EXISTS `cred_superstore_mi`;
