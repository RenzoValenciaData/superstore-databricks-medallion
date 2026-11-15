-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seguridad · Grants a usuarios y grupos (Unity Catalog)
-- MAGIC
-- MAGIC Usuarios y grupos **ficticios** creados en la *Account Console* (User management) y asignados a los workspaces
-- MAGIC `dbw-superstore-dev` y `dbw-superstore-prod` (Workspace → Permissions → Add).
-- MAGIC
-- MAGIC | Grupo | Miembros | Rol |
-- MAGIC |---|---|---|
-- MAGIC | `grp_ingenieros_datos` | ing.datos1@example.com, ing.datos2@example.com | Construyen y operan el ETL |
-- MAGIC | `grp_analistas_finanzas` | analista.fin1@example.com, analista.fin2@example.com | Analizan silver y golden |
-- MAGIC | `grp_consumidores_bi` | gerente.bi1@example.com | Solo consumen KPIs golden |
-- MAGIC | *(usuario individual)* | auditor.superstore@example.com | Revisa la bitácora del ETL |
-- MAGIC
-- MAGIC Estos mismos grants los aplica automáticamente la última tarea del workflow (`proceso/6_Grants`).

-- COMMAND ----------

CREATE WIDGET DROPDOWN env DEFAULT "prod" CHOICES SELECT * FROM (VALUES ("dev"), ("prod"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Ingenieros de datos — todas las capas + lectura de archivos raw

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG superstore_${env} TO `grp_ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA superstore_${env}.bronze  TO `grp_ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA superstore_${env}.silver  TO `grp_ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA superstore_${env}.golden  TO `grp_ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA superstore_${env}.control TO `grp_ingenieros_datos`;
GRANT READ FILES ON EXTERNAL LOCATION `exlt_raw` TO `grp_ingenieros_datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Analistas de finanzas — lectura de silver y golden

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG superstore_${env} TO `grp_analistas_finanzas`;
GRANT USE SCHEMA, SELECT ON SCHEMA superstore_${env}.silver TO `grp_analistas_finanzas`;
GRANT USE SCHEMA, SELECT ON SCHEMA superstore_${env}.golden TO `grp_analistas_finanzas`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Consumidores BI — solo tablas golden puntuales

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG superstore_${env} TO `grp_consumidores_bi`;
GRANT USE SCHEMA ON SCHEMA superstore_${env}.golden TO `grp_consumidores_bi`;
GRANT SELECT ON TABLE superstore_${env}.golden.kpi_mensual TO `grp_consumidores_bi`;
GRANT SELECT ON TABLE superstore_${env}.golden.rentabilidad_subcategoria TO `grp_consumidores_bi`;
GRANT SELECT ON TABLE superstore_${env}.golden.clientes_pareto TO `grp_consumidores_bi`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Usuario individual — auditor

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG superstore_${env} TO `auditor.superstore@example.com`;
GRANT USE SCHEMA ON SCHEMA superstore_${env}.control TO `auditor.superstore@example.com`;
GRANT SELECT ON TABLE superstore_${env}.control.auditoria_etl TO `auditor.superstore@example.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Verificación

-- COMMAND ----------

SHOW GRANTS ON CATALOG superstore_${env};

-- COMMAND ----------

SHOW GRANTS ON SCHEMA superstore_${env}.golden;

-- COMMAND ----------

SHOW GRANTS ON EXTERNAL LOCATION `exlt_raw`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Revocación (ejemplo de mínimo privilegio)

-- COMMAND ----------

-- REVOKE SELECT ON TABLE superstore_${env}.golden.clientes_pareto FROM `grp_consumidores_bi`;
