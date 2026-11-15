# Databricks notebook source
# MAGIC %md
# MAGIC # 6 · Grants — Seguridad en Unity Catalog
# MAGIC
# MAGIC Última tarea del workflow: otorga permisos sobre los objetos creados por el ETL, aplicando
# MAGIC **mínimo privilegio** por rol. Los usuarios y grupos se crean previamente en la **Account Console**
# MAGIC y se asignan a ambos workspaces (dev y prod).
# MAGIC
# MAGIC | Principal | Tipo | Permisos |
# MAGIC |---|---|---|
# MAGIC | `grp_ingenieros_datos` | Grupo | `USE CATALOG`, `USE SCHEMA` + `SELECT` + `MODIFY` en bronze/silver/golden/control, `READ FILES` en `exlt_raw` |
# MAGIC | `grp_analistas_finanzas` | Grupo | `USE CATALOG`, `USE SCHEMA` + `SELECT` en silver y golden |
# MAGIC | `grp_consumidores_bi` | Grupo | `USE CATALOG`, `USE SCHEMA` golden, `SELECT` solo en 3 tablas Golden |
# MAGIC | `auditor.superstore@example.com` | Usuario | `USE CATALOG`, `USE SCHEMA` control, `SELECT` en `control.auditoria_etl` |

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Ambiente")

# COMMAND ----------

env = dbutils.widgets.get("env")
catalogo = f"superstore_{env}"

GRP_ING = "grp_ingenieros_datos"
GRP_ANALISTAS = "grp_analistas_finanzas"
GRP_BI = "grp_consumidores_bi"
USR_AUDITOR = "auditor.superstore@example.com"

grants = [
    # --- Ingenieros de datos: operan todas las capas ---
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{GRP_ING}`",
    *[f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {catalogo}.{e} TO `{GRP_ING}`"
      for e in ("bronze", "silver", "golden", "control")],
    f"GRANT READ FILES ON EXTERNAL LOCATION `exlt_raw` TO `{GRP_ING}`",

    # --- Analistas de finanzas: lectura de silver y golden ---
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{GRP_ANALISTAS}`",
    *[f"GRANT USE SCHEMA, SELECT ON SCHEMA {catalogo}.{e} TO `{GRP_ANALISTAS}`" for e in ("silver", "golden")],

    # --- Consumidores BI: solo tablas Golden puntuales ---
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{GRP_BI}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalogo}.golden TO `{GRP_BI}`",
    *[f"GRANT SELECT ON TABLE {catalogo}.golden.{t} TO `{GRP_BI}`"
      for t in ("kpi_mensual", "rentabilidad_subcategoria", "clientes_pareto")],

    # --- Usuario individual: auditor ---
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{USR_AUDITOR}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalogo}.control TO `{USR_AUDITOR}`",
    f"GRANT SELECT ON TABLE {catalogo}.control.auditoria_etl TO `{USR_AUDITOR}`",
]

# COMMAND ----------

errores = []
for sentencia in grants:
    try:
        spark.sql(sentencia)
        print(f"✔ {sentencia}")
    except Exception as e:
        errores.append((sentencia, str(e).splitlines()[0]))
        print(f"✘ {sentencia}\n   -> {str(e).splitlines()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidencia: permisos efectivos

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG {catalogo}"))

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.golden"))

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON TABLE {catalogo}.golden.kpi_mensual"))

# COMMAND ----------

if errores:
    raise Exception(f"{len(errores)} grants fallaron (¿grupos/usuarios creados y asignados al workspace?): {errores}")

# COMMAND ----------

dbutils.notebook.exit(f"OK grants: {len(grants)} sentencias aplicadas en {catalogo}")
