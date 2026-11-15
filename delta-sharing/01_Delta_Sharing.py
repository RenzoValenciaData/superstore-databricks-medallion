# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing · Compartir tablas Golden con un socio externo
# MAGIC
# MAGIC Caso de negocio: un **socio comercial** (p. ej. un distribuidor) necesita los KPIs mensuales y la rentabilidad por
# MAGIC subcategoría **sin acceso a nuestro workspace** ni copias de archivos. Con Delta Sharing (protocolo abierto) recibe un
# MAGIC enlace de activación y lee las tablas desde pandas, Spark o **Power BI** (conector *Delta Sharing*).
# MAGIC
# MAGIC Pre-requisito: en la Account Console → *Catalog* → metastore → habilitar **"Allow Delta Sharing with parties outside your organization"**.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "prod", ["dev", "prod"], "Ambiente")

# COMMAND ----------

env = dbutils.widgets.get("env")
catalogo = f"superstore_{env}"
share = "share_superstore_golden"
recipient = "rcp_socio_comercial"
tablas = ["kpi_mensual", "rentabilidad_subcategoria"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear el share y agregar tablas (con historial para *time travel* y CDF)

# COMMAND ----------

spark.sql(f"CREATE SHARE IF NOT EXISTS {share} COMMENT 'KPIs golden de Superstore para socios'")

ya_compartidas = {r["shared_object"] for r in spark.sql(f"SHOW ALL IN SHARE {share}").collect()}
for t in tablas:
    nombre = f"{catalogo}.golden.{t}"
    if nombre in ya_compartidas:
        print(f"= {nombre} ya estaba en el share")
        continue
    spark.sql(f"ALTER SHARE {share} ADD TABLE {nombre} WITH HISTORY")
    print(f"✔ {nombre} agregado al share")

display(spark.sql(f"SHOW ALL IN SHARE {share}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear el destinatario (open sharing) y darle acceso

# COMMAND ----------

spark.sql(f"CREATE RECIPIENT IF NOT EXISTS {recipient} COMMENT 'Socio comercial externo (open sharing)'")
spark.sql(f"GRANT SELECT ON SHARE {share} TO RECIPIENT {recipient}")

display(spark.sql(f"SHOW GRANTS ON SHARE {share}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enlace de activación
# MAGIC El `activation_link` se envía al socio: descarga un archivo `config.share` con el endpoint y el token.
# MAGIC 📸 Evidencia: captura de esta salida **ocultando el token**.

# COMMAND ----------

display(spark.sql(f"DESCRIBE RECIPIENT {recipient}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. (Opcional) Consumo como lo haría el socio
# MAGIC Desde una laptop con Python:
# MAGIC ```python
# MAGIC # pip install delta-sharing
# MAGIC import delta_sharing
# MAGIC perfil = "config.share"                                   # archivo descargado del activation link
# MAGIC print(delta_sharing.SharingClient(perfil).list_all_tables())
# MAGIC df = delta_sharing.load_as_pandas(f"{perfil}#share_superstore_golden.golden.kpi_mensual")
# MAGIC df.head()
# MAGIC ```
# MAGIC O en **Power BI Desktop** → *Obtener datos* → *Delta Sharing* → endpoint + token del `config.share`.
