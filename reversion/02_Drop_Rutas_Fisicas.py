# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión 2 · Eliminación de rutas físicas (ADLS Gen2)
# MAGIC
# MAGIC Las tablas Bronze/Silver/Golden son **externas**: `DROP TABLE` borra el metadato pero no los archivos.
# MAGIC Este notebook elimina los directorios Delta del ambiente indicado. La capa **raw no se toca** (es la fuente).

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("env", "prod", ["dev", "prod"], "Ambiente")
dbutils.widgets.text("storageName", "adlssuperstorervc1", "Storage Account")
dbutils.widgets.dropdown("confirmar", "no", ["no", "si"], "¿Confirmar borrado?")

# COMMAND ----------

env = dbutils.widgets.get("env")
storageName = dbutils.widgets.get("storageName")
confirmar = dbutils.widgets.get("confirmar") == "si"

rutas = {
    "bronze": ["orders", "returns", "people"],
    "silver": ["ventas_detalle", "dim_cliente", "dim_producto"],
    "golden": ["kpi_mensual", "rentabilidad_subcategoria", "clientes_pareto", "desempeno_envios"],
}

objetivos = [f"abfss://{c}@{storageName}.dfs.core.windows.net/{env}/{t}" for c, ts in rutas.items() for t in ts]
objetivos.append(f"abfss://metastore@{storageName}.dfs.core.windows.net/{env}")   # managed location del catálogo

for ruta in objetivos:
    print(("BORRAR  " if confirmar else "SIMULAR ") + ruta)

# COMMAND ----------

if not confirmar:
    dbutils.notebook.exit("Simulación: cambia el widget 'confirmar' a 'si' para borrar")

for ruta in objetivos:
    try:
        dbutils.fs.rm(ruta, True)
        print(f"✔ eliminado {ruta}")
    except Exception as e:
        print(f"(info) {ruta}: {str(e).splitlines()[0]}")

dbutils.notebook.exit(f"OK rutas físicas de {env} eliminadas")
