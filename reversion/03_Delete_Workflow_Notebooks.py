# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión 3 · Eliminación del workflow y notebooks desplegados (API)
# MAGIC
# MAGIC Revierte lo que crea el pipeline de GitHub Actions en **producción**:
# MAGIC - el job `WF_SUPERSTORE_ETL` (Jobs API, vía Databricks SDK)
# MAGIC - la carpeta de notebooks desplegados `/Shared/superstore/proceso` (Workspace API)
# MAGIC
# MAGIC Ejecutar en el workspace **prod**. El SDK se autentica solo con la identidad del notebook.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("workflowName", "WF_SUPERSTORE_ETL", "Nombre del workflow")
dbutils.widgets.text("rutaNotebooks", "/Shared/superstore/proceso", "Carpeta desplegada")
dbutils.widgets.dropdown("confirmar", "no", ["no", "si"], "¿Confirmar borrado?")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
workflow = dbutils.widgets.get("workflowName")
ruta = dbutils.widgets.get("rutaNotebooks")
confirmar = dbutils.widgets.get("confirmar") == "si"

jobs = [j for j in w.jobs.list(name=workflow)]
print(f"Jobs encontrados con nombre '{workflow}': {[j.job_id for j in jobs]}")

# COMMAND ----------

if not confirmar:
    dbutils.notebook.exit("Simulación: cambia 'confirmar' a 'si' para eliminar")

for j in jobs:
    w.jobs.delete(job_id=j.job_id)
    print(f"✔ Job {j.job_id} eliminado")

try:
    w.workspace.delete(path=ruta, recursive=True)
    print(f"✔ Carpeta {ruta} eliminada")
except Exception as e:
    print(f"(info) {ruta}: {e}")

dbutils.notebook.exit("OK reversión de despliegue")
